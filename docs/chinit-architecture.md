# chinit architecture

`chinit` bootstraps a ClickHouse node on any cloud: it detects the
platform, fetches a SHA-gated config bundle, converges the node to a
declarative spec, and installs its own recurring maintenance jobs as
subcommands. [chinit.md](chinit.md) is the user-facing doc — what it
replaces, how to wire it into node boot. This doc is for contributors:
how the binary is put together, why each layer exists, and where the
load-bearing decisions live.

## The shape in one paragraph

One process, four layers, no cloud SDK imports above the `infra/` line:

```
cmd/chinit ──► pkg/chinit/app ──► pkg/chinit/domain ──► pkg/chinit/infra
   main()        flags,            model, port,           aws/, azure/,
                 subcommands,      service (engine,       detect/, system/,
                 provider wiring   steps, units), fail    fakes/
```

Dependencies point inward only. `domain/` compiles without AWS, Azure,
or a Linux box; `infra/` adapts real clouds and real system calls to
the ports the domain declares. The same discipline the operator's
Kubernetes control plane uses, pointed at a different edge: instead of
reconciling CustomResources against a pod-shaped reality, chinit
reconciles a `BootSpec` against a bare machine.

## Layers and what each owns

| Layer | Owns | Never does |
|---|---|---|
| `cmd/chinit` | `main()` — one call into `app.Main` | logic |
| `app/` | flag parsing, subcommand dispatch, provider registry wiring, the `boot` orchestration (`pkg/chinit/app/boot.go`) | cloud calls; it talks to the provider through `port.Provider` |
| `domain/model` | the data shapes: `BootSpec`, `NodeIdentity`, role/mode enums — plain structs with yaml tags | behavior; validation only |
| `domain/port` | capability interfaces + the capability probes (`AsOrdinalClaimer`, `AsDNSRegistrar`, `AsSpotSignals` in `provider.go`) | implementations |
| `domain/service` | the boot pipeline (`steps.go`), the retry engine (`engine.go`), step implementations (`render.go`, `units.go`, `identity in steps.go`), the state store | direct cloud/system access — everything goes through `infra/` or the injected provider |
| `domain/fail` | `Retryable`/`Fatal` classification (`fail/`) | — |
| `infra/detect` | cloud detection: `-cloud` flag → `CHINIT_CLOUD` env → DMI `sys_vendor` → IMDS probe | — |
| `infra/aws`, `infra/azure` | the provider adapters: metadata, secrets, bundle fetch, ordinal leases, DNS, spot signals — one package per cloud, one registry entry each | boot logic |
| `infra/system` | machine facts and mutations: disks, OS tuning, config install, unit install (`system/disks.go`, `ostune.go`, `configs.go`, `units.go`) | cloud API calls |
| `infra/fakes` | canned IMDS/secrets/bundle for the conformance suite | — |

The rule of thumb for placement: if a change would make sense on a
different cloud, it belongs in `domain/`; if it names a cloud API, it
belongs in `infra/<cloud>/`; if it mutates the machine, it belongs in
`infra/system/`.

## The provider factory: mandatory core, optional capabilities

`port.Provider` is deliberately small. Three things are mandatory —
every cloud can name itself, describe its instance, fetch the config
bundle, and resolve secrets:

```go
// pkg/chinit/domain/port/provider.go
type Provider interface {
    Name() string
    MetadataProvider          // instance id, region, private IP
    port.SecretSource         // the same secrets port chctl uses
    ConfigSource              // SHA-gated bundle fetch
}
```

Everything else is an **optional capability**, probed once at boot:

```go
func AsOrdinalClaimer(p Provider) OrdinalClaimer   // readers' identity leases
func AsDNSRegistrar(p Provider) DNSRegistrar       // private-zone A records
func AsSpotSignals(p Provider) SpotSignals         // interruption notices
```

A provider without a capability does not fail the boot — the pipeline
degrades loudly (a `WARNING` naming the missing capability) and the
step either skips or refuses spec shapes that demand it
(`identity_mode: lease` with no claimer is `Fatal`, because a leased
reader without a claimer boots as an ephemeral `ip-*` name and breaks
replication identity — the exact production finding this rule comes
from).

This is the seam that makes new clouds cheap: GCP or Civo is one
`infra/<cloud>` package implementing at minimum the three mandatory
ports, plus a registry entry. The conformance suite (below) is the
proof the contract holds.

## The boot pipeline

`app.Main("boot", …)` → detect cloud → build provider → parse spec →
`boot.Steps(ctx)` → `engine.Run(ctx, steps)`. The step list is the
architecture's spine (`pkg/chinit/domain/service/steps.go`):

| # | Step | Crit | Policy | What it does / why here |
|---|------|------|--------|-------------------------|
| 1 | `fetch_config_bundle` | Critical | apiRetry | S3/Blob fetch of the XMLs + chi.yaml + keeper.yaml; the SHA256 gate is Fatal (a stale hash means the spec does not vouch for these bytes) |
| 2 | `os_optimizations` | Critical | onceFatal | THP off, limits, sysctls — convergent probes first |
| 3 | `require_binaries` | Critical | onceFatal | chctl + clickhouse present; the `aws` CLI check is cloud-gated (an Azure node never has it) |
| 4 | `mount_data_disks` | Critical | bootWait | mount by LABEL (NVMe renames device paths); ownership; the keeper state dirs prepared and chowned |
| 5 | `install_static_configs` | Critical | onceFatal | the bundle's XMLs → `/etc/clickhouse-server/config.d` |
| 6 | `settle_identity` | Critical | claimSweep | the identity contract (below); persists immediately |
| 7 | `configure_peer_hosts` | Critical | onceFatal | `/etc/hosts` entries for writer short names — the replication queue addresses the leader by short name and systemd-resolved does not answer short names |
| 8 | `retrieve_passwords` | Critical | apiRetry | secrets → 0600 files; values never enter state or logs |
| 9 | `configure_tls` | Critical | apiRetry | TLS material from the cloud store, or self-signed interserver-only when no secrets are configured |
| 10 | `configure_server_env` | Critical | onceFatal | `/etc/default/clickhouse-server` + the systemd drop-in; `storage.xml`'s `from_env` endpoint is a start requirement (Code 36), not a warning |
| 11 | `render_node_config` | Critical | onceFatal | the whole node config through chctl's library (below) |
| 12 | `configure_keeper` | BestEffort | onceFatal | keeper nodes: single-file render + myid/raft-id discipline + service enable; no-ops for others |
| 13 | `write_node_env` | BestEffort | onceFatal | the shell-contract file for anything not yet on typed state |
| 14 | `start_clickhouse` | Critical | onceFatal | enable, start, wait for `/ping`, S3-disk health probe; keepers return immediately |
| 15 | `health_gate` | BestEffort | onceFatal | `chctl health` with the CR inventory; non-fatal BY DESIGN so a reader booting while a peer is down still joins |
| 16 | `prometheus_reload` | BestEffort | bootWait | reload + wait for the metrics endpoint; restart fallback |
| 17 | `install_units` | BestEffort | onceFatal | the recurring-job units (below) |
| 18 | `configure_backup` | BestEffort | onceFatal | clickhouse-backup config + daily timer (backup replica only) |
| 19 | `configure_monitoring` | BestEffort | onceFatal | OTEL/Fluent Bit shipping; silently disabled when the endpoint is empty |
| 20 | `install_spot_watch` | BestEffort | onceFatal | readers on spot-capable clouds: the drain daemon |
| 21 | `boot_finished` | Critical | onceFatal | the final timing summary |

Three deliberate orderings, all paid for in production findings:

- **Identity before renders.** A pre-claim hostname bakes into Keeper
  paths and macros and the node joins as the wrong replica.
- **Passwords before renders.** The users render resolves
  `passwordFrom` references at render time through the provider's
  secret source.
- **Health gate non-fatal.** A degraded boot must be visible (the
  verdict prints) but not wedged in a boot loop — the runbooks, not
  the boot, decide whether degraded is acceptable.

### The retry engine and failure taxonomy

Every step runs through `engine.Do(ctx, policy, op)`
(`domain/service/engine.go`): bounded attempts, exponential backoff
with jitter, per-policy deadline. Classification is by error type, not
by step: `fail.Fatal` short-circuits immediately (IAM AccessDenied,
SHA mismatch, a missing image binary — retrying masks a config bug and
burns the boot window), everything else retries under the step's
policy:

```go
// steps.go — the named policies
apiRetry   = Policy{Attempts: 5,  BaseDelay: 2s, MaxDelay: 10s} // transient API ills
bootWait   = Policy{Attempts: 30, BaseDelay: 2s, MaxDelay: 10s} // the system being slow
onceFatal  = Policy{Attempts: 1}                                // config-class: one try, no masking
claimSweep = Policy{Attempts: 3, BaseDelay: 5s, MaxDelay: 5s}    // ordinal claim races
```

Criticality decides what a *failure* means: a Critical step failing
aborts the boot with a non-zero exit (an ASG member then gets
replaced — the node cannot self-heal a broken disk or wrong image);
a BestEffort step logs and continues. The bash era encoded this
distinction as `set +e`/`set -e` toggling and sixteen ad-hoc sleep
loops; here it is one table row per step.

### Convergence and resume: the state contract

`engine.Run` loads `/var/lib/chinit/state.json` once and treats it as
the run's working state:

```go
// domain/service/engine.go
type modelState struct {
    Identity *bootIdentity `json:"identity,omitempty"` // the settled snapshot
    SpecHash string        `json:"spec_hash,omitempty"` // the bundle SHA this run targets
    Steps    []stepRecord  `json:"steps"`                // per-step done/failed records
}
```

- **Resume** — a step recorded `done` is skipped; a crash mid-boot
  re-runs only what is missing.
- **Spec-hash invalidation** — a state recorded under a different
  bundle hash is stale by definition: the configs it installed are not
  what the current release vouches for, so the records wipe and the
  pipeline re-renders.
- **Identity is shared state, not a side file** — `settle_identity`
  persists through the same in-memory object the engine saves per
  step. Two Save paths racing was a real bug (the persister's
  Load+Save clobbered the run's pre-settle copy; the subcommands then
  read "no settled identity" from a file whose node-env proved the
  settle ran). The engine owns one state object for exactly this
  reason.

The subcommands (`schema-sync`, `replica-sync`, `dead-replica-gc`, …)
reload `state.json` and never re-derive identity — the typed version
of the bash era's `node-env` shell contract, which step 13 still
writes for anything not yet migrated to typed state.

## Identity: two modes, one naming contract

`settle_identity` (`steps.go`) is where the cluster's view of "who is
this node" is decided, and it is the step most of the pipeline's
invariants hang off.

**Static mode** (writers, keepers, backup): the private IP's last octet
maps to a role-ordinal (`10.0.1.1x` → `writer-x`, `10.0.1.2x` →
`keeper-x`, `10.0.1.30` → `backup-0`) — `StaticHostname()`. Fixed
machines keep fixed addresses; identity derives from the address.

**Lease mode** (readers, and writers on ASG-only stacks): an ordinal
claim against the provider's lease table (DynamoDB on AWS, Table
Storage on Azure) — a conditional write that succeeds only if the row
is free or its lease expired, renewed by timer, released on spot
drain. The claim ceiling is the role's ASG **max** (`reader_max` /
`writer_max`), not today's count: a scale-out past the baseline must
be able to claim, or it boots forever identity-less.

Both modes then derive the **CR name** — the identity the Altinity
operator's machinery uses (`CRName()` in `boot.go`). The CR layout
names its single shard `01` (never an anonymous zero index — that
re-keys every table's Keeper path), so CR hosts are `01-0..01-(N-1)`
over the full server fleet: writers take `01-0..`, readers continue at
`01-(writer_count + ordinal)..`. Keepers and the backup replica are
not CR members — empty CR name is the signal every consumer keys on.

The settled identity persists **immediately** at claim time
(`PersistIdentity`), before renders and the server start: a crash
later must not lose it, and the job subcommands read it from state.

## The render path: chctl's library, not its CLI

`render_node_config` and `configure_keeper` do not shell out to
chctl. They call the same code paths the `chctl` CLI resolves to —
`SpecLoader`, `NodeRenderer`, `ConfigRenderer` from
`pkg/chctl/infra/operator` — as a library:

```
BootSpec ──► LoadCHI/LoadCHK ──► NodeRenderer.RenderNode(...)
                                    │  Hosts:       CR-name → DNS-address map
                                    │  Replicas:     CR-name → short-name map ({replica} macro)
                                    │  Networks:     per-user allowlists + subnet CIDR
                                    │  RemoteUser/Password: distributed-query credentials
                                    │  Keeper:      single-file mode, RaftIDs, ServerID
                                    ▼
                       config.d/users.d XML — staged, validated, chowned,
                       0640/0644 by credential-bearing-ness, manifest-pruned
```

Why each map exists (every one is a live finding):

- `Hosts` keyed by CR name — key by hostname and `remote_servers`
  renders the operator's unresolvable pod-DNS pattern.
- `Replicas` — without it the `{replica}` macro renders the pod
  pattern; the table registers under a Keeper path no cloud tooling
  recognizes and `SYSTEM SYNC/DROP REPLICA` operate on the wrong
  identity.
- `Networks` with the **node subnet CIDR merged in** — the render
  defaults to localhost-only; the cluster's own peers arrive from the
  subnet and every cross-node query dies Code 516 otherwise.
- `RemoteUser/RemotePassword` — spliced into `remote_servers`
  `<replica>` entries; without them nodes authenticate as passwordless
  `default` against nodes whose `default` requires a password.
- Keeper mode renders **one merged file** (`-single-file` semantics):
  `clickhouse-keeper` runs with an explicit `--config`, so config.d
  auto-include is disabled and the node needs a single complete
  document — with 1-based raft IDs (the myid-disk convention; the CR's
  zero-based indices render `<id>0</id>` and keeper exits Code 568:
  "our server id 1 not found in raft_configuration") and the
  root-level `listen_host` from the spec's `files:` section
  (`keeper_server/listen_host` is silently ignored — the config is
  rendered, the lint warns, the section never applies).

Keeper identity is **cloud state, not render state**: `server_id`
comes from the raft disk's `myid` file when it exists (re-image
safety — the ensemble's identity outlives the instance), is seeded
1-based when it does not, and a stale `0` is never honored.

## The recurring jobs: subcommands as the unit interface

Everything the bash era's cron litter and embedded-bash systemd units
did after boot is a `chinit` subcommand. Units are one `ExecStart`
line — logic lives in testable Go:

| Unit (service/timer) | ExecStart | Roles | Cadence |
|---|---|---|---|
| `schema-sync` | `chinit schema-sync -retries 3 -prune-stale-self` | writer, reader | hourly |
| `replica-sync` | `chinit replica-sync` | writer, reader | post-boot |
| `keeper-health` | `chinit keeper-health` | keeper | timer |
| `lease-renew` | `chinit lease-renew` | leased readers | every 5 min |
| `dead-replica-gc` | `chinit dead-replica-gc` | writer | weekly |
| `orphaned-data-gc` | `chinit orphaned-gc` | writer | weekly |
| `clickhouse-backup-daily` | `chinit backup-daily` | backup replica | daily 02:00 |
| `chinit-spot-watch` | `chinit spot-watch` | spot readers | daemon |

Each subcommand is a thin orchestration over the **chctl CLI** for
cluster operations (`chctl schema sync`, `chctl replica gc`, `chctl
keeper mntr` — one process per job, typed exit codes, the units'
contract) and over the provider for cloud operations (lease renewal,
DNS cleanup). They reload identity from `state.json`; none re-derive
it.

## The conformance suite: one contract, every cloud

`domain/service/conformance` is the provider contract as executable
scenarios: metadata probes, secret resolution, bundle fetch with the
SHA gate, ordinal claim/renew/release, DNS upsert, spot notices —
run per registered provider against `infra/fakes` (canned IMDS,
canned secrets, a canned bundle). Optional-capability scenarios
**skip** for providers that lack the capability; the mandatory core
must pass everywhere.

Adding a cloud = one `infra/<cloud>` package + one suite entry. The
scenarios never change — that is the point: the factory's contract is
pinned by a test, not by convention.

## Invariants worth knowing before you change something

1. **Identity precedes rendering.** Any step that touches config must
   run after `settle_identity`; anything that derives a name must read
   the settled identity, never re-derive it.
2. **The shard's name is load-bearing.** `01` expands into every
   ReplicatedMergeTree Keeper path; an anonymous layout re-keys the
   cluster's data.
3. **Keeper IDs are 1-based and disk-owned.** myid file wins; a fresh
   disk seeds ordinal+1; zero is never valid.
4. **Exclusive resources order destroy-then-create** — the static IPs,
   the raft volume, the quorum seat. The IaC that drives node
   replacement must not let create-before-destroy flags propagate to
   these instances (the InvalidIPAddress.InUse class of failure).
5. **Fatal is a design decision, not an escape hatch.** If a retry
   would mask a config bug, classify Fatal; if a failure should stop
   the boot, mark the step Critical. The matrix is the product.
6. **The spec is data; the binary is logic.** Behavioral changes ship
   as a new chinit release (the golden image bakes it, checksummed);
   the spec changes what the same binary targets. Never smuggle logic
   into the spec template.

## Where to look in the tree

```
cmd/chinit/main.go               entry point
pkg/chinit/app/
  app.go                          subcommand dispatch + version
  boot.go                         boot orchestration, provider wiring, state store
  subcommands.go                  the recurring jobs + the chctl CLI seam
pkg/chinit/domain/
  model/spec.go                   BootSpec + validation (the data contract)
  port/provider.go                Provider, capabilities, registry, probes
  service/steps.go                the pipeline table, identity, TLS, env
  service/engine.go               retry Do(), state store, shared-state discipline
  service/render.go               chctl-library renders (node + keeper)
  service/units.go                unit composition per role
  service/conformance/            the provider contract suite
  fail/                           Retryable/Fatal classification
pkg/chinit/infra/
  detect/                         flag/env/DMI/IMDS detection
  system/                         disks, ostune, configs, units — the machine
  aws/, azure/                    one package per cloud
  fakes/                          the conformance doubles
```
