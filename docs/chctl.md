# chctl — the operator's control plane, without Kubernetes

`chctl` is a single binary that gives you the Altinity operator's behavior
on **any** ClickHouse deployment — VMs, EC2 Auto Scaling Groups, VMSS,
bare metal — no Kubernetes required. It renders node configuration from a
CHI/CHK spec and runs the schema/replica/keeper operations the operator
runs on-cluster, using the exact same code paths.

## Why you'd use it

| You have | chctl replaces |
|---|---|
| Hand-written ClickHouse XML config templates | `chctl config render` — one CHI spec is the source of truth; the operator's generator produces the XML |
| Hand-written keeper `config.xml` (raft list, `server_id`, logging) | `chctl config render -keeper -single-file` — the keeper generator emits ONE complete `<clickhouse>`-rooted file per node |
| Boot scripts creating tables on new replicas | `chctl schema sync` — the operator's schema propagation (CREATE ... IF NOT EXISTS against `clusterAllReplicas`) |
| Stale replicas after scale-in / spot reclaim | `chctl replica drop` — the operator's `SYSTEM DROP REPLICA` cleanup |
| Scheduled bash loops that detect + drop stale replicas | `chctl replica gc` — detection and drop in one pass, behind a writer/keeper-safe name gate |
| Runbook instructions "restart keepers one at a time" | `chctl keeper safe-to-stop` — a hard quorum gate; refuses (exit 2) if the stop would drop Raft majority |
| `echo mntr \| nc host 9181 \| awk` in health scripts | `chctl keeper mntr` — parsed, sorted key=value (or `-json`) with a real exit code; the metric push stays in the caller |
| Ad-hoc verification SQL + mntr gates in boot/refresh/runbooks | `chctl health` — one command checks server reachability (every host), replica writability, keeper mntr, spec tables and self registration, with pass/fail/skip per check and exit `0`/`3` |
| Passwords baked into spec files or argv | `chctl config render -user-password` / `-remote-user` — files and env supply the secrets, the spec only declares the accounts |

Anything you can express in a `ClickHouseInstallation`/`ClickHouseKeeperInstallation`
spec works here — profiles, quotas, users, custom config files — and it is
validated exactly as on-cluster (including the 0.27.4 `k8s_secret_`
syntax rejection).

## Get the binary

```bash
# from a GitHub release of this repo
CHCTL_VERSION=v0.1.0
curl -fsSL -o chctl \
  "https://github.com/sks/clickhouse-operator/releases/download/chctl-${CHCTL_VERSION}/chctl-linux-arm64"
chmod +x chctl

# or build from source (Go ≥ per go.mod)
go build -o chctl ./cmd/chctl
```

Verify a downloaded binary:

```bash
curl -fsSL -o SHA256SUMS \
  "https://github.com/sks/clickhouse-operator/releases/download/chctl-${CHCTL_VERSION}/chctl-SHA256SUMS"
sha256sum -c --ignore-missing SHA256SUMS
```

## 1. Render node configs from one spec

Write your cluster once as a CHI spec (cloud-neutral subset — topology,
settings, users, profiles, files; see
[docs/chctl-example-chi.yaml](chctl-example-chi.yaml)):

```yaml
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: logs
spec:
  configuration:
    clusters:
      - name: logexplorer
        layout: { shardsCount: 1, replicasCount: 2 }
    profiles:
      default/async_insert: "1"
```

Render the config files the operator would put in ConfigMaps — straight to
the directories ClickHouse reads:

```bash
chctl config render \
  -spec cluster.yaml \
  -host 0-0 \
  -out /etc/clickhouse-server/config.d \
  -users-dir /etc/clickhouse-server/users.d
```

```
common: chop-generated-remote_servers.xml
common: chop-generated-settings.xml
users: chop-generated-profiles.xml
users: chop-generated-users.xml
host: chop-generated-hostname-ports.xml
host: chop-generated-macros.xml
```

Each host gets its own macros/remote_servers — pass a bare `-host 0-0`
(the CR host name, `<shard>-<replica>`) per node from your node bootstrap
(cloud-init, ignition, whatever). Drop `-host` to render only the
cluster-wide sections. Keeper specs render the same way with `-keeper`.

**Wire it into node bootstrap** (the pattern used by the AWS EC2 stack in
`appcd-dev/langchain-examples`): upload the spec to S3/blob storage,
download it in cloud-init, render per-role at boot, and re-render + restart
on config change instead of rebuilding instances.

### Cloud inventory and replica identity (`-host`, `-replica`)

`-host` is dual-form. A **bare name** selects the host whose files render.
A **`name=address`** pair adds a cloud inventory entry — and every place
the render would otherwise emit an unresolvable Kubernetes pod pattern,
the cloud address comes out instead:

- `remote_servers` `<host>` entries
- keeper raft `<hostname>` entries
- `interserver_http_host`

```bash
chctl config render \
  -spec cluster.yaml \
  -host 0-0 \
  -host 0-0=writer-0.ch.internal \
  -host 0-1=writer-1.ch.internal \
  -out /etc/clickhouse-server/config.d
```

`-replica name=cloudname` (repeatable) maps the same CR host to its
**replica identity** — the value `macros.xml`'s `<replica>` must carry.
On an existing cluster this is not cosmetic: Keeper replica paths and
per-replica S3 backup prefixes encode `<replica>`, so a re-render that
changes it silently re-identifies the node. Without the mapping the
operator's pod pattern (`chi-logs-logexplorer-0-0`) lands in macros —
correct on Kubernetes, an identity break off-cluster.

Before/after in `chop-generated-macros.xml` with `-replica 0-0=writer-0`:

```xml
<!-- operator-native (no mapping) -->
<replica>chi-logs-logexplorer-0-0</replica>

<!-- with -replica 0-0=writer-0: the cloud short hostname exactly -->
<replica>writer-0</replica>
```

Unmapped hosts keep operator-native output; a render with no mappings at
all is byte-identical to the pre-inventory behavior.

### Distributed-query credentials (`-remote-user`)

Rendered `remote_servers.xml` carries one `<replica>` per host. Peers
authenticate distributed queries against it — without credentials each
peer connects as passwordless `default` and gets `PASSWORD_REQUIRED`.
Pass the account once; chctl splices the XML-escaped `<user>/<password>`
pair into every `<replica>` of every rendered cluster:

```bash
chctl config render \
  -spec cluster.yaml -host 0-0 \
  -host 0-0=writer-0.ch.internal -host 0-1=writer-1.ch.internal \
  -remote-user distuser -remote-password-env CH_REMOTE_PASSWORD \
  -out /etc/clickhouse-server/config.d
```

`-remote-password-env <VAR>` and `-remote-password-file <path>` are
mutually exclusive; the password never appears in argv, the spec, or
logs. Rendered files are written `0640` — they can carry credentials.

### Runtime user passwords (`-user-password`)

The spec declares **who exists**; the secret store supplies **what they
authenticate with**. `-user-password name=path` (repeatable) reads each
file, sha256-hashes it, and emits it as that user's `password_sha256_hex`
— the spec never carries a runtime secret:

```bash
# your secret store drops per-role files at boot
chctl config render \
  -spec cluster.yaml -out /etc/clickhouse-server/config.d \
  -user-password admin=/run/secrets/admin.pass \
  -user-password writer=/run/secrets/writer.pass
```

Guards: the spec must declare the user (a typo fails the render instead
of silently keeping the old password), and any spec-declared password
for that user is stripped so the injected hash is the only credential
in play. Trailing newlines are trimmed before hashing.

### Keeper mode: one file, real identities (`-single-file`, `-server-id`, `-raft-id`)

Keeper nodes run with an explicit `--config`, which **disables**
ClickHouse's `config.d` auto-include — a per-group render is useless
there. `-single-file <path>` merges the common + host sections into ONE
complete `<clickhouse>`-rooted config instead (and skips `-out`):

```bash
chctl config render \
  -spec keeper.yaml -keeper \
  -host 0-1 \
  -host 0-0=keeper-0.aws.internal \
  -host 0-1=keeper-1.aws.internal \
  -host 0-2=keeper-2.aws.internal \
  -server-id 2 \
  -raft-id 0-0=1 -raft-id 0-1=2 -raft-id 0-2=3 \
  -single-file /etc/clickhouse-keeper/keeper_config.xml
```

producing:

```xml
<clickhouse>
    <keeper_server>
        <server_id>2</server_id>
        <raft_configuration>
            <server>
                <id>1</id>
                <hostname>keeper-0.aws.internal</hostname>
                <port>9444</port>
            </server>
            <server>
                <id>2</id>
                <hostname>keeper-1.aws.internal</hostname>
                <port>9444</port>
            </server>
            ...
```

`-server-id <n>` and `-raft-id name=id` (repeatable) exist for
**existing ensembles**: the CR's zero-based replica indices are fine
for a freshly created ensemble, but an ensemble already running has its
member ids persisted in raft logs (typically 1-based, whatever the
bootstrap wrote). A re-imaged keeper presenting a CR-derived id looks
like a different member to its peers, breaking Raft recovery:

- `-server-id` overrides this node's `<server_id>` with the persisted
  identity (the `myid` file's value). `0`/unset keeps the CR index.
- `-raft-id name=id` rewrites the `raft_configuration` `<server><id>`
  values for the whole ensemble; unmapped hosts keep CR-derived ids.
- consistency guard: `-server-id` must agree with the selected host's
  `-raft-id` — a node with two identities is refused before any rewrite.

These flags are keeper-mode only: against a CHI spec they are refused
(`-single-file applies to keeper mode only: add -keeper`, etc.), and
`-single-file`/`-server-id` require a `-host` selection (a single file
is host-complete).

#### The `keeper_server/listen_host` gotcha (render-time warning)

clickhouse-keeper **ignores** `keeper_server/listen_host` — `listen_host`
is a ROOT-level setting. Specifying it nested under `keeper_server`
renders fine but leaves the client port on `127.0.0.1`: no fleet node
can reach `mntr` or replication. Live finding: the whole ensemble was
unreachable until the setting moved to a root-level `files` entry.

`chctl config render -keeper` warns at render time when it sees the key:

```
WARNING: keeper_server/listen_host is IGNORED by clickhouse-keeper (it is a
ROOT-level setting; put <listen_host> in a root-level files entry in the
spec) — the rendered config will not listen as intended (hosts: 0-0, 0-1)
```

The fix is in the spec, not the flag list — a root-level `files` entry:

```yaml
spec:
  configuration:
    settings:
      keeper_server/tcp_port: "9181"
    files:
      listen.xml: |
        <clickhouse>
            <listen_host>0.0.0.0</listen_host>
        </clickhouse>
```

The render is never failed on this key (output stays valid) — the
warning is the early signal, not the gate.

## 2. Bring up a new replica with the schema already in place

A scaled-out reader must re-create every table before serving traffic.
Inventory keys are CR host names (`<shard>-<replica>`) mapped to cloud
addresses; the positional target accepts either a CR name or a cloud
name, and the positional and the flags may appear in any order:

```bash
chctl schema sync \
  -spec cluster.yaml \
  -cluster logexplorer \
  -host 0-0=writer-0.ch.internal \
  -host 0-1=reader-1.ch.internal \
  -user default -password "$CH_PASSWORD" \
  0-1
```

This runs the operator's `HostCreateTables` — replicated and distributed
objects get created on the target host from cluster peers.

### `-retries` — deterministic MV-target ordering (default 2)

The operator's per-SQL retry re-runs failed statements, but ordering
across a *fresh pass* is not guaranteed. Live finding: one materialized
view was left uncreated because its `TO`-target table landed later in
the same batch — a manual second run created it. `-retries N` (default
`2`, min `1`) runs the full create pass up to N times, stopping early on
the first clean pass:

```bash
chctl schema sync -spec cluster.yaml -cluster logexplorer ... reader-0 -retries 3
```

### `-prune-stale-self` — reclaim a recycled ordinal's stale znode

An ASG/VMSS replacement node re-claims its ordinal (say `reader-0`). If
the dead predecessor's Keeper registration survived — a PERSISTENT
znode — the fresh node's `CREATE` fails with Code 253 (`Replica
/clickhouse/tables/.../replicas/reader-0 already exists`), the auto
`ATTACH` fallback fails too (fresh disk, nothing to attach), and the
node stays schemaless until someone runs `replica drop reader-0` on a
peer.

`-prune-stale-self` (default `false`, opt-in) automates that recovery:
when the create pass fails with a 253 for the **target's own replica
name**, chctl runs the same `SYSTEM DROP REPLICA` + `DROP DATABASE
REPLICA` pair `replica drop` uses — on the target itself, for exactly
the name the failed pass reported — then retries the create pass:

```bash
# ASG replacement node bootstrapping as reader-0, whose dead
# predecessor left its znode behind:
chctl schema sync -spec cluster.yaml -cluster logexplorer ... reader-0 -prune-stale-self
```

Safe by construction: it only ever drops the replica name the target
itself is about to claim (its OWN identity), never any other host's,
and only after the CREATE pass reports that the registration already
exists. Use it on replacement nodes; it is destructive-adjacent, so it
stays opt-in.

## 3. Clean up after a dead node

After spot reclaim or scale-in, drop the stale replica path from Keeper so
DDL and future re-joins stay reliable:

```bash
chctl replica drop \
  -spec cluster.yaml \
  -on 0-0 \
  -cluster logexplorer \
  -host 0-0=writer-0.ch.internal \
  -host 0-1=reader-1.ch.internal \
  -user default -password "$CH_PASSWORD" \
  reader-1
```

The positional name goes into `SYSTEM DROP REPLICA '<name>'` verbatim, so
it must be the identity your deployment's macros declare — the cloud
name (`writer-1`, `reader-3`), not the CR ordinal. Addressing (the
positional for `sync`, and `-on`) accepts either form: the CR name or a
cloud name. After a planned restart, re-queue replication:

```bash
chctl replica sync \
  -spec cluster.yaml \
  -cluster logexplorer \
  -host 0-0=writer-0.ch.internal \
  -user default -password "$CH_PASSWORD" \
  0-0
```

### Sweep stale replicas unattended (`replica gc`)

`replica gc` does the whole loop in one pass: it detects names registered
in the peer's `system.replicas` but absent from live cluster membership
(`clusterAllReplicas` with `skip_unavailable_shards=1`, so one down peer
doesn't fail the sweep), then drops each candidate with the same
`SYSTEM DROP REPLICA` + `DROP DATABASE REPLICA` pair `replica drop` uses.

```bash
chctl replica gc \
  -spec cluster.yaml \
  -on writer-0 \
  -cluster logexplorer \
  -host 0-0=writer-0.ch.internal \
  -host 0-1=writer-1.ch.internal \
  -name-pattern 'reader-*'
```

Dropped names print to stdout as TSV — one per line — so pipes and logs
keep their existing format; per-host failures and the
`gc: N stale replica(s) detected, N dropped, N failed` summary go to
stderr. A per-host failure doesn't abort the sweep. Exit 3 means an
operational failure — unreachable peer, invalid pattern, or at least one
drop that failed; missing required flags exit 1.

`-name-pattern` is the safety gate: only hosts matching it (default
`reader-*`, a `path.Match` glob) are dropped. Non-matching stale
registrations — a writer, anything else — are reported but never
dropped: a gate that silently reaped a writer registration would be
worse than the garbage it removes.

A systemd timer that runs it hourly on a writer:

```ini
# /etc/systemd/system/dead-replica-gc.service
[Unit]
Description=Drop stale reader replica registrations (chctl)

[Service]
Type=oneshot
EnvironmentFile=/var/lib/clickhouse-bootstrap/env
ExecStart=/usr/local/bin/chctl replica gc \
  -spec /var/lib/clickhouse-bootstrap/chi.yaml \
  -on writer-0 -cluster logexplorer \
  -host 0-0=writer-0.ch.internal -host 0-1=writer-1.ch.internal \
  -user admin -password ${CH_PASSWORD} \
  -name-pattern 'reader-*'
```

```ini
# /etc/systemd/system/dead-replica-gc.timer
[Timer]
OnCalendar=hourly
Persistent=true

[Install]
WantedBy=timers.target
```

## 4. Gate keeper maintenance on Raft quorum

`safe-to-stop` checks the ensemble's live Raft state (via Keeper's `mntr`
4lw) and exits with a verdict — designed for ASG lifecycle hooks, spot
drain handlers and VMSS terminate-notification scripts:

```bash
if chctl keeper safe-to-stop -keeper 10.0.3.10:9181; then
  # quorum survives: release the node
  proceed_with_termination
else
  case $? in
    2) hold_the_hook ;;   # gate held: quorum would drop — retry later
    3) alert_and_hold ;;  # operational error: keeper unreachable
  esac
fi
```

Exit codes: `0` safe · `2` quorum gate held the node · `3` operational
error. It waits up to `-wait-timeout` (default 2m, the operator's budget)
for a flapping follower to re-sync before refusing. Ensembles smaller than
3 are never gated (a 2-member ensemble can't survive any planned stop;
holding it would just block maintenance).

### Inspect a keeper from scripts (`keeper mntr`)

`keeper mntr` fetches the same payload `safe-to-stop` reasons over and
prints it parsed: sorted `key=value` lines, or one JSON object with
`-json`. Exit `0` healthy, `3` unreachable/empty — the same contract as
`safe-to-stop`, so a health script needs no special-casing:

```bash
chctl keeper mntr -keeper 10.0.3.10:9181
# zk_approximate_data_size=1024
# zk_avg_latency=1
# zk_followers=2
# ...

chctl keeper mntr -keeper 10.0.3.10:9181 -timeout 2s -json
# {"zk_approximate_data_size":"1024", ...}
```

It carries no decision logic — it replaces the
`echo mntr | nc host 9181 | awk` plumbing in node bootstrap and health
scripts. Pushing to CloudWatch/metrics stays in the caller:

```bash
if payload=$(chctl keeper mntr -keeper "$KEEPER" -json); then
  aws cloudwatch put-metric-data --namespace "ClickHouse/Keeper" \
    --metric-name avg_latency \
    --value "$(echo "$payload" | jq -r .zk_avg_latency)"
fi
```

## 5. Verify the whole stack in one pass (`health`)

`chctl health` is the one verification command that gates boots, config
refreshes and runbooks — it replaces the ad-hoc SQL snippets
(`clusterAllReplicas ... is_readonly`, `system.replicas` greps) and the
separate `keeper mntr` invocation each of those carried:

```bash
chctl health \
  -cluster ch-int-dev \
  -host 01-0=writer-0.ch.internal \
  -host 01-1=reader-0.ch.internal \
  -spec /var/lib/clickhouse-bootstrap/chi.yaml \
  -keeper keeper-0.ch.internal:9181 \
  -user admin -password "$(cat /var/lib/clickhouse-bootstrap/admin-password)"
```

```
✓ server: 2/2 hosts up
✓ keeper: leader (keeper-0.ch.internal:9181)
✓ replicas: 3 replicated table(s), none readonly
✓ tables: 3 expected table(s) present on 2 host(s)
SKIP self: no -self given
status: healthy (4 pass, 0 fail, 1 skip)
```

Checks (each reports pass/fail/skip; a skip is an optional check that
was not requested and never counts against health):

1. **server** — every `-host` inventory entry answers SQL (`SELECT 1`
   over the same driver path the mutating commands use). A cluster
   health command fans out over the full inventory, not the first
   reachable host — a down reader behind a healthy writer must fail,
   not hide behind the load balancer's blind spot.
2. **keeper** (with `-keeper <host:port>`) — mntr reachable and
   `zk_server_state` non-empty; the state (leader/follower/standalone)
   is reported, not judged — a healthy ensemble HAS followers.
3. **replicas** — `system.replicas` across the cluster
   (`clusterAllReplicas` with `skip_unavailable_shards=1`, the same
   fan-out the schemer's stale-replica detection uses): any
   `is_readonly` row fails the check naming the table, replica and
   host. `skip_unavailable_shards` tolerates down peers — the server
   check already named them.
4. **tables** (with `-spec`) — the table names the spec's `.sql`
   `files:` entries declare (the operator's own initdb pattern for
   schema DDL), compared against existing tables; a table present on
   ANY host counts as present (schema sync materializes per-host, and
   a fresh reader may lag — which `-self` covers). `-spec` also gates
   `-cluster` against the CR's declared clusters, same as `replica gc`.
5. **self** (with `-self <replica>`) — the named replica (the
   `{replica}` macro value, e.g. `reader-0`) appears in the replicas
   fan-out AND is active — the post-boot gate for a fresh reader:
   tables created but registration absent means the node is not
   serving replication yet.

Exit `0` all requested checks pass · `3` any check degraded · `1`
usage error. `-json` emits the stable wire form scripts pin on:

```json
{"status":"degraded","checks":[{"name":"server","status":"fail","detail":"..."},{"name":"keeper","status":"skip","detail":"no -keeper given"},...]}
```

Wire it into node bootstrap's final step, the config-refresher's
post-step, and the cluster-health runbook's verification section
(`if chctl health ...; then` gates the same way `safe-to-stop` does).

## Command reference

```
chctl config render  -spec chi.yaml [-host 0-0] [-host name=address]... [-replica name=cloudname]...
                      [-user-password name=path]... [-out config.d] [-users-dir users.d] [-dry-run] [-check] [-keeper]
                      [-remote-user <name> -remote-password-env <VAR> | -remote-password-file <path>]
                      keeper mode: [-keeper] -host <host> [-server-id <n>] [-raft-id name=id]... [-single-file <path>]
chctl schema sync    <host> -cluster <name> -host <name=addr>... [-user u] [-password p] [-port n] [-secure]
                      [-retries 2] [-prune-stale-self]
chctl replica drop   <host> -on <peer> -cluster <name> -host <name=addr>...
chctl replica sync   <host> -cluster <name> -host <name=addr>...
chctl replica gc     -on <peer> -cluster <name> -host <name=addr>... [-name-pattern reader-*]
chctl keeper safe-to-stop -keeper <host:port> [-members n] [-wait-timeout 2m]
chctl keeper mntr    -keeper <host:port> [-timeout 5s] [-json]
chctl health    -cluster <name> -host <name=addr>... [-user u] [-password p] [-port n] [-secure]
                [-spec chi.yaml] [-keeper <host:port>] [-self <replica>] [-json] [-timeout 5s]
chctl version
```

All schema/replica commands require `-spec` and take the full host
inventory as repeated `-host name=address` flags (they need peer
addresses to run cluster SQL). The positional host argument and the
flags may appear in **any order** — chctl pre-scans the tokens before
parsing, so `chctl replica drop reader-0 -cluster x ...` works the same
as flags-first. Passwords via `-password` (prefer env/secrets over
argv); never bake them into the spec — specs carry references, secret
stores carry values.

`chctl version` prints the binary's version (`chctl <version>`, plus the
Go toolchain when available) — releases inject the tag version at build
time, so node bootstrap can compare the installed binary against the
targeted release instead of unconditionally overwriting.

Render flags in detail:

| Flag | Applies to | Meaning |
|---|---|---|
| `-spec chi.yaml` | all renders | CHI (or CHK with `-keeper`) spec, required |
| `-host 0-0` | all renders | bare name: select the host whose host-scoped files render |
| `-host name=address` | all renders | repeatable cloud inventory entry; `remote_servers`, keeper raft and `interserver_http_host` resolve to the cloud address |
| `-replica name=cloudname` | CHI | repeatable; macros `<replica>` becomes the cloud short hostname — the replica identity |
| `-user-password name=path` | CHI | repeatable; file is read, sha256-hashed, injected as `password_sha256_hex` |
| `-remote-user` + `-remote-password-env` \| `-remote-password-file` | CHI | escaped `<user>/<password>` into every `remote_servers` `<replica>`; env/file mutually exclusive |
| `-keeper` | — | spec is a `ClickHouseKeeperInstallation` |
| `-server-id <n>` | keeper | overrides the CR's zero-based replica index with the persisted `myid` identity |
| `-raft-id name=id` | keeper | repeatable; the ensemble's persisted raft ids; must agree with `-server-id` for the selected host |
| `-single-file <path>` | keeper | ONE merged `<clickhouse>`-rooted config (common+host); requires `-host` |
| `-keeper-address crName=address` | keeper | repeatable keeper-only inventory; same raft `<hostname>` effect as the shared `-host` inventory |
| `-out`, `-users-dir`, `-dry-run` | all renders | config output dir, users group dir, print without writing |
| `-check` | all renders | byte-compare the would-be-rendered files against `-out` (users group against `-users-dir`) without writing; prints `IN SYNC` / `DRIFTED <file>` / `MISSING <file>` per file, exits `0` in sync / `4` on drift — the config refresher and health runbook's probe |

Schema/replica flags in detail:

| Flag | Applies to | Meaning |
|---|---|---|
| `-retries 2` | `schema sync` | full create passes (min 1); early-stops on the first clean pass. Covers the MV whose `TO`-target lands later in the same batch |
| `-prune-stale-self` | `schema sync` | opt-in: on a Code 253 (the target's own registration already exists — a recycled ordinal), drop that registration and retry. Only ever the target's own name |
| `-on <peer>` | `replica drop`, `replica gc` | host that executes the SQL (either CR or cloud name) |
| `-name-pattern reader-*` | `replica gc` | the safety gate: only matching stale hosts are dropped |
| `-spec chi.yaml` | `health` (optional) | enables the tables-present check (expected names from the spec's `.sql` `files:`) and whitelists `-cluster` against the CR's declared clusters |
| `-keeper <host:port>` | `health` (optional) | enables the keeper mntr check; the state is reported, not judged |
| `-self <replica>` | `health` (optional) | enables the self-registration check for this node's `{replica}` macro value (works with the full multi-host inventory) |
| `-json` | `health` | machine-readable `{"status":...,"checks":[{name,status,detail}...]}` instead of the human lines |
| `-timeout 5s` | `health` | per-probe timeout (ping, each SQL query, mntr) |

Exit codes: `0` ok · `1` usage error · `2` quorum gate held the node
(`safe-to-stop` only) · `3` operational error — keeper unreachable
(`keeper` subcommands), an unreachable peer or a failed drop
(`replica gc`), any degraded check (`health`) · `4` config drift detected (`config render -check`
only): some rendered file is missing or different on disk; nothing was
written.

## Release / distribution

Releases are published from this repository (tag `chctl-v*`) with binaries
for `linux/arm64`, `linux/amd64`, `darwin/arm64`, `darwin/amd64` plus
`SHA256SUMS`. The workflow at `.github/workflows/release-chctl.yaml` runs
the full test suite before publishing — a tag that fails its tests
produces no release.
