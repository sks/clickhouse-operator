# chinit — cloud-adaptive node bootstrapper

> Contributor-facing internals — layers, pipeline, identity contract,
> the conformance suite — live in
> [chinit-architecture.md](chinit-architecture.md). This page is the
> operator's manual.

`chinit` is a single Go binary that bootstraps a ClickHouse node on **any**
cloud: it detects the platform it runs on (AWS, Azure; more via the provider
factory), fetches its configuration bundle, converges the node to a
declarative spec and installs its own recurring maintenance jobs as
subcommands. It replaces the 3,170-line Terraform-templated `cloud-init.sh`
stage-2 script **and** the `bootstrap.sh` stage-1 downloader — the entire
bootstrap is one binary plus one small YAML spec.

## Why you'd use it

| The bash era | chinit |
|---|---|
| ~40 Terraform variables interpolated into 3,170 lines of shell at template time | `chinit.yaml` — a typed, validated spec fetched (SHA256-gated) from object storage; the binary is identical across roles and clouds |
| ~16 ad-hoc `sleep` retry loops, `set -e` toggling between fatal and best-effort | A retry engine: Retryable/Fatal error classification, exponential backoff with jitter, per-step deadlines, Critical/BestEffort criticality |
| A re-run redid everything (or failed halfway) | Convergence: every step probes real system state first; `state.json` records done steps and resumes; a new bundle hash invalidates and re-renders |
| systemd units whose ExecStart were *more embedded bash* (schema-sync, replica-sync, spot drain, lease renewal, keeper health, GCs, backup) | `chinit schema-sync`, `replica-sync`, `lease-renew`, `keeper-health`, `dead-replica-gc`, `orphaned-gc`, `backup-daily`, `spot-watch` — units are one ExecStart line, all logic testable Go |
| Cloud CLI calls shelled out (aws/az) with hand-built JSON | Provider adapters over the SDKs: Secrets Manager/Key Vault, DynamoDB/Table Storage ordinal leases, Route53/Private DNS, S3/Blob bundle fetch, IMDSv2 |
| Nothing testable | Unit tests per package + a **provider conformance suite**: one contract, run per cloud against fake IMDS/canned secrets/canned bundle |
| Azure port = copy 2,461 lines of bash and swap `az` for `aws` | One `infra/azure` package implementing the same ports; GCP/Civo are one package + one registry line each |

## How it adapts to the cloud

Detection runs in precedence order: `-cloud` flag → `CHINIT_CLOUD` env →
DMI `sys_vendor` signature (`Amazon`/`Microsoft`) → IMDS probe. The first
hit builds the provider from the registry:

```go
// pkg/chinit/domain/port/provider.go
type Provider interface {
    Name() string
    MetadataProvider          // instance id, region, private IP — mandatory
    port.SecretSource          // the shared secrets port
    ConfigSource               // SHA-gated bundle fetch — mandatory
}
// Optional capabilities, probed at boot; nil = disabled with a WARNING:
type OrdinalClaimer interface { ClaimOrdinal(...); RenewLease(...); ReleaseLease(...) }
type DNSRegistrar interface  { ResolveZoneID(...); UpsertARecord(...); DeleteARecord(...) }
type SpotSignals interface    { Watch(ctx) <-chan InterruptionNotice }
```

A cloud without a capability (Civo has no IMDS; a provider without managed
DNS) still boots — the pipeline degrades loudly instead of failing.

## The boot

`chinit boot` runs 18 convergent steps in a load-bearing order (identity
settles **before** renders — the "ip-10-0-1-101 never joined the cluster"
production finding; passwords land **before** the render; keepers skip the
server start; the health gate is non-fatal so a reader booting while a peer
is down still joins):

```
fetch_config_bundle → os_optimizations → require_binaries → mount_data_disks
→ install_static_configs → settle_identity → retrieve_passwords
→ render_node_config (in-process library — no process boundary)
→ configure_keeper → write_node_env → start_clickhouse → health_gate
→ prometheus_reload → install_units → configure_backup → configure_monitoring
→ install_spot_watch → boot_finished
```

The settled identity (hostname, CR name, lease, DNS zone) is persisted to
`/var/lib/chinit/state.json` at claim time — the job subcommands reload
it, never re-derive (the typed version of the bash node-env contract).

## The spec

`chinit.yaml` (rendered by Terraform per role, uploaded to the bundle
prefix, fetched by chinit itself):

```yaml
role: reader
identity_mode: lease        # static (IP-octet map) or lease (ordinal claim)
cluster_name: logexplorer
config_bundle:
  bucket: data-bucket
  prefix: bootstrap/clickhouse
  sha256: <config_files_hash>   # the gate; a stale hash is Fatal
disks:
  metadata_label: ch-metadata   # mounted by LABEL, never device path
secrets:                        # NAMES only; values resolve via the
  default_password: logexplorer-default   # instance role at boot
region: us-east-1
```

## Get the binary

```bash
CHINIT_VERSION=v0.1.0
curl -fsSL -o chinit \
  "https://github.com/sks/clickhouse-operator/releases/download/chinit-${CHINIT_VERSION}/chinit-linux-arm64"
chmod +x chinit
# supply-chain gate:
curl -fsSL "https://github.com/sks/clickhouse-operator/releases/download/chinit-${CHINIT_VERSION}/chinit-SHA256SUMS" | grep arm64 | sha256sum -c
```

Releases build from the `chinit-v*` tag workflow (vet+test gate, stripped
static binaries for linux/darwin arm64+amd64). The golden AMI bakes a
pinned version; the user-data wrapper upgrades from the staged
`bootstrap/artifacts/chinit/<version>/` prefix with the same sha256 gate
the cluster-ops port uses.

## Wiring into node boot (the langchain-examples stacks)

EC2/VM user_data shrinks to a ~1.5KB wrapper:

1. ensure the pinned chinit binary (upgrade-or-keep, checksum-gated)
2. fetch the role's `chinit-<role>.yaml` from the bundle prefix
3. `exec chinit boot -spec /var/lib/clickhouse-bootstrap/chinit.yaml -cloud aws`

Everything the bash era did at boot — and everything its cron/systemd
litter did after — is chinit now.

## Layout

```
cmd/chinit/            entry point
pkg/chinit/app/        flag parsing, subcommands, provider wiring
pkg/chinit/domain/
  model/               BootSpec, NodeIdentity, state-file shapes
  port/                provider capability interfaces + registry
  service/             retry engine, boot pipeline, state store
  fail/                Retryable/Fatal classification
pkg/chinit/infra/
  detect/              cloud detection (flag/env/DMI/IMDS)
  system/              disks, OS tuning, units, XML validation
  aws/                 IMDS, Secrets Manager, S3, DynamoDB leases, Route53, spot
  azure/               IMDS, Key Vault, Blob, Table leases, Private DNS, scheduled events
  fakes/               canned IMDS/secrets/bundle for the conformance suite
```
