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
| Boot scripts creating tables on new replicas | `chctl schema sync` — the operator's schema propagation (CREATE ... IF NOT EXISTS against `clusterAllReplicas`) |
| Stale replicas after scale-in / spot reclaim | `chctl replica drop` — the operator's `SYSTEM DROP REPLICA` cleanup |
| Runbook instructions "restart keepers one at a time" | `chctl keeper safe-to-stop` — a hard quorum gate; refuses (exit 2) if the stop would drop Raft majority |

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
  -host chi-logs-logexplorer-0-0 \
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

Each host gets its own macros/remote_servers — pass `-host` per node from
your node bootstrap (cloud-init, ignition, whatever). Drop `-host` to
render only the cluster-wide sections. Keeper specs render the same way
with `-keeper`.

**Wire it into node bootstrap** (the pattern used by the AWS EC2 stack in
`appcd-dev/langchain-examples`): upload the spec to S3/blob storage,
download it in cloud-init, render per-role at boot, and re-render + restart
on config change instead of rebuilding instances.

## 2. Bring up a new replica with the schema already in place

A scaled-out reader must re-create every table before serving traffic.

```bash
chctl schema sync chi-logs-logexplorer-0-1 \
  -cluster logexplorer \
  -host chi-logs-logexplorer-0-0=10.0.1.10 \
  -host chi-logs-logexplorer-0-1=10.0.2.10 \
  -user default -password "$CH_PASSWORD"
```

This runs the operator's `HostCreateTables` — replicated and distributed
objects get created on the target host from cluster peers.

## 3. Clean up after a dead node

After spot reclaim or scale-in, drop the stale replica path from Keeper so
DDL and future re-joins stay reliable:

```bash
chctl replica drop chi-logs-logexplorer-0-1 \
  -on chi-logs-logexplorer-0-0 \
  -cluster logexplorer \
  -host chi-logs-logexplorer-0-0=10.0.1.10 \
  -user default -password "$CH_PASSWORD"
```

After a planned restart, re-queue replication:

```bash
chctl replica sync chi-logs-logexplorer-0-0 \
  -cluster logexplorer \
  -host chi-logs-logexplorer-0-0=10.0.1.10 \
  -user default -password "$CH_PASSWORD"
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

## Command reference

```
chctl config render  -spec chi.yaml [-host 0-0] [-out config.d] [-users-dir users.d] [-dry-run] [-keeper]
chctl schema sync    <host> -cluster <name> -host <name=addr>... [-user u] [-password p] [-port n] [-secure]
chctl replica drop   <host> -on <peer> -cluster <name> -host <name=addr>...
chctl replica sync   <host> -cluster <name> -host <name=addr>...
chctl keeper safe-to-stop -keeper <host:port> [-members n] [-wait-timeout 2m]
```

All schema/replica commands take the full host inventory as repeated
`-host name=address` flags (they need peer addresses to run cluster SQL).
Passwords via `-password` or stdin; never bake them into the spec — specs
carry references, secret stores carry values.

## Release / distribution

Releases are published from this repository (tag `chctl-v*`) with binaries
for `linux/arm64`, `linux/amd64`, `darwin/arm64`, `darwin/amd64` plus
`SHA256SUMS`. The workflow at `.github/workflows/release-chctl.yaml` runs
the full test suite before publishing — a tag that fails its tests
produces no release.
