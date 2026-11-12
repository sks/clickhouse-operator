# chctl — the operator, without Kubernetes

A single binary that runs the **Altinity operator's actual code paths**
against ClickHouse clusters that don't live on Kubernetes — EC2 Auto
Scaling Groups, Azure VMSS, plain VMs, bare metal.

## The problem it solves

The operator is good at three things: turning a `ClickHouseInstallation`
spec into node configuration, propagating schema to new replicas, and
keeping Keeper ensembles safe during planned disruption. Today those
behaviors only exist on Kubernetes — everyone else reimplements them:

- **Hand-written config XML per deployment.** Every VM/EC2 stack carries
  its own `config.d` templates that drift from ClickHouse behavior and
  from each other. `chctl config render` replaces them with the operator's
  config generators: one CHI spec in, the same files out that the operator
  would put in ConfigMaps on-cluster.
- **Boot-time schema scripts.** A scaled-out reader needs every table
  re-created before serving traffic; stacks usually hack this in bash.
  `chctl schema sync` runs the operator's `HostCreateTables` instead.
- **Stale replicas after scale-in.** Dead nodes leave znode garbage that
  breaks DDL and re-joins until cleaned by hand. `chctl replica drop` is
  the operator's `HostDropReplica` against a live peer.
- **Runbook-only keeper safety.** "Restart keepers one at a time" is
  procedure, not enforcement — one mistake drops Raft quorum and the whole
  cluster goes read-only. `chctl keeper safe-to-stop` reuses the
  controller's raft-quorum math, reads live ensemble state via Keeper
  `mntr`, and refuses (exit 2) when a stop would break majority.

## What the GitHub Action is doing

When you see the **"Release chctl"** workflow run in this repo, it is
publishing this binary as a GitHub Release:

1. Runs the chctl test suites (a failing tag produces no release — these
   binaries get piped into `/usr/local/bin` by node bootstrap).
2. Cross-compiles static binaries: `linux/arm64` (Graviton),
   `linux/amd64`, `darwin/arm64`, `darwin/amd64`.
3. Publishes them plus `chctl-SHA256SUMS` to the release named after the
   `chctl-v*` tag that triggered it.

Anyone can download from there:

```bash
curl -fsSL -o chctl \
  https://github.com/sks/clickhouse-operator/releases/download/chctl-v0.1.0/chctl-linux-arm64
chmod +x chctl && ./chctl help
```

## Where to go next

- **Usage and examples:** [docs/chctl.md](../../docs/chctl.md)
- **Example spec:** [docs/chctl-example-chi.yaml](../../docs/chctl-example-chi.yaml)
- **How it's built:** the CLI is a thin entry over
  [pkg/chctl](../../pkg/chctl) — domain services over the operator's own
  normalizer, config generators and schemer; the fork notice in the repo
  [README](../../README.md#fork-notice) explains the fork's divergence
  from upstream.
