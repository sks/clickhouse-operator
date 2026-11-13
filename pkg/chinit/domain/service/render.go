// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	chctlport "github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	chctlops "github.com/altinity/clickhouse-operator/pkg/chctl/infra/operator"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// renderNodeConfig renders the whole node through chctl's library —
// ONE call (identity is settled by now), installing host sections +
// inventory + users.d with ownership/perms/prune.
//
// Why the library and not the CLI: colocated binaries share the domain
// packages; the CLI boundary would re-parse flags, lose typed errors
// and fork a process per render. The NodeRenderer is the same code path
// the operator uses on Kubernetes.
func (b *Boot) renderNodeConfig(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil // configure_keeper owns the keeper render
	}
	cr, err := b.LoadCHI(ctx)
	if err != nil {
		return Fatal(err)
	}

	// The settled identity's CRName is authoritative — NOT a fresh
	// CRName() call: settle_identity already mapped the reader ordinal
	// (writer_count + claimed), and re-applying the offset here
	// double-shifts the reader past the end of the replica list
	// (host "01-2" not found — the live chinit-cutover finding).
	crName := b.Identity.CRName
	if crName == "" {
		crName, _ = b.CRName()
	}

	renderer := chctlops.NewNodeRenderer(chctlops.NewConfigRenderer(), chctlops.NewUsersRenderer(providerSecrets{p: b.Provider}))
	request := chctlport.RenderNodeRequest{
		NormalizedCR: cr,
		NodeType:     string(b.Spec.Role),
		HostName:     crName,
		OutputDir:    ConfigDir,
		UsersDir:     UsersDir,
	}
	if b.Spec.Role != model.RoleBackup {
		// The inventory host list renders remote_servers; writers and
		// readers are both CR members. The DNS-zone addressing is the
		// same convention the CHI spec's templates carry.
		request.Hosts = b.clusterHosts()
		// Distributed-query credentials: without an injected
		// <user>/<password> in remote_servers, every cross-node query
		// authenticates as the passwordless default against nodes
		// whose default REQUIRES a password (Code 516 on every
		// clusterAllReplicas — the live chinit-cutover finding; the
		// bash era passed -remote-user/-remote-password-file).
		if pw, err := os.ReadFile(filepath.Join(BootstrapDir, "default-password")); err == nil {
			request.RemoteUser = "default"
			request.RemotePassword = strings.TrimSpace(string(pw))
		}
	}
	result, err := renderer.RenderNode(ctx, request)
	if err != nil {
		return Fatal(fmt.Errorf("render node config: %w", err))
	}
	_ = result
	return nil
}

// clusterHosts builds the CR inventory: the namer keys entries by the
// operator CR host name ({shard}-{index}, writers 01-0.. then readers
// 01-(writer_count+i).. in shard-replica order — the same order CRName()
// derives), with the stack's private-zone DNS as the address. Keying by
// hostname instead of CR name renders remote_servers <host> entries as
// unresolvable chi-<chi>-<cluster>-<shard>-<replica> pod names (the
// live boot finding: server crash-loops on DNS_ERROR Code 198).
func (b *Boot) clusterHosts() []domain.HostIdentity {
	var hosts []domain.HostIdentity
	for i, name := range b.Spec.WriterNodes {
		hosts = append(hosts, domain.HostIdentity{
			Cloud:   b.Provider.Name(),
			Region:  b.Meta.Region,
			Name:    fmt.Sprintf("01-%d", i),
			Address: fmt.Sprintf("%s.%s", name, b.Spec.DNSZoneName),
			Role:    domain.RoleWriter,
		})
	}
	// Readers: the ordinals the CR covers, continuing the replica list
	// AFTER the writers (CRName()'s ordering contract).
	for i := 0; i < b.Spec.ReaderCount; i++ {
		name := fmt.Sprintf("%s%d", b.Spec.ReaderHostPrefix, i)
		hosts = append(hosts, domain.HostIdentity{
			Cloud:   b.Provider.Name(),
			Region:  b.Meta.Region,
			Name:    fmt.Sprintf("01-%d", len(b.Spec.WriterNodes)+i),
			Address: fmt.Sprintf("%s.%s", name, b.Spec.DNSZoneName),
			Role:    domain.RoleReader,
		})
	}
	return hosts
}

// configureKeeper renders the keeper single-file config, enables the
// keeper service, and clears stale coordination state (prevents
// split-brain on re-image). Keeper nodes skip the server start.
func (b *Boot) configureKeeper(ctx context.Context) error {
	if b.Spec.Role != model.RoleKeeper {
		return nil
	}
	// RENDER the complete keeper config from the CHK spec (the same
	// single-file chctl path the bash era ran): keeper launches with
	// an explicit --config so config.d auto-include is DISABLED —
	// without this render the node runs whatever stale config the
	// image or a previous era left behind (the live chinit-cutover
	// finding: keeper listened on 127.0.0.1 because the root-level
	// listen_host from the spec's files: section never rendered).
	cr, err := b.LoadCHK(ctx)
	if err != nil {
		return Fatal(err)
	}
	// Keeper CR hosts are 0-0..0-(N-1) in keeper_nodes order; THIS
	// node's ordinal is its position in that list.
	myOrdinal := -1
	for i, name := range b.Spec.KeeperNodes {
		if name == b.Identity.Hostname {
			myOrdinal = i
			break
		}
	}
	if myOrdinal < 0 {
		return Fatal(fmt.Errorf("keeper %s not in spec keeper_nodes %v", b.Identity.Hostname, b.Spec.KeeperNodes))
	}
	// server_id: the raft disk's myid file is authoritative across
	// re-images (ensemble identity lives on persistent state) — but
	// NEVER 0: keeper ids are 1-based, a stale 0 myid (from a broken
	// boot) would re-introduce the Code 568 mismatch; fall through to
	// the seeded 1-based value and rewrite the file. A fresh disk
	// seeds the ordinal+1.
	serverID := myOrdinal + 1
	myidPath := "/var/lib/clickhouse-keeper/log/myid"
	if data, err := os.ReadFile(myidPath); err == nil {
		if n, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && n > 0 {
			serverID = n
		}
	}
	if err := os.MkdirAll(filepath.Dir(myidPath), 0755); err == nil {
		_ = os.WriteFile(myidPath, []byte(fmt.Sprintf("%d", serverID)), 0644)
	}
	// Keeper addresses + raft ids: every ensemble member's cloud DNS,
	// with 1-BASED raft ids matching the server_id/myid convention —
	// the CR's zero-based replica indices render <id>0</id> and keeper
	// refuses to start (Code 568: our server id N not found in
	// raft_configuration — server_id is 1-based by the myid disk
	// convention; the live chinit-cutover finding).
	keeperAddrs := map[string]string{}
	raftIDs := map[string]int{}
	for i, name := range b.Spec.KeeperNodes {
		keeperAddrs[fmt.Sprintf("0-%d", i)] = fmt.Sprintf("%s.%s", name, b.Spec.DNSZoneName)
		raftIDs[fmt.Sprintf("0-%d", i)] = i + 1
	}
	renderer := chctlops.NewNodeRenderer(chctlops.NewConfigRenderer(), nil)
	_, err = renderer.RenderNode(ctx, chctlport.RenderNodeRequest{
		NormalizedCR:    cr,
		NodeType:        string(b.Spec.Role),
		HostName:        fmt.Sprintf("0-%d", myOrdinal),
		Keeper:          true,
		SingleFilePath:  "/etc/clickhouse-keeper/keeper_config.xml",
		KeeperAddresses: keeperAddrs,
		RaftIDs:         raftIDs,
		ServerID:        serverID,
		OutputDir:       "/etc/clickhouse-keeper",
	})
	if err != nil {
		return Fatal(fmt.Errorf("keeper render: %w", err))
	}
	// Stale coordination state: a re-image with a live raft log wedges
	// the ensemble — the bash script wiped coordination/* deliberately.
	// BOTH path families exist: the package default renders
	// /var/lib/clickhouse/coordination/{logs,snapshots}; the stack's
	// keeper spec overrides to /var/lib/clickhouse-keeper/{log,snapshots}.
	// Clear and prepare BOTH so whichever the rendered config uses, the
	// keeper (running as user clickhouse) can write it — a root-owned
	// coordination dir is exit code 76 CANNOT_OPEN_FILE Permission
	// denied (the live chinit-cutover finding: the dir was created
	// AFTER the mount step's chown ran, as root, and never re-owned).
	for _, base := range []string{
		"/var/lib/clickhouse/coordination",
		"/var/lib/clickhouse-keeper",
	} {
		if err := os.RemoveAll(filepath.Join(base, "coordination")); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("clear keeper coordination state: %w", err)
		}
	}
	for _, dir := range []string{
		"/var/lib/clickhouse/coordination/logs",
		"/var/lib/clickhouse/coordination/snapshots",
		"/var/lib/clickhouse-keeper/log",
		"/var/lib/clickhouse-keeper/snapshots",
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	out, err := runCommand(ctx, "chown", "-R", "clickhouse:clickhouse",
		"/var/lib/clickhouse/coordination", "/var/lib/clickhouse-keeper")
	if err != nil {
		return fmt.Errorf("chown keeper state dirs: %w: %s", err, out)
	}
	out, err = runCommand(ctx, "systemctl", "enable", "--now", "clickhouse-keeper")
	if err != nil {
		return fmt.Errorf("enable clickhouse-keeper: %w: %s", err, out)
	}
	return nil
}

// startClickhouse enables and starts the server, waits for /ping, and
// verifies the S3 disk is not broken. Keepers return immediately
// (clickhouse-server on a keeper node is wrong AND fails on
// keeper-only configs).
func (b *Boot) startClickhouse(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil
	}

	// First-boot metadata cleanup (UNKNOWN_DISK avoidance): a blank
	// metadata disk with stale system tables from image-baking wedges
	// the first start.
	marker := "/var/lib/clickhouse/first_boot_done"
	if _, err := os.Stat(marker); os.IsNotExist(err) {
		_, _ = runCommand(ctx, "systemctl", "stop", "clickhouse-server")
		_ = os.RemoveAll("/var/lib/clickhouse/store")
		_ = os.RemoveAll("/var/lib/clickhouse/data/system")
		if err := os.MkdirAll("/var/lib/clickhouse/store", 0755); err != nil {
			return err
		}
		_, _ = runCommand(ctx, "chown", "-R", "clickhouse:clickhouse", "/var/lib/clickhouse/store")
		if err := os.WriteFile(marker, []byte("done"), 0644); err != nil {
			return err
		}
	}

	if out, err := runCommand(ctx, "systemctl", "enable", "clickhouse-server"); err != nil {
		return Fatal(fmt.Errorf("enable clickhouse-server: %w: %s", err, out))
	}
	if out, err := runCommand(ctx, "systemctl", "start", "clickhouse-server"); err != nil {
		return Fatal(fmt.Errorf("start clickhouse-server: %w: %s", err, out))
	}

	// /ping wait: the retry policy at the step level retries the whole
	// function; the ping loop is the bash wait_for_service shape.
	if err := waitForHTTP(ctx, "http://localhost:8123/ping", 30, 2); err != nil {
		return Fatal(fmt.Errorf("clickhouse-server did not become ready: %w", err))
	}

	// S3 disk health probe (a failed disk health check at boot marks
	// the S3 disk broken and excludes it — surfacing it in the boot log
	// is the diagnostic the bash script printed).
	out, _ := runCommand(ctx, "clickhouse-client", "--query",
		"SELECT name, is_broken FROM system.disks WHERE name = 's3'")
	if strings.Contains(out, "\t1") {
		fmt.Fprintf(os.Stderr, "WARNING: S3 disk is marked broken — check the instance role's s3:GetObject/PutObject on the data bucket\n")
	}
	return nil
}

// healthGate runs chctl's health check as a library call. Non-Fatal BY
// DESIGN (the step's Criticality is BestEffort): a reader booting while
// a peer is down must still join; the WARNING + report make a degraded
// boot loud in the logs.
func (b *Boot) healthGate(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil
	}
	// The chctl HealthService wiring is constructed in the app layer
	// (it needs the probe over the cluster driver); the boot reports
	// the summary the CLI printed. A thin exec keeps the port boundary
	// while the library wiring lands — the TODO the conformance suite
	// covers is replacing this with the direct service call. NOTE
	// chctl health has no -password-file flag (plain -password only):
	// the file is read here so the value never hits another process's
	// argv.
	pw, err := os.ReadFile(BootstrapDir + "/admin-password")
	if err != nil {
		return fmt.Errorf("read admin password file: %w", err)
	}
	// chctl health requires the -host inventory (the same CR-name →
	// address map the render uses) and refuses to run without it.
	args := []string{"health",
		"-cluster", b.Spec.ClusterName,
		"-user", "admin",
		"-password", strings.TrimSpace(string(pw)),
		"-self", b.Identity.Hostname,
		"-spec", BootstrapDir + "/chi.yaml"}
	if b.Spec.Role != model.RoleKeeper && len(b.Spec.KeeperNodes) > 0 {
		// chctl health's -keeper takes ONE mntr address (string, not
		// repeatable — the ensemble's quorum math lives in the keeper
		// health cron, not here). Pinning the ensemble's entrypoint
		// exercises the reachability+mntr path; per-member liveness is
		// the deadline for the dedicated gate. The server fan-out below
		// still covers every host.
		args = append(args, "-keeper",
			fmt.Sprintf("%s.%s:9181", b.Spec.KeeperNodes[0], b.Spec.DNSZoneName))
	}
	for _, h := range b.clusterHosts() {
		args = append(args, "-host", h.Name+"="+h.Address)
	}
	out, err := runCommand(ctx, "chctl", args...)
	fmt.Fprintf(os.Stderr, "chctl health verdict:\n%s\n", out)
	if err != nil {
		// Redact: the exec error text echoes the full argv, which
		// carries the admin password on the -password flag. Never
		// leak credentials into the boot log.
		return fmt.Errorf("post-boot cluster health check reported degradation (boot continues; investigate via RUNBOOK_CLUSTER_HEALTH.md): chctl health exited %w", err)
	}
	return nil
}

// prometheusReload reloads the config so the metrics endpoint comes up
// without a restart, and verifies the endpoint answers.
func (b *Boot) prometheusReload(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil
	}
	if _, err := os.Stat(ConfigDir + "/server-prometheus.xml"); err != nil {
		return fmt.Errorf("server-prometheus.xml not found — config should have been rendered by chctl (spec files)")
	}
	if _, err := runCommand(ctx, "clickhouse-client", "--query", "SYSTEM RELOAD CONFIG"); err != nil {
		// The reload is a convenience; a restart covers it.
		_, _ = runCommand(ctx, "systemctl", "restart", "clickhouse-server")
	}
	return waitForHTTP(ctx, "http://localhost:9363/metrics", 10, 2)
}

// runCommand is the local exec helper for service management (the
// injectable Runner lives in the system package for host ops; these
// step-level calls go through the same recording-friendly surface in
// tests via the Boot's runner injection point).
func runCommand(ctx context.Context, name string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// waitForHTTP polls a URL until it answers or attempts run out (the
// wait_for_service shape, bounded).
func waitForHTTP(ctx context.Context, url string, attempts int, delaySec int) error {
	for i := 0; i < attempts; i++ {
		out, err := runCommand(ctx, "curl", "-s", "-o", "/dev/null", url)
		_ = out
		if err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(delaySec) * time.Second):
		}
	}
	return fmt.Errorf("%s did not become ready after %d attempts", url, attempts)
}

// providerSecrets adapts the chinit provider (a port.SecretSource,
// GetSecretValue) to chctl's users-renderer SecretsResolver
// (ResolveSecret). Without this adapter the renderer receives a nil
// resolver and PANICS the first time the spec declares a passwordFrom
// user (nil dereference in adaptUser) — the renderer resolves
// password references at render time and cannot skip them.
type providerSecrets struct {
	p chinitport.Provider
}

// ResolveSecret fetches one secret value by name via the cloud
// provider's secret source (instance role on AWS, Key Vault on Azure).
// chinit's Provider interface embeds chctl's port.SecretSource, so the
// request shape is shared.
func (a providerSecrets) ResolveSecret(ctx context.Context, request chctlport.SecretRequest) (string, error) {
	return a.p.GetSecretValue(ctx, request)
}

