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
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	chctlport "github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	chctlops "github.com/altinity/clickhouse-operator/pkg/chctl/infra/operator"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
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

	// Hosts inventory: the CR's shape (writers 00-N, readers 01-N);
	// the backup replica rides as an out-of-CR host the sync paths
	// tolerate. Address resolution is by DNS name inside the zone.
	crName, _ := b.CRName()

	renderer := chctlops.NewNodeRenderer(chctlops.NewConfigRenderer(), chctlops.NewUsersRenderer(nil))
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
	}
	result, err := renderer.RenderNode(ctx, request)
	if err != nil {
		return Fatal(fmt.Errorf("render node config: %w", err))
	}
	_ = result
	return nil
}

// clusterHosts builds the CR inventory (writers + readers, DNS names).
func (b *Boot) clusterHosts() []domain.HostIdentity {
	var hosts []domain.HostIdentity
	for _, name := range b.Spec.WriterNodes {
		hosts = append(hosts, domain.HostIdentity{
			Cloud:   b.Provider.Name(),
			Region:  b.Meta.Region,
			Name:    name,
			Address: fmt.Sprintf("%s.%s", name, b.Spec.DNSZoneName),
			Role:    domain.RoleWriter,
		})
	}
	// Readers: the ordinals the CR covers (reader_count).
	for i := 0; i < b.Spec.ReaderCount; i++ {
		name := fmt.Sprintf("%s%d", b.Spec.ReaderHostPrefix, i)
		hosts = append(hosts, domain.HostIdentity{
			Cloud:   b.Provider.Name(),
			Region:  b.Meta.Region,
			Name:    name,
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
	// Stale coordination state: a re-image with a live raft log wedges
	// the ensemble — the bash script wiped coordination/* deliberately.
	if err := os.RemoveAll("/var/lib/clickhouse-keeper/coordination"); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("clear keeper coordination state: %w", err)
	}
	if err := os.MkdirAll("/var/lib/clickhouse-keeper/coordination", 0755); err != nil {
		return err
	}
	out, err := runCommand(ctx, "systemctl", "enable", "--now", "clickhouse-keeper")
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
	// covers is replacing this with the direct service call.
	out, err := runCommand(ctx, "chctl", "health",
		"-cluster", b.Spec.ClusterName,
		"-user", "admin",
		"-password-file", BootstrapDir+"/admin-password",
		"-spec", BootstrapDir+"/chi.yaml")
	fmt.Fprintf(os.Stderr, "chctl health verdict:\n%s\n", out)
	if err != nil {
		return fmt.Errorf("post-boot cluster health check reported degradation (boot continues; investigate via RUNBOOK_CLUSTER_HEALTH.md): %w", err)
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
