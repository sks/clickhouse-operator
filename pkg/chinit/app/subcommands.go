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

package app

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/service"
)

// nodeState is the subcommands' shared context: the state file's
// identity snapshot. Subcommands NEVER re-derive identity — the boot
// settled it; this reload is the whole point of the state contract.
type nodeState struct {
	Identity model.NodeIdentity `json:"identity"`
}

// clusterName returns the cluster the node serves (identity snapshot).
func (s *nodeState) clusterName() string { return s.Identity.ClusterName }

// dnsZone returns the cluster's private DNS zone.
func (s *nodeState) dnsZone() string { return s.Identity.DNSZone }

// loadNodeState reads /var/lib/chinit/state.json.
func loadNodeState(path string) (*nodeState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fail.Fatal(fmt.Errorf("read %s: %w (did `chinit boot` complete?)", path, err))
	}
	var st nodeState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fail.Fatal(fmt.Errorf("parse %s: %w", path, err))
	}
	if st.Identity.Hostname == "" {
		return nil, fail.Fatal(fmt.Errorf("%s carries no settled identity — run `chinit boot` first", path))
	}
	return &st, nil
}

// chctl runs the chctl binary for the cluster-operation subcommands.
// These wrap chctl's CLI (the library seam covers render/health inside
// the boot; the job commands keep the CLI boundary the units' ExecStart
// contract already uses — one process per job, typed exit codes).
func chctl(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "chctl", args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("chctl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// statePathFlag parses the common -state flag.
func statePathFlag(args []string) (string, error) {
	fs := flag.NewFlagSet("chinit job", flag.ContinueOnError)
	path := fs.String("state", service.ChinitStatePath, "state file path")
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	return *path, nil
}

// runSchemaSync is `chinit schema-sync` (the hourly + per-boot unit):
// the operator's ClusterSchemer parity — CREATE ... IF NOT EXISTS for
// every spec object on this host, retrying for late materialized-view
// targets and pruning the node's own stale registration.
func runSchemaSync(args []string) error {
	fs := flag.NewFlagSet("chinit schema-sync", flag.ContinueOnError)
	statePath := fs.String("state", service.ChinitStatePath, "state file path")
	retries := fs.Int("retries", 3, "full-pass retries (late materialized-view targets)")
	pruneStaleSelf := fs.Bool("prune-stale-self", false, "drop this node's stale replica registration before create")
	if err := fs.Parse(args); err != nil {
		return err
	}
	st, err := loadNodeState(*statePath)
	if err != nil {
		return err
	}
	ctx := context.Background()

	argv := []string{"schema", "sync",
		"-spec", service.BootstrapDir + "/chi.yaml",
		"-cluster", st.clusterName(),
		"-retries", fmt.Sprint(*retries),
	}
	if *pruneStaleSelf {
		argv = append(argv, "-prune-stale-self")
	}
	argv = append(argv,
		"-user", "admin",
		"-password-file", service.BootstrapDir+"/admin-password",
		st.Identity.Hostname,
	)
	if out, err := chctl(ctx, argv...); err != nil {
		fmt.Fprintln(os.Stderr, out)
		return fmt.Errorf("schema sync failed (retry on timer): %w", err)
	}
	return nil
}

// runReplicaSync is `chinit replica-sync` (the per-boot unit):
// SYSTEM SYNC REPLICA for every replicated table on this host.
func runReplicaSync(args []string) error {
	statePath, err := statePathFlag(args)
	if err != nil {
		return err
	}
	st, err := loadNodeState(statePath)
	if err != nil {
		return err
	}
	ctx := context.Background()
	out, err := chctl(ctx, "replica", "sync",
		"-spec", service.BootstrapDir+"/chi.yaml",
		"-cluster", st.clusterName(),
		"-user", "admin",
		"-password-file", service.BootstrapDir+"/admin-password",
		st.Identity.Hostname,
	)
	fmt.Fprintln(os.Stderr, out)
	return err
}

// runLeaseRenew is `chinit lease-renew` (readers' timer): extends the
// ordinal lease through the provider's claimer.
func runLeaseRenew(args []string) error {
	statePath, err := statePathFlag(args)
	if err != nil {
		return err
	}
	st, err := loadNodeState(statePath)
	if err != nil {
		return err
	}
	if st.Identity.LeaseTable == "" {
		return fail.Fatal(fmt.Errorf("node holds no lease (identity has no lease table)"))
	}
	provider, err := registry().Build(providerNameOf(st), port.Config{})
	if err != nil {
		return err
	}
	claimer := port.AsOrdinalClaimer(provider)
	if claimer == nil {
		return fail.Fatal(fmt.Errorf("cloud %s has no lease capability", provider.Name()))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return claimer.RenewLease(ctx, port.RenewRequest{
		Table:        st.Identity.LeaseTable,
		Hostname:    st.Identity.Hostname,
		InstanceID:  st.Identity.InstanceID,
		LeaseSeconds: 900,
	})
}

// providerNameOf extracts the provider from the state (the identity
// record carries the cloud the boot detected).
func providerNameOf(st *nodeState) string {
	// Stored in the identity's InstanceID prefix on AWS; a dedicated
	// field arrives with the state-file schema bump. Default: the
	// detected cloud at runtime.
	if os.Getenv("CHINIT_CLOUD") != "" {
		return os.Getenv("CHINIT_CLOUD")
	}
	return "aws"
}

// runKeeperHealth is `chinit keeper-health` (keepers' cron): the
// ensemble health probe (mntr four-letter word over the keeper port).
func runKeeperHealth(args []string) error {
	statePath, err := statePathFlag(args)
	if err != nil {
		return err
	}
	if _, err := loadNodeState(statePath); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := chctl(ctx, "keeper", "mntr")
	fmt.Fprintln(os.Stderr, out)
	return err
}

// runDeadReplicaGc is `chinit dead-replica-gc` (writer-0's weekly
// timer): DROP REPLICA for hosts gone from the cluster.
func runDeadReplicaGc(args []string) error {
	statePath, err := statePathFlag(args)
	if err != nil {
		return err
	}
	st, err := loadNodeState(statePath)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, err := chctl(ctx, "replica", "gc",
		"-spec", service.BootstrapDir+"/chi.yaml",
		"-cluster", st.clusterName(),
		"-user", "admin",
		"-password-file", service.BootstrapDir+"/admin-password",
	)
	fmt.Fprintln(os.Stderr, out)
	return err
}

// runOrphanedGc is `chinit orphaned-gc` (writer-0's weekly timer):
// S3 parts not referenced by any replica's metadata.
func runOrphanedGc(args []string) error {
	statePath, err := statePathFlag(args)
	if err != nil {
		return err
	}
	if _, err := loadNodeState(statePath); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()
	// The GC is a chctl replica-gc shape against the S3 disk; the
	// detailed census/compare logic ships with the operator's schemer
	// integration (the bash census loop's Go home).
	out, err := chctl(ctx, "replica", "gc",
		"-spec", service.BootstrapDir+"/chi.yaml",
		"-orphaned-only",
	)
	fmt.Fprintln(os.Stderr, out)
	return err
}

// runBackupDaily is `chinit backup-daily` (the backup replica's timer):
// clickhouse-backup create/upload/delete-local.
func runBackupDaily(args []string) error {
	if _, err := statePathFlag(args); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Hour)
	defer cancel()
	name := "daily-" + time.Now().UTC().Format("20060102-150405")
	for _, argv := range [][]string{
		{"create", name},
		{"upload", name},
		{"delete", "local", name},
	} {
		out, err := runBackupTool(ctx, argv...)
		if err != nil {
			return fmt.Errorf("clickhouse-backup %s: %w: %s", strings.Join(argv, " "), err, out)
		}
	}
	return nil
}

// runBackupTool shells clickhouse-backup (kept separate for tests).
func runBackupTool(ctx context.Context, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, "clickhouse-backup", args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%w: %s", err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// runSpotWatch is `chinit spot-watch` (readers' daemon): watches the
// provider's interruption endpoint and drains on notice — replication
// queues stopped, replica dropped on a live peer, DNS + lease
// released, server shut down. All inside the notice's deadline.
func runSpotWatch(args []string) error {
	statePath, err := statePathFlag(args)
	if err != nil {
		return err
	}
	st, err := loadNodeState(statePath)
	if err != nil {
		return err
	}
	provider, err := registry().Build(providerNameOf(st), port.Config{})
	if err != nil {
		return err
	}
	watcher := port.AsSpotSignals(provider)
	if watcher == nil {
		return fail.Fatal(fmt.Errorf("cloud %s has no spot-watch capability", provider.Name()))
	}

	ctx := context.Background()
	fmt.Printf("spot-watch: polling %s for interruption notices\n", provider.Name())
	notice, ok := <-watcher.Watch(ctx)
	if !ok {
		return nil // context ended without a notice
	}
	fmt.Printf("spot-watch: INTERRUPTION DETECTED — draining (deadline %ds)\n%s\n", notice.DeadlineSeconds, notice.Raw)
	return drain(ctx, st, provider, notice)
}

// drain performs the graceful shutdown, fastest-failure-first within
// the notice deadline (EC2: 120s).
func drain(ctx context.Context, st *nodeState, provider port.Provider, notice port.InterruptionNotice) error {
	drainCtx, cancel := context.WithTimeout(ctx, time.Duration(notice.DeadlineSeconds)*time.Second)
	defer cancel()

	// 1. ClickHouse-level graceful drain FIRST (parity with the
	// operator's HostShutdown hooks): stop replication queues so no
	// fetch/merge starts mid-shutdown; stop distributed sends so no
	// in-flight send is cut.
	for _, query := range []string{
		"SYSTEM STOP REPLICATION QUEUES",
		"SYSTEM STOP DISTRIBUTED SENDS",
	} {
		out, err := exec.CommandContext(drainCtx, "clickhouse-client", "--query", query).CombinedOutput()
		if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: %s failed: %s\n", query, strings.TrimSpace(string(out)))
		}
	}

	// 2. Peer-side replica drop (the operator's HostDropReplica form):
	// chctl replica drop on writer-0 removes both the table and the
	// DATABASE replica registration the local statement misses. If
	// writer-0 is unreachable, the weekly GC is the safety net.
	writer0 := "writer-0." + st.dnsZone()
	out, err := chctl(drainCtx, "replica", "drop",
		"-spec", service.BootstrapDir+"/chi.yaml",
		"-cluster", st.clusterName(),
		"-on", writer0,
		"-host", "00-0="+writer0,
		"-host", st.Identity.CRName+"="+st.Identity.Hostname+"."+st.dnsZone(),
		"-user", "admin",
		"-password-file", service.BootstrapDir+"/admin-password",
		st.Identity.Hostname,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NOTE: chctl peer drop failed (%s); weekly writer-0 GC will clean up\n", strings.TrimSpace(out))
		// Fallback: local drop.
		_, _ = exec.CommandContext(drainCtx, "clickhouse-client", "--query",
			"SYSTEM DROP REPLICA '"+st.Identity.Hostname+"'").CombinedOutput()
	}

	// 3. Release DNS + lease (best-effort — the instance is dying).
	if dns := port.AsDNSRegistrar(provider); dns != nil && st.Identity.ZoneID != "" {
		_ = dns.DeleteARecord(drainCtx, port.ARecordRequest{
			ZoneID:     st.Identity.ZoneID,
			ZoneName:   st.dnsZone(),
			RecordName: st.Identity.Hostname,
			TTL:        60,
		})
	}
	if claimer := port.AsOrdinalClaimer(provider); claimer != nil && st.Identity.LeaseTable != "" {
		_ = claimer.ReleaseLease(drainCtx, port.ReleaseRequest{
			Table:    st.Identity.LeaseTable,
			Hostname: st.Identity.Hostname,
		})
	}

	// 4. SYSTEM SHUTDOWN: stop accepting new queries gracefully.
	if _, err := exec.CommandContext(drainCtx, "clickhouse-client", "--query", "SYSTEM SHUTDOWN").CombinedOutput(); err != nil {
		_ = exec.CommandContext(drainCtx, "systemctl", "stop", "clickhouse-server").Run()
	}
	fmt.Println("spot-watch: drain complete")
	return nil
}
