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
	"log/slog"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

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

// inventorySpec is the topology subset of chinit.yaml specInventory
// needs. It stays a local raw struct (not model.BootSpec) on purpose:
// the subcommand re-reads the spec from disk, and a partial parse keeps
// a role-field rename in the model from breaking the job inventory.
type inventorySpec struct {
	WriterNodes []string `yaml:"writer_nodes"`
	WriterMax   int      `yaml:"writer_max"`
	// WriterCount is parsed-but-DELIBERATELY-IGNORED: the deprecated
	// compat field must never leak into the slot arithmetic (the specs
	// assert a drifted WriterCount cannot move a reader slot).
	WriterCount      int    `yaml:"writer_count"`
	WriterHostPrefix string `yaml:"writer_hostname_prefix"`
	ReaderMax        int    `yaml:"reader_max"`
	ReaderCount      int    `yaml:"reader_count"`
	ReaderHostPrefix string `yaml:"reader_hostname_prefix"`
	DNSZoneName      string `yaml:"dns_zone_name"`
	// ClaimerTableName is the lease table for nodes that hold no lease
	// of their own (the maintenance jobs list it for the writer-GC
	// liveness guard; the state's own LeaseTable wins when present).
	ClaimerTableName string `yaml:"claimer_table_name"`
}

// hostInventory renders the CR-name → address pairs for the chctl
// cluster operations. Pure over the parsed spec subset so the slot
// arithmetic has its own tests and A3's live-claims discovery can
// reuse the exact same arithmetic from a different enumeration source.
//
// CR-name arithmetic (the contract CRName() lands): writers 01-0..
// 01-(writerCeiling-1), readers 01-{writerCeiling+i} — the writer slot
// CEILING (writer_max = the writers ASG max), never the live writer
// count, so scaling writers never re-keys reader CR slots. The static
// fallback (writer_max unset → the WriterNodes list length) covers the
// interim static-writer path; A3's live-claims discovery replaces the
// enumeration SOURCE with active leases, not this slot arithmetic.
func hostInventory(spec inventorySpec) []string {
	if spec.WriterHostPrefix == "" {
		spec.WriterHostPrefix = "writer-"
	}
	if spec.ReaderHostPrefix == "" {
		spec.ReaderHostPrefix = "reader-"
	}
	// writerCeiling: the writer slot ceiling — writer_max when the
	// writers ASG owns the role, else the legacy WriterNodes list
	// length (static writer specs have no writer_max).
	writerCeiling := spec.WriterMax
	if writerCeiling == 0 {
		writerCeiling = len(spec.WriterNodes)
	}
	var out []string
	// Writers: leased specs enumerate the reserved ordinals
	// (writer-i.zone — the spec's writer_hostname_prefix); static specs
	// keep the explicit WriterNodes list.
	if spec.WriterMax > 0 {
		for i := 0; i < writerCeiling; i++ {
			name := fmt.Sprintf("%s%d", spec.WriterHostPrefix, i)
			out = append(out, fmt.Sprintf("01-%d=%s.%s", i, name, spec.DNSZoneName))
		}
	} else {
		for i, name := range spec.WriterNodes {
			out = append(out, fmt.Sprintf("01-%d=%s.%s", i, name, spec.DNSZoneName))
		}
	}
	// Readers: the ASG's reserved ordinals, offset by the writer
	// ceiling (the same slot arithmetic CRName() lands; reader_max is
	// the ceiling since the ASG can scale past the baseline count).
	readerCeiling := spec.ReaderMax
	if readerCeiling == 0 {
		readerCeiling = spec.ReaderCount
	}
	for i := 0; i < readerCeiling; i++ {
		name := fmt.Sprintf("%s%d", spec.ReaderHostPrefix, i)
		out = append(out, fmt.Sprintf("01-%d=%s.%s", writerCeiling+i, name, spec.DNSZoneName))
	}
	return out
}

// loadInventorySpec reads the bundle's chinit.yaml into the topology
// subset (the legacy path's enumeration source). A missing or
// unparsable file yields an error: the callers treat it as "no
// inventory" (chctl schema sync fails with its own "host inventory
// is empty" error — never a panic here).
//
// inventorySpecPath is the seam the tests redirect (a package-level
// func variable, not a flag: the unit files' ExecStart contract owns
// the CLI surface and the tests run without /var/lib privileges).
var inventorySpecPath = func() string { return service.BootstrapDir + "/chinit.yaml" }

func loadInventorySpec() (*inventorySpec, error) {
	data, err := os.ReadFile(inventorySpecPath())
	if err != nil {
		return nil, err
	}
	var spec inventorySpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, err
	}
	return &spec, nil
}

// writerCeiling is the writer slot ceiling (writer_max, falling back
// to the static WriterNodes list length — pre-max specs). The live
// path needs the offset but not the full enumeration; WriterCount is
// the last-resort legacy fallback. Nil-receiver-safe: never called
// that way today, but the nil contract is the caller's.
func (is *inventorySpec) writerCeiling() int {
	if is == nil {
		return -1
	}
	if is.WriterMax > 0 {
		return is.WriterMax
	}
	if n := len(is.WriterNodes); n > 0 {
		return n
	}
	return is.WriterCount
}

// writerPrefix defaults the leased-writer hostname prefix.
// Nil-receiver-safe (see writerCeiling).
func (is *inventorySpec) writerPrefix() string {
	if is == nil || is.WriterHostPrefix == "" {
		return model.DefaultWriterHostPrefix
	}
	return is.WriterHostPrefix
}

// readerPrefix defaults the leased-reader hostname prefix.
// Nil-receiver-safe (see writerCeiling).
func (is *inventorySpec) readerPrefix() string {
	if is == nil || is.ReaderHostPrefix == "" {
		return model.DefaultReaderHostPrefix
	}
	return is.ReaderHostPrefix
}

// claimRoleOrdinal classifies a claim hostname into its ASG role and
// the ordinal it holds (writer-2 → writer, 2) — the shared input to
// every CR-slot mapping (claimsHostFlags, the drain's peer selection).
// Foreign rows (backup-0, keeper-N) and malformed ordinals answer
// false: a reserved slot they never had means no CR name either.
// Writer prefix wins, mirroring the claim keys' no-overlap contract.
func claimRoleOrdinal(hostname, writerPrefix, readerPrefix string) (model.Role, int, bool) {
	switch {
	case strings.HasPrefix(hostname, writerPrefix):
		if n, err := strconv.Atoi(strings.TrimPrefix(hostname, writerPrefix)); err == nil && n >= 0 {
			return model.RoleWriter, n, true
		}
	case strings.HasPrefix(hostname, readerPrefix):
		if n, err := strconv.Atoi(strings.TrimPrefix(hostname, readerPrefix)); err == nil && n >= 0 {
			return model.RoleReader, n, true
		}
	}
	return "", -1, false
}

// claimCRSlot maps one live claim to its CR slot ordinal — THE slot
// arithmetic hostInventory, claimsHostFlags and the drain's peer
// selection all key on: writers occupy 01-<ordinal> bounded by the
// writer slot CEILING (writer_max — a stale claim from a shrunk max
// reserves no slot), readers offset by that ceiling
// (01-<ceiling+ordinal>). Why one helper: the inventory flags and the
// drain's peer mapping must never disagree about which CR slot a
// hostname lives in. -1 = no slot (a negative writerCeiling means the
// topology is unknown, so reader slots cannot be computed — never
// guessed).
func claimCRSlot(role model.Role, ordinal, writerCeiling int) int {
	switch role {
	case model.RoleWriter:
		if writerCeiling <= 0 || ordinal < writerCeiling {
			return ordinal
		}
	case model.RoleReader:
		if writerCeiling >= 0 {
			return writerCeiling + ordinal
		}
	}
	return -1
}

// claimsHostFlags renders ACTIVE lease rows as chctl -host flags with
// the same CR-name arithmetic hostInventory uses (writers 01-{ordinal};
// readers 01-{writerCeiling+ordinal} — the contract CRName() lands),
// so schema-sync's replica set and remote_servers never disagree.
// Sorted by CR slot (the lease-table Scan order is unordered) and
// includes THIS node (self is a member). Claims at or beyond a role's
// ceiling are stale rows from a shrunk max — the CR reserves no slot
// for them, so they are dropped. A negative writerCeiling (-1: the
// spec lacks writer topology) skips the writer ordinal bound check
// (no writer claims can exist in that stack anyway).
func claimsHostFlags(claims []port.Claim, dnsZone string, writerCeiling int, writerPrefix, readerPrefix string) []string {
	type flag struct {
		slot string
		host string
	}
	var out []flag
	for _, claim := range claims {
		role, ordinal, ok := claimRoleOrdinal(claim.Hostname, writerPrefix, readerPrefix)
		if !ok {
			continue // foreign row (backup-0, expired other-role key)
		}
		slotOrdinal := claimCRSlot(role, ordinal, writerCeiling)
		if slotOrdinal < 0 {
			continue // stale row from a shrunk ceiling — no CR slot
		}
		out = append(out, flag{slot: fmt.Sprintf("01-%d", slotOrdinal), host: claim.Hostname})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].slot < out[j].slot })
	flags := make([]string, 0, len(out))
	for _, f := range out {
		flags = append(flags, f.slot+"="+f.host+"."+dnsZone)
	}
	return flags
}

// specInventory loads the node's chinit.yaml and renders the CR-name →
// address pairs the chctl cluster operations need as -host flags.
//
// Two sources, in precedence order:
//  1. LIVE claims: when this node holds a lease (identity.LeaseTable —
//     the same table the boot claimed from) and the cloud can list it,
//     the inventory is the ACTIVE claims — the same contract
//     remote_servers renders, so schema-sync never targets a
//     reserved-but-vacant slot (an unreachable host fails every
//     fan-out).
//  2. Static spec lists (legacy path): the parsed chinit.yaml
//     (never the state: the state carries identity, not topology) —
//     keepers, backup nodes, clouds without the listing capability and
//     a lease-table outage all land here, with the outage degraded
//     loudly in the log.
//
// The provider is built from the registry (the subcommand's own entry
// into the factory — the boot's provider instance died with its
// process). If the cloud cannot be named from the state the static
// path answers rather than erroring: a missing provider name means a
// very old state file, and the job still has a spec to enumerate.
func (s *nodeState) specInventory() []string {
	return s.specInventoryWithSource(func(cloud string) (port.LeaseLister, string) {
		provider, err := registry().Build(cloud, port.Config{})
		if err != nil {
			return nil, "unknown provider " + cloud
		}
		return port.AsLeaseLister(provider), provider.Name()
	})
}

// specInventoryWithSource is specInventory with the lister factory
// injected (tests substitute a fake; production builds the registry
// provider). The factory returns the capability plus the provider's
// display name for the warning paths.
//
// The bundle spec is read FIRST in both paths: the live path needs the
// writer ceiling from it (reader slots are ceiling-offset), and the
// legacy path enumerates it outright — so a node whose spec cannot be
// read gets the same loud empty inventory either way (chctl's own
// "host inventory is empty" is the visible error, never a guessed
// slot layout).
func (s *nodeState) specInventoryWithSource(listerFor func(cloud string) (port.LeaseLister, string)) []string {
	spec, _ := loadInventorySpec()

	// The live path: the SETTLED identity's LeaseTable is the table the
	// boot claimed from (empty for static nodes — the static lists are
	// their source).
	if table := s.Identity.LeaseTable; table != "" && listerFor != nil {
		if lister, cloudName := listerFor(providerNameOf(s)); lister != nil {
			if spec == nil {
				// The lease is live but the spec (the writer ceiling)
				// is not readable: reader slots cannot be computed, and
				// the legacy enumeration below is equally empty —
				// degrade loudly instead of guessing slot offsets.
				slog.Warn("WARNING: " + s.Identity.Hostname + " holds a lease but the bundle spec is unreadable; -host inventory is empty (re-run chinit boot)")
				fmt.Fprintf(os.Stderr, "WARNING: %s holds a lease but the bundle spec is unreadable; -host inventory is empty (re-run chinit boot)\n", s.Identity.Hostname)
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			claims, err := lister.ListActiveClaims(ctx, port.ListClaimsRequest{Table: table})
			if err == nil {
				return claimsHostFlags(claims, s.Identity.DNSZone, spec.writerCeiling(), spec.writerPrefix(), spec.readerPrefix())
			}
			// A lease-table outage falls back to static — the hourly
			// job must still run (a possibly-vacant list beats no run),
			// and the warning makes the degraded source visible.
			slog.Warn("WARNING: live-claims discovery failed; inventory enumerated from static spec (may include vacant slots)",
				"cloud", cloudName,
				"err", err)
			fmt.Fprintf(os.Stderr, "WARNING: live-claims discovery on cloud %s failed (%s); inventory enumerated from static spec (may include vacant slots)\n", cloudName, err)
		} else {
			slog.Warn("WARNING: cloud has no lease-listing capability; inventory enumerated from static spec (may include vacant slots)",
				"cloud", cloudName)
			fmt.Fprintf(os.Stderr, "WARNING: cloud %s has no lease-listing capability; inventory enumerated from static spec (may include vacant slots)\n", cloudName)
		}
	}

	if spec == nil {
		return nil
	}
	return hostInventory(*spec)
}

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

// adminPassword reads the boot's admin password file. chctl's CLI has
// no -password-file flag (plain -password only); the value is read
// here so the file stays 0600 and the chctl argv carries it like every
// other credential-bearing CLI call. The path seam keeps tests off
// the real /var/lib.
func adminPassword() string {
	return adminPasswordAt(adminPasswordPath())
}

// adminPasswordPath is the admin password file seam (tests redirect).
var adminPasswordPath = func() string { return service.BootstrapDir + "/admin-password" }

// adminPasswordAt reads one password file, trimming the newline.
func adminPasswordAt(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

// chctl runs the chctl binary for the cluster-operation subcommands.
// These wrap chctl's CLI (the library seam covers render/health inside
// the boot; the job commands keep the CLI boundary the units' ExecStart
// contract already uses — one process per job, typed exit codes).
func chctl(ctx context.Context, args ...string) (string, error) {
	out, err := runCommand(ctx, "chctl", args...)
	if err != nil {
		return string(out), fmt.Errorf("chctl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// runCommand is the exec seam: production shells out via
// exec.CommandContext (CombinedOutput), tests substitute a recorder.
// A package-level var rather than a runner struct on purpose — the
// drain is one function over the nodeState, and the var matches the
// package's existing seams (inventorySpecPath).
var runCommand = func(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).CombinedOutput()
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
	// Host inventory: chctl's schemer names replicas from the spec but
	// resolves addresses from the -host flags ("host inventory is
	// empty" without them). CR names: writers 01-0.. then readers
	// 01-(writer ceiling+i).. — CRName()'s order; the addresses are the
	// stack's private-zone DNS.
	for _, h := range st.specInventory() {
		argv = append(argv, "-host", h)
	}
	if *pruneStaleSelf {
		argv = append(argv, "-prune-stale-self")
	}
	argv = append(argv,
		"-user", "admin",
		"-password", adminPassword(),
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
		"-password", adminPassword(),
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
	provider, err := buildProvider(providerNameOf(st))
	if err != nil {
		return err
	}
	claimer := port.AsOrdinalClaimer(provider)
	if claimer == nil {
		return fail.Fatal(fmt.Errorf("cloud %s has no lease capability", provider.Name()))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	slog.Info("lease-renew: extending lease",
		"hostname", st.Identity.Hostname,
		"lease_table", st.Identity.LeaseTable,
		"lease_seconds", 900)
	err = claimer.RenewLease(ctx, port.RenewRequest{
		Table:        st.Identity.LeaseTable,
		Hostname:     st.Identity.Hostname,
		InstanceID:   st.Identity.InstanceID,
		LeaseSeconds: 900,
	})
	if err != nil {
		slog.Warn("lease-renew: renewal failed", "err", err)
		return err
	}
	slog.Info("lease-renew: lease extended", "hostname", st.Identity.Hostname)
	return nil
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

// buildProvider constructs the subcommands' provider from the
// registry (the boot's provider instance died with its process). A
// package-level var so the drain's SSM-invocation specs inject a
// fake — production always builds the real registry provider.
var buildProvider = func(cloud string) (port.Provider, error) {
	return registry().Build(cloud, port.Config{})
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

// runDeadReplicaGc is `chinit dead-replica-gc` (the weekly timer on
// every writer; the ordinal-0 holder does the work): DROP REPLICA for
// hosts gone from the cluster, in two passes:
//
//  1. the reader pass — the historic `reader-*` sweep, ALWAYS run
//     (readers are disposable; their drain drops registrations
//     authoritatively and the weekly GC is the safety net);
//  2. the writer pass — gated on liveness data: it lists the
//     cluster's ACTIVE ordinal leases first and passes every live
//     writer as `-exclude` to chctl, so a mid-boot re-join whose lease
//     is minutes fresh can never lose its registration to the sweep.
//     Without the lister (capability absent or the lease table
//     unreachable) the writer pass is SKIPPED with a warning —
//     dropping writer registrations without liveness data is too
//     dangerous to guess at.
//
// The GC safety contract: the reader pass always runs; the writer pass
// runs only with liveness data AND never drops a live-leased writer.
func runDeadReplicaGc(args []string) error {
	statePath, err := statePathFlag(args)
	if err != nil {
		return err
	}
	st, err := loadNodeState(statePath)
	if err != nil {
		return err
	}
	// The ordinal-0 gate: every writer installs this timer, only the
	// holder of writer-{prefix}0 runs the sweep (with ASG writers the
	// ordinal is a lease, not a machine).
	if gateMaintenanceJob(st) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	return deadReplicaGc(ctx, st, gcLivelinessFrom(st))
}

// gcLivelinessFrom builds the writer pass's liveness source the way
// every subcommand reaches a capability: the provider's LeaseLister
// (via buildProvider — the boot's provider instance died with its
// process; the var is the specs' injection seam). The two-pass sweep
// itself takes the lister as a parameter (deadReplicaGc), so the
// specs drive the passes without the registry.
func gcLivelinessFrom(st *nodeState) port.LeaseLister {
	provider, err := buildProvider(providerNameOf(st))
	if err != nil {
		return nil
	}
	return port.AsLeaseLister(provider)
}

// gcRunFlags builds the shared chctl replica-gc argv prefix: the
// maintenance holder IS the detection peer — chctl runs its stale
// detection against this node's own system.replicas, and the
// clusterAllReplicas fan-out rides the node's rendered remote_servers
// config (not chctl's -host map, which only builds the client
// connection). That is why -on is the settled hostname and the single
// -host entry is the node's own CR slot: chctl resolves the run-on
// peer through the -host inventory, and self is the only host it must
// reach.
func gcRunFlags(st *nodeState) []string {
	flags := []string{
		"replica", "gc",
		"-spec", service.BootstrapDir + "/chi.yaml",
		"-cluster", st.clusterName(),
		"-on", st.Identity.Hostname,
	}
	if st.Identity.CRName != "" {
		flags = append(flags, "-host", st.Identity.CRName+"="+st.Identity.Hostname+"."+st.dnsZone())
	}
	return append(flags, "-user", "admin", "-password", adminPassword())
}

// deadReplicaGc is the two-pass sweep over a settled identity and a
// liveness source (nil lister = skip the writer pass; the reader pass
// still runs). Extracted from runDeadReplicaGc so the specs can drive
// the passes without the provider registry.
func deadReplicaGc(ctx context.Context, st *nodeState, lister port.LeaseLister) error {
	// Pass 1: readers, unconditionally (no -name-pattern: chctl's
	// reader-* default is the pass).
	slog.Info("dead-replica-gc: reader pass starting", "on", st.Identity.Hostname)
	out, err := chctl(ctx, gcRunFlags(st)...)
	fmt.Fprintln(os.Stderr, out)
	if err != nil {
		return fmt.Errorf("reader GC pass failed: %w", err)
	}
	slog.Info("dead-replica-gc: reader pass complete")

	// Pass 2: writers, only with liveness data. The active claims are
	// the exclude set — chctl reports a stale-but-live writer and
	// leaves its registration alone.
	argv, skip, err := writerGcPassFlags(ctx, st, lister)
	if err != nil || skip != "" {
		slog.Warn("WARNING: skipping the writer GC pass; the reader pass ran",
			"reason", reasonOf(err, skip))
		fmt.Fprintf(os.Stderr, "WARNING: skipping the writer GC pass (%s); the reader pass ran\n", reasonOf(err, skip))
		return nil
	}
	out, err = chctl(ctx, argv...)
	fmt.Fprintln(os.Stderr, out)
	if err != nil {
		return fmt.Errorf("writer GC pass failed: %w", err)
	}
	slog.Info("dead-replica-gc: writer pass complete")
	return nil
}

// reasonOf renders the skip reason from either the error path (the
// listing failed) or the note path (no capability).
func reasonOf(err error, note string) string {
	if err != nil {
		return err.Error()
	}
	return note
}

// runOrphanedGc is `chinit orphaned-gc` (the weekly timer on every
// writer; the ordinal-0 holder does the work): S3 parts not referenced
// by any replica's metadata.
func runOrphanedGc(args []string) error {
	statePath, err := statePathFlag(args)
	if err != nil {
		return err
	}
	st, err := loadNodeState(statePath)
	if err != nil {
		return err
	}
	// Same ordinal-0 gate as the dead-replica GC: the two maintenance
	// jobs travel together.
	if gateMaintenanceJob(st) {
		return nil
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

// gateMaintenanceJob answers whether this node should skip the
// maintenance jobs (true = skip and exit 0). With ASG writers
// "writer-0" is a LEASE ORDINAL — whichever instance currently holds
// it — so the gate compares the settled hostname (the state's
// authoritative identity) against the boot's writer prefix, exact
// match: `reader-0` must not match, and prefix tricks must never win.
// The prefix comes from the bundle spec the same way the inventory
// paths read it (the state carries identity, not topology); an
// unreadable spec falls back to the model default — the fail-safe
// direction (a custom-prefix holder that cannot read its spec skips
// rather than sweeping from the wrong designation).
//
// Non-writers (readers; SSM could invoke this manually) take the same
// skip path — maintenance is a writer job.
func gateMaintenanceJob(st *nodeState) bool {
	prefix := model.DefaultWriterHostPrefix
	if spec, err := loadInventorySpec(); err == nil && spec != nil {
		prefix = spec.writerPrefix()
	}
	ordinalZero := prefix + "0"
	if st.Identity.Role == model.RoleWriter && st.Identity.Hostname == ordinalZero {
		return false
	}
	slog.Info("maintenance jobs run on ordinal 0; this node holds a different ordinal; skipping",
		"ordinal_zero", ordinalZero,
		"this_node", st.Identity.Hostname)
	fmt.Printf("maintenance jobs run on ordinal 0 (%s); this node holds %s; skipping\n", ordinalZero, st.Identity.Hostname)
	return true
}

// writerGcPassFlags builds the writer-pass chctl argv: the writer
// pattern plus one -exclude per ACTIVE writer lease. A nil argv (with
// skipNote or err set) means the pass must be skipped — the caller
// treats both as fail-safe (the reader pass already ran).
//
// The exclude set comes from the cluster's ACTIVE claims (lease_until
// > now — the lister's own filter): every live writer hostname. Reader
// claims are not excluded (the reader pass owns them and the writer
// pattern never matches them). A nil lister means the cloud has no
// lease-listing capability — the note says so.
func writerGcPassFlags(ctx context.Context, st *nodeState, lister port.LeaseLister) (argv []string, skipNote string, err error) {
	if lister == nil {
		return nil, "cloud has no lease-listing capability", nil
	}
	table := st.Identity.LeaseTable
	if table == "" {
		// The maintenance holder need not hold a lease itself
		// (theoretically it always does in this stack, but a manual
		// SSM run on a static writer must still find the table the
		// boot's spec names).
		if spec, specErr := loadInventorySpec(); specErr == nil && spec != nil {
			table = spec.ClaimerTableName
		}
	}
	if table == "" {
		return nil, "", fmt.Errorf("no lease table to list (identity and bundle spec both carry none)")
	}
	claims, err := lister.ListActiveClaims(ctx, port.ListClaimsRequest{Table: table})
	if err != nil {
		return nil, "", fmt.Errorf("live-claims listing failed: %w", err)
	}
	prefix := model.DefaultWriterHostPrefix
	if spec, specErr := loadInventorySpec(); specErr == nil && spec != nil {
		prefix = spec.writerPrefix()
	}
	var excludes []string
	for _, claim := range claims {
		if strings.HasPrefix(claim.Hostname, prefix) {
			excludes = append(excludes, claim.Hostname)
		}
	}
	sort.Strings(excludes)
	slog.Info("writer GC pass: live writer leases excluded from the sweep",
		"excluded", len(excludes),
		"name_pattern", prefix+"*")
	fmt.Fprintf(os.Stderr, "writer GC pass: %d live writer lease(s) excluded from the sweep\n", len(excludes))

	argv = append(gcRunFlags(st), "-name-pattern", prefix+"*")
	for _, host := range excludes {
		argv = append(argv, "-exclude", host)
	}
	return argv, "", nil
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
	provider, err := buildProvider(providerNameOf(st))
	if err != nil {
		return err
	}
	watcher := port.AsSpotSignals(provider)
	if watcher == nil {
		return fail.Fatal(fmt.Errorf("cloud %s has no spot-watch capability", provider.Name()))
	}

	ctx := context.Background()
	slog.Info("spot-watch: polling for interruption notices", "cloud", provider.Name())
	fmt.Printf("spot-watch: polling %s for interruption notices\n", provider.Name())
	notice, ok := <-watcher.Watch(ctx)
	if !ok {
		return nil // context ended without a notice
	}
	slog.Warn("spot-watch: INTERRUPTION DETECTED — draining",
		"deadline_seconds", notice.DeadlineSeconds,
		"raw", notice.Raw)
	fmt.Printf("spot-watch: INTERRUPTION DETECTED — draining (deadline %ds)\n%s\n", notice.DeadlineSeconds, notice.Raw)
	return drain(ctx, st, provider, drainOptions{DeadlineSeconds: notice.DeadlineSeconds, Reason: drainReasonSpot})
}

// drainReason values — the invocation context a drain runs under.
// Why named: the termination lifecycle hook (SSM → `chinit drain`)
// and the spot watcher share the drain; the reason lands in the log
// so post-termination forensics can tell a scale-in from a spot
// reclaim apart.
const (
	drainReasonSpot            = "spot"
	drainReasonTerminationHook = "termination-hook"
)

// defaultDrainDeadlineSeconds is the CLI drain's budget — 240s,
// matching the Terraform termination-hook's drain allowance so the
// SSM invocation and the hook heartbeat run out together (the hook
// Lambda CONTINUEs on our non-zero exit; exceeding its budget has the
// same outcome, so a local overage must never be the failure mode).
const defaultDrainDeadlineSeconds = 240

// drainOptions is the drain's tunables: the deadline the drain runs
// inside and the reason (log context).
type drainOptions struct {
	// DeadlineSeconds bounds the whole drain (spot: the notice's
	// reclaim window; termination hook: 240s — the hook budget).
	DeadlineSeconds int
	// Reason names the trigger: drainReasonSpot or
	// drainReasonTerminationHook.
	Reason string
}

// runDrain is `chinit drain` (the ASG termination lifecycle hook's
// SSM invocation): reloads the settled identity, then performs the
// graceful drain — the same sequence the spot watcher runs, with the
// hook's generous budget instead of a spot notice's reclaim window.
// Writers are on-demand only (no spot notices), so the termination
// hook is their drain trigger; the SSM agent is the entry point and
// no systemd unit is needed.
//
// Exit contract: a node whose state cannot be read exits 1 — a node
// that never settled identity cannot drain safely, and the SSM
// failure makes the hook Lambda CONTINUE (the instance dies on
// schedule, the weekly GC reaps the registration).
func runDrain(args []string) error {
	fs := flag.NewFlagSet("chinit drain", flag.ContinueOnError)
	statePath := fs.String("state", service.ChinitStatePath, "state file path")
	reason := fs.String("reason", drainReasonTerminationHook, "drain trigger for the log: spot|termination-hook")
	timeout := fs.Int("timeout", defaultDrainDeadlineSeconds, "drain budget in seconds (the termination hook's drain allowance)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch *reason {
	case drainReasonSpot, drainReasonTerminationHook:
	default:
		return fmt.Errorf("invalid -reason %q: want %q or %q", *reason, drainReasonSpot, drainReasonTerminationHook)
	}
	if *timeout <= 0 {
		return fmt.Errorf("invalid -timeout %d: must be positive", *timeout)
	}
	st, err := loadNodeState(*statePath)
	if err != nil {
		// Deliberately a PLAIN error (exit 1, not the boot-fatal 2):
		// SSM needs any non-zero exit, and the text tells the
		// operator reading the SSM log why the drain declined plus
		// what happens next (hook CONTINUEs; weekly GC reaps).
		slog.Warn("drain: cannot load node state; the termination hook will CONTINUE and the weekly dead-replica GC will clean up",
			"state_path", *statePath,
			"err", err)
		return fmt.Errorf("drain: cannot load node state from %s: %s\n"+
			"a node that never settled identity cannot drain safely; the ASG termination hook will CONTINUE and the weekly dead-replica GC will clean up (SSM sees this failure)",
			*statePath, err)
	}
	provider, err := buildProvider(providerNameOf(st))
	if err != nil {
		return err
	}
	slog.Info("drain: invoked",
		"hostname", st.Identity.Hostname,
		"reason", *reason,
		"budget_seconds", *timeout)
	return drain(context.Background(), st, provider, drainOptions{
		DeadlineSeconds: *timeout,
		Reason:          *reason,
	})
}

// selectDrainPeer picks the LIVE peer the replica drop runs against,
// from the lease table's active claims.
//
// Why a live peer and not a hardcoded writer-0: the ASG termination
// hook fires for ANY writer, including the node holding ordinal 0
// (draining writer-0 against itself is a no-op drop that strands the
// registration) and including the LAST writer during a scale-in to
// min (writer-0 may already be gone). Preference order:
//  1. writer-0 (the ordinal the maintenance jobs and the weekly GC
//     run on — the node most likely to be healthy);
//  2. the lowest-ordinal OTHER live writer;
//  3. any live reader (readers are full replicas — the peer-side
//     chctl drop against a reader carries the same registration
//     authority; the note in the log says so);
//  4. no capability/no claims: the writer-0 DNS-name fallback
//     (static/interim clusters), degraded loudly — the weekly GC
//     catches what a dead peer missed.
//
// Self is excluded (dropping a replica via itself is the no-op the
// local fallback already covers).
func selectDrainPeer(ctx context.Context, st *nodeState, spec *inventorySpec, lister port.LeaseLister, cloudName string) *port.Claim {
	self := st.Identity.Hostname
	// The writer-0 fallback answers when live discovery cannot (no
	// capability, no lease table, an unreadable spec, a scan outage):
	// a possibly-vacant peer beats NO peer for a best-effort drop,
	// degraded loudly — the weekly GC is the safety net.
	fallback := func(why string) *port.Claim {
		slog.Warn("WARNING: drain peer fell back to writer-0 without live-claims discovery — the peer may be vacant; the weekly dead-replica GC is the safety net",
			"why", why)
		fmt.Fprintf(os.Stderr, "WARNING: drain peer fell back to writer-0.%s without live-claims discovery (%s) — the peer may be vacant; the weekly dead-replica GC is the safety net\n", st.dnsZone(), why)
		return &port.Claim{Hostname: "writer-0"}
	}
	switch {
	case spec == nil:
		return fallback("bundle spec unreadable — no slot arithmetic")
	case lister == nil:
		return fallback("cloud " + cloudName + " has no lease-listing capability")
	case st.Identity.LeaseTable == "":
		return fallback("static node — identity holds no lease table")
	}
	claims, err := lister.ListActiveClaims(ctx, port.ListClaimsRequest{Table: st.Identity.LeaseTable})
	if err != nil {
		return fallback("cloud " + cloudName + " claims scan failed: " + err.Error())
	}
	// One pass, two buckets: writers and readers by ordinal, self
	// skipped. Ordinals, not scan order, pick the peer. A claim at or
	// beyond the writer ceiling is a stale row from a shrunk max — it
	// reserves no CR slot (the same rule claimsHostFlags applies).
	var writers, readers []*port.Claim
	for i := range claims {
		if claims[i].Hostname == self {
			continue
		}
		role, ordinal, ok := claimRoleOrdinal(claims[i].Hostname, spec.writerPrefix(), spec.readerPrefix())
		if !ok || claimCRSlot(role, ordinal, spec.writerCeiling()) < 0 {
			continue
		}
		if role == model.RoleWriter {
			writers = append(writers, &claims[i])
		} else {
			readers = append(readers, &claims[i])
		}
	}
	// Writers sort numerically (writer-10 > writer-2 — lexically 10
	// sorts first, which would pick the wrong "lowest" peer).
	ordinalOf := func(c *port.Claim) int {
		_, ordinal, _ := claimRoleOrdinal(c.Hostname, spec.writerPrefix(), spec.readerPrefix())
		return ordinal
	}
	sortPeers := func(peers []*port.Claim) {
		sort.SliceStable(peers, func(i, j int) bool { return ordinalOf(peers[i]) < ordinalOf(peers[j]) })
	}
	if len(writers) > 0 {
		sortPeers(writers)
		// writer-0 first when present; otherwise the lowest live
		// ordinal. Sorting already puts 0 first when present — the
		// explicit bucket is the documentation, not the arithmetic.
		return writers[0]
	}
	if len(readers) > 0 {
		sortPeers(readers)
		slog.Info("NOTE: no live writer peer; draining via a reader (a reader is a full replica — the peer-side drop carries the same registration authority)",
			"peer", readers[0].Hostname)
		fmt.Fprintf(os.Stderr, "NOTE: no live writer peer; draining via reader %s (a reader is a full replica — the peer-side drop carries the same registration authority)\n", readers[0].Hostname)
		return readers[0]
	}
	return fallback("no live claims besides self")
}

// drain performs the graceful shutdown, fastest-failure-first inside
// the drain budget (spot notice: the reclaim window; termination hook:
// 240s). The sequence is the hook Lambda's contract:
//  1. SYSTEM STOP REPLICATION QUEUES + STOP DISTRIBUTED SENDS (no
//     fetch/merge/send starts mid-shutdown);
//  2. chctl replica drop on a LIVE peer (removes the table AND the
//     database replica registration the local statement misses),
//     falling back to the local SYSTEM DROP REPLICA;
//  3. DNS record delete + lease release (best-effort);
//  4. SYSTEM SHUTDOWN (graceful), systemctl stop as the last resort.
//
// Every step is best-effort by design: the instance is dying, and the
// weekly GC reaps whatever this pass could not reach.
func drain(ctx context.Context, st *nodeState, provider port.Provider, opts drainOptions) error {
	deadline := opts.DeadlineSeconds
	if deadline <= 0 {
		deadline = defaultDrainDeadlineSeconds
	}
	drainCtx, cancel := context.WithTimeout(ctx, time.Duration(deadline)*time.Second)
	defer cancel()
	slog.Info("drain: draining node",
		"hostname", st.Identity.Hostname,
		"reason", opts.Reason,
		"budget_seconds", deadline)
	fmt.Printf("drain: draining %s (reason %s, budget %ds)\n", st.Identity.Hostname, opts.Reason, deadline)

	// 1. ClickHouse-level graceful drain FIRST (parity with the
	// operator's HostShutdown hooks): stop replication queues so no
	// fetch/merge starts mid-shutdown; stop distributed sends so no
	// in-flight send is cut.
	for _, query := range []string{
		"SYSTEM STOP REPLICATION QUEUES",
		"SYSTEM STOP DISTRIBUTED SENDS",
	} {
		out, err := runCommand(drainCtx, "clickhouse-client", "--query", query)
		if err != nil {
			slog.Warn("WARNING: drain query failed",
				"query", query,
				"err", err,
				"out", strings.TrimSpace(string(out)))
			fmt.Fprintf(os.Stderr, "WARNING: %s failed: %s\n", query, strings.TrimSpace(string(out)))
		} else {
			slog.Info("drain: query issued", "query", query)
		}
	}

	// 2. Peer-side replica drop (the operator's HostDropReplica form):
	// chctl replica drop on a LIVE peer removes both the table and the
	// DATABASE replica registration the local statement misses. If no
	// peer is reachable, the weekly GC is the safety net.
	spec, _ := loadInventorySpec()
	peer := selectDrainPeer(drainCtx, st, spec, port.AsLeaseLister(provider), provider.Name())
	if peer != nil {
		slog.Info("drain: dropping replica on peer", "peer", peer.Hostname)
		out, err := dropOnPeer(drainCtx, st, spec, peer)
		if err != nil {
			slog.Warn("NOTE: chctl peer drop failed; the weekly dead-replica GC will clean up",
				"peer", peer.Hostname,
				"err", err,
				"out", strings.TrimSpace(out))
			fmt.Fprintf(os.Stderr, "NOTE: chctl peer drop on %s failed (%s); the weekly dead-replica GC will clean up\n", peer.Hostname, strings.TrimSpace(out))
			// Fallback: local drop.
			_, _ = runCommand(drainCtx, "clickhouse-client", "--query",
				"SYSTEM DROP REPLICA '"+st.Identity.Hostname+"'")
		}
	}

	// 3. Release DNS + lease (best-effort — the instance is dying).
	if dns := port.AsDNSRegistrar(provider); dns != nil && st.Identity.ZoneID != "" {
		if err := dns.DeleteARecord(drainCtx, port.ARecordRequest{
			ZoneID:     st.Identity.ZoneID,
			ZoneName:   st.dnsZone(),
			RecordName: st.Identity.Hostname,
			TTL:        60,
		}); err != nil {
			slog.Warn("WARNING: drain DNS release failed",
				"record", st.Identity.Hostname,
				"err", err)
		} else {
			slog.Info("drain: DNS A record released",
				"record", st.Identity.Hostname,
				"zone_id", st.Identity.ZoneID)
		}
	}
	if claimer := port.AsOrdinalClaimer(provider); claimer != nil && st.Identity.LeaseTable != "" {
		if err := claimer.ReleaseLease(drainCtx, port.ReleaseRequest{
			Table:    st.Identity.LeaseTable,
			Hostname: st.Identity.Hostname,
		}); err != nil {
			slog.Warn("WARNING: drain lease release failed",
				"hostname", st.Identity.Hostname,
				"lease_table", st.Identity.LeaseTable,
				"err", err)
		} else {
			slog.Info("drain: lease released",
				"hostname", st.Identity.Hostname,
				"lease_table", st.Identity.LeaseTable)
		}
	}

	// 4. SYSTEM SHUTDOWN: stop accepting new queries gracefully.
	if _, err := runCommand(drainCtx, "clickhouse-client", "--query", "SYSTEM SHUTDOWN"); err != nil {
		slog.Warn("WARNING: graceful shutdown failed; falling back to systemctl stop", "err", err)
		_, _ = runCommand(drainCtx, "systemctl", "stop", "clickhouse-server")
	} else {
		slog.Info("drain: server shut down gracefully")
	}
	slog.Info("drain: complete", "hostname", st.Identity.Hostname)
	fmt.Println("drain: complete")
	return nil
}

// dropOnPeer runs the chctl replica drop against one live peer,
// preserving the drain's argv shape: the peer's own CR slot flag
// (writers 01-{N}; readers 01-{writerCeiling+N} — the same
// claimCRSlot arithmetic the inventory flags use) plus self, so
// chctl resolves both replica addresses. The peer's -on address is
// its DNS name; a CR slot the peer cannot have (beyond a shrunk
// ceiling) or a peer with no slot arithmetic (nil spec) means no peer
// flag — chctl falls back to its spec-resolved address for the peer.
func dropOnPeer(ctx context.Context, st *nodeState, spec *inventorySpec, peer *port.Claim) (string, error) {
	peerFQDN := peer.Hostname + "." + st.dnsZone()
	argv := []string{"replica", "drop",
		"-spec", service.BootstrapDir + "/chi.yaml",
		"-cluster", st.clusterName(),
		"-on", peerFQDN,
	}
	if role, ordinal, ok := claimRoleOrdinal(peer.Hostname, spec.writerPrefix(), spec.readerPrefix()); ok {
		if slot := claimCRSlot(role, ordinal, spec.writerCeiling()); slot >= 0 {
			argv = append(argv, "-host", fmt.Sprintf("01-%d=%s", slot, peerFQDN))
		}
	}
	argv = append(argv,
		"-host", st.Identity.CRName+"="+st.Identity.Hostname+"."+st.dnsZone(),
		"-user", "admin",
		"-password", adminPassword(),
		st.Identity.Hostname,
	)
	return chctl(ctx, argv...)
}
