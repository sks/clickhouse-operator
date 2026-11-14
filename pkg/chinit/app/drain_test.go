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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/clusterops"
)

// This suite pins the first-class `chinit drain` subcommand (A4): the
// ASG termination lifecycle hook invokes it over SSM, so the drain's
// own contract is load-bearing — the peer the replica drop runs
// against must be a LIVE lease holder (a hardcoded writer-0 is the
// ordinal the scale-in just removed), the argv shape must stay what
// chctl's CLI contract expects, and a node with no state must fail
// the SSM invocation loudly (the hook Lambda CONTINUEs, the weekly
// GC is the safety net).

// recordedCommand is one runCommand invocation.
type recordedCommand struct {
	Name string
	Args []string
}

// fakeRunner is the exec seam's recorder: every command the drain
// shells out lands here in order, and per-binary outcomes let the
// specs drive failure paths (chctl drop failing → the local
// SYSTEM DROP REPLICA fallback fires).
type fakeRunner struct {
	commands   []recordedCommand
	chctlFails bool
}

func (f *fakeRunner) run(_ context.Context, name string, args ...string) ([]byte, error) {
	f.commands = append(f.commands, recordedCommand{Name: name, Args: append([]string{}, args...)})
	if name == "chctl" && f.chctlFails {
		return []byte("connection refused"), fmt.Errorf("exit status 1")
	}
	return []byte("ok"), nil
}

// lines returns the recorded commands as "name arg1 arg2" strings —
// the order-sensitive sequence assertions read better over full argv.
func (f *fakeRunner) lines() []string {
	out := make([]string, 0, len(f.commands))
	for _, c := range f.commands {
		out = append(out, strings.Join(append([]string{c.Name}, c.Args...), " "))
	}
	return out
}

// drainProvider is the fake port.Provider with ALL the lease
// capabilities wired (LeaseLister + OrdinalClaimer + DNSRegistrar —
// the full method sets, since the capability probes are type
// assertions: a half-implemented interface reads as NO capability).
// The fixtures always populate all three recorders; specs assert on
// the ones they care about.
type drainProvider struct {
	name    string
	lister  port.LeaseLister
	claimer port.OrdinalClaimer
	dns     port.DNSRegistrar
}

func (p *drainProvider) Name() string { return p.name }

func (p *drainProvider) Metadata(context.Context) (port.InstanceMetadata, error) {
	return port.InstanceMetadata{}, nil
}

func (p *drainProvider) GetSecretValue(context.Context, port.SecretRequest) (string, error) {
	return "", nil
}

func (p *drainProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

func (p *drainProvider) ListActiveClaims(ctx context.Context, req port.ListClaimsRequest) ([]port.Claim, error) {
	return p.lister.ListActiveClaims(ctx, req)
}

func (p *drainProvider) ClaimOrdinal(ctx context.Context, req port.ClaimRequest) (int, error) {
	return p.claimer.ClaimOrdinal(ctx, req)
}

func (p *drainProvider) RenewLease(ctx context.Context, req port.RenewRequest) error {
	return p.claimer.RenewLease(ctx, req)
}

func (p *drainProvider) ReleaseLease(ctx context.Context, req port.ReleaseRequest) error {
	return p.claimer.ReleaseLease(ctx, req)
}

func (p *drainProvider) ResolveZoneID(ctx context.Context, zone string) (string, error) {
	return p.dns.ResolveZoneID(ctx, zone)
}

func (p *drainProvider) UpsertARecord(ctx context.Context, req port.ARecordRequest) error {
	return p.dns.UpsertARecord(ctx, req)
}

func (p *drainProvider) DeleteARecord(ctx context.Context, req port.ARecordRequest) error {
	return p.dns.DeleteARecord(ctx, req)
}

// bareProvider is the fake port.Provider WITHOUT any optional
// capability: AsLeaseLister/AsOrdinalClaimer/AsDNSRegistrar all
// answer nil on it, which is the azure-shaped degradation path.
type bareProvider struct {
	providerName string
}

func (p *bareProvider) Name() string { return p.providerName }

func (p *bareProvider) Metadata(context.Context) (port.InstanceMetadata, error) {
	return port.InstanceMetadata{}, nil
}

func (p *bareProvider) GetSecretValue(context.Context, port.SecretRequest) (string, error) {
	return "", nil
}

func (p *bareProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

// drainClaimer records the lease release the drain performs, and the
// renewals the lease-renew subcommand drives through the same
// capability seam.
type drainClaimer struct {
	releases []port.ReleaseRequest
	renews   []port.RenewRequest
}

func (c *drainClaimer) ClaimOrdinal(context.Context, port.ClaimRequest) (int, error) {
	return 0, nil
}

func (c *drainClaimer) RenewLease(_ context.Context, req port.RenewRequest) error {
	c.renews = append(c.renews, req)
	return nil
}

func (c *drainClaimer) ReleaseLease(_ context.Context, req port.ReleaseRequest) error {
	c.releases = append(c.releases, req)
	return nil
}

// drainDNS records the A-record delete.
type drainDNS struct {
	deletes []port.ARecordRequest
}

func (d *drainDNS) ResolveZoneID(context.Context, string) (string, error) { return "z-1", nil }

func (d *drainDNS) UpsertARecord(context.Context, port.ARecordRequest) error { return nil }

func (d *drainDNS) DeleteARecord(_ context.Context, req port.ARecordRequest) error {
	d.deletes = append(d.deletes, req)
	return nil
}

// fakeClusterOps records the clusterOps calls the subcommands make so
// the specs assert on the typed requests instead of any argv. The
// zero value is ready to use; setting dropErr/gcErr/failRunOn drives
// the failure paths.
type fakeClusterOps struct {
	calls   []recordedClusterOp
	dropErr error
	// gcErrs is a per-call sequence of errors for GcReplica: nil entry
	// = success. A short slice is padded with nils (remaining calls succeed).
	gcErrs      []error
	gcCallCount int
}

type recordedClusterOp struct {
	Op  string
	Req any
}

func (f *fakeClusterOps) SyncSchema(_ context.Context, req clusterops.SyncSchemaRequest) error {
	f.calls = append(f.calls, recordedClusterOp{Op: "SyncSchema", Req: req})
	return nil
}
func (f *fakeClusterOps) SyncReplica(_ context.Context, req clusterops.SyncReplicaRequest) error {
	f.calls = append(f.calls, recordedClusterOp{Op: "SyncReplica", Req: req})
	return nil
}
func (f *fakeClusterOps) DropReplica(_ context.Context, req clusterops.DropReplicaRequest) error {
	f.calls = append(f.calls, recordedClusterOp{Op: "DropReplica", Req: req})
	return f.dropErr
}
func (f *fakeClusterOps) GcReplica(_ context.Context, req clusterops.GcReplicaRequest) (*clusterops.GcResult, error) {
	f.calls = append(f.calls, recordedClusterOp{Op: "GcReplica", Req: req})
	idx := f.gcCallCount
	f.gcCallCount++
	if idx < len(f.gcErrs) && f.gcErrs[idx] != nil {
		return nil, f.gcErrs[idx]
	}
	return &clusterops.GcResult{Dropped: []string{"stale-host"}}, nil
}
func (f *fakeClusterOps) KeeperMntr(context.Context, clusterops.MntrRequest) (string, error) {
	return "", nil
}
func (f *fakeClusterOps) Health(context.Context, clusterops.HealthRequest) (*clusterops.HealthReport, error) {
	return &clusterops.HealthReport{Healthy: true}, nil
}
func (f *fakeClusterOps) gcCalls() []clusterops.GcReplicaRequest {
	var out []clusterops.GcReplicaRequest
	for _, c := range f.calls {
		if c.Op == "GcReplica" {
			out = append(out, c.Req.(clusterops.GcReplicaRequest))
		}
	}
	return out
}
func (f *fakeClusterOps) dropCalls() []clusterops.DropReplicaRequest {
	var out []clusterops.DropReplicaRequest
	for _, c := range f.calls {
		if c.Op == "DropReplica" {
			out = append(out, c.Req.(clusterops.DropReplicaRequest))
		}
	}
	return out
}

// drainSpec is the fixture bundle spec the slot arithmetic reads:
// writer_max 4 / reader_max 10 — the CR ceilings.
const drainSpec = `role: writer
identity_mode: lease
cluster_name: logexplorer
writer_max: 4
reader_max: 10
reader_hostname_prefix: reader-
dns_zone_name: ch.internal
`

// drainDir is this suite's scratch dir (bundle spec, admin password,
// state files) — never /var/lib.
var drainDir = filepath.Join(os.TempDir(), "chinit-app-drain-test")

// drainFakes bundles the fixtures a drain spec drives: the exec
// recorder plus the provider capabilities.
type drainFakes struct {
	runner  *fakeRunner
	lister  *fakeJobLister
	claimer *drainClaimer
	dns     *drainDNS
	ops     *fakeClusterOps
}

// drainFixtures installs the runner, spec, password and provider
// seams, builds a fresh capability set, and returns the fakes plus
// the restore func (DeferCleanup). The provider seam makes the
// subcommand entry specs (runDrain) run against the fake instead of
// the real registry provider (AWS IMDS retries would dominate the
// test clock).
func drainFixtures() (*drainFakes, func()) {
	f := &drainFakes{
		runner:  &fakeRunner{},
		lister:  &fakeJobLister{},
		claimer: &drainClaimer{},
		dns:     &drainDNS{},
		ops:     &fakeClusterOps{},
	}
	origRun := runCommand
	runCommand = f.runner.run
	origOps := clusterOps
	clusterOps = f.ops
	origBuild := buildProvider
	buildProvider = func(string, string) (port.Provider, error) {
		return &drainProvider{name: "aws", lister: f.lister, claimer: f.claimer, dns: f.dns}, nil
	}

	if err := os.MkdirAll(drainDir, 0755); err != nil {
		Fail(err.Error())
	}
	specPath := filepath.Join(drainDir, "chinit.yaml")
	if err := os.WriteFile(specPath, []byte(drainSpec), 0644); err != nil {
		Fail(err.Error())
	}
	passwordPath := filepath.Join(drainDir, "admin-password")
	if err := os.WriteFile(passwordPath, []byte("sekrit\n"), 0644); err != nil {
		Fail(err.Error())
	}
	origSpec, origPassword := inventorySpecPath, adminPasswordPath
	inventorySpecPath = func() string { return specPath }
	adminPasswordPath = func() string { return passwordPath }

	return f, func() {
		runCommand = origRun
		clusterOps = origOps
		buildProvider = origBuild
		inventorySpecPath = origSpec
		adminPasswordPath = origPassword
	}
}

// hostPairs renders a ClusterRequest host list as ["01-0=addr", ...]
// so the specs assert the inventory shape without reaching into the
// struct. Order follows the request's construction order.
func hostPairs(hosts []clusterops.Host) []string {
	out := make([]string, 0, len(hosts))
	for _, h := range hosts {
		out = append(out, h.Name+"="+h.Address)
	}
	return out
}

// drainer builds a writer-1 nodeState — the node the ASG termination
// hook is draining.
func drainer() *nodeState {
	return &nodeState{Identity: model.NodeIdentity{
		Role:        model.RoleWriter,
		Hostname:    "writer-1",
		InstanceID:  "i-w1",
		ClusterName: "logexplorer",
		DNSZone:     "ch.internal",
		CRName:      "01-1",
		ZoneID:      "z-1",
		LeaseTable:  "node-claims",
	}}
}

var _ = Describe("drain peer selection", func() {
	var st *nodeState
	var lister *fakeJobLister

	BeforeEach(func() {
		st = drainer()
		lister = &fakeJobLister{}
	})

	// pick returns the selected peer's hostname ("" when nil) — the
	// selection contract, not the pointer.
	pick := func() string {
		peer := selectDrainPeer(context.Background(), st, &inventorySpec{
			WriterMax:        4,
			ReaderMax:        10,
			ReaderHostPrefix: "reader-",
		}, lister, "aws")
		if peer == nil {
			return ""
		}
		return peer.Hostname
	}

	Context("writer-0 is live", func() {
		It("prefers writer-0", func() {
			lister.claims = []port.Claim{
				{Hostname: "writer-1"}, // self
				{Hostname: "writer-0"},
				{Hostname: "writer-3"},
				{Hostname: "reader-0"},
			}
			Expect(pick()).To(Equal("writer-0"))
		})
	})

	Context("writer-0 absent, other writers live", func() {
		It("picks the lowest live writer ordinal", func() {
			// Scan order deliberately unsorted; ordinals 10 vs 2
			// catch a lexical sort picking the wrong "lowest".
			lister.claims = []port.Claim{
				{Hostname: "writer-10"},
				{Hostname: "writer-2"},
				{Hostname: "writer-5"},
			}
			Expect(pick()).To(Equal("writer-2"))
		})

		It("excludes self from the candidates", func() {
			// Draining writer-1 while ONLY writer-1 is live among
			// writers: the peer cannot be self — the readers answer.
			lister.claims = []port.Claim{
				{Hostname: "writer-1"},
				{Hostname: "reader-0"},
			}
			Expect(pick()).To(Equal("reader-0"))
		})
	})

	Context("the node being drained holds writer-0 (self == the preferred peer)", func() {
		It("never selects self — the next live writer answers", func() {
			// The termination hook fires for ANY writer, including the
			// ordinal-0 holder: writer-0 is the PREFERRED peer, so
			// the preference must not override the self-exclusion —
			// draining writer-0 against itself is the no-op drop that
			// strands the registration.
			st = &nodeState{Identity: model.NodeIdentity{
				Role:        model.RoleWriter,
				Hostname:    "writer-0",
				ClusterName: "logexplorer",
				DNSZone:     "ch.internal",
				CRName:      "01-0",
				LeaseTable:  "node-claims",
			}}
			lister.claims = []port.Claim{
				{Hostname: "writer-0"}, // self, first in scan order
				{Hostname: "writer-2"},
				{Hostname: "writer-1"},
			}
			Expect(pick()).To(Equal("writer-1"))
		})
	})

	Context("only readers live (last-writer scale-in)", func() {
		It("picks the lowest-ordinal reader — a full replica carries the drop", func() {
			lister.claims = []port.Claim{
				{Hostname: "reader-3"},
				{Hostname: "reader-0"},
			}
			Expect(pick()).To(Equal("reader-0"))
		})
	})

	Context("no live claims at all", func() {
		It("falls back to the writer-0 DNS name", func() {
			Expect(pick()).To(Equal("writer-0"))
		})
	})

	Context("claims scan fails", func() {
		It("falls back to the writer-0 DNS name", func() {
			lister.err = fmt.Errorf("lease table down")
			Expect(pick()).To(Equal("writer-0"))
		})
	})

	Context("cloud has no lease-listing capability", func() {
		It("falls back to the writer-0 DNS name without scanning", func() {
			peer := selectDrainPeer(context.Background(), st, &inventorySpec{WriterMax: 4}, nil, "azure")
			Expect(peer).NotTo(BeNil())
			Expect(peer.Hostname).To(Equal("writer-0"))
			Expect(lister.tables).To(BeEmpty(), "no capability means no scan")
		})
	})

	Context("stale rows beyond the ceiling", func() {
		It("ignores writer claims at or beyond writer_max", func() {
			lister.claims = []port.Claim{
				{Hostname: "writer-4"}, // == ceiling: no slot
				{Hostname: "writer-9"}, // beyond ceiling
				{Hostname: "reader-1"},
			}
			Expect(pick()).To(Equal("reader-1"))
		})
	})
})

var _ = Describe("drain", func() {
	var f *drainFakes

	BeforeEach(func() {
		var restore func()
		f, restore = drainFixtures()
		DeferCleanup(restore)
	})

	It("runs the hook's full sequence against a live writer-0 peer", func() {
		st := drainer()
		f.lister.claims = []port.Claim{
			{Hostname: "writer-0"},
			{Hostname: "writer-1"}, // self
		}
		provider := &drainProvider{name: "aws", lister: f.lister, claimer: f.claimer, dns: f.dns}
		Expect(drain(context.Background(), st, provider, drainOptions{
			DeadlineSeconds: 240,
			Reason:          drainReasonTerminationHook,
		})).To(Succeed())

		// The sequence the hook Lambda relies on, in order: stop
		// queues, stop sends, peer drop on the LIVE writer-0 (self
		// plus the peer's CR slot), then the graceful shutdown. DNS +
		// lease release run between drop and shutdown — pinned via the
		// capability recorders below, not the exec order.
		Expect(f.runner.lines()).To(Equal([]string{
			"clickhouse-client --query SYSTEM STOP REPLICATION QUEUES",
			"clickhouse-client --query SYSTEM STOP DISTRIBUTED SENDS",
			"clickhouse-client --query SYSTEM SHUTDOWN",
		}))
		drops := f.ops.dropCalls()
		Expect(drops).To(HaveLen(1))
		Expect(drops[0].RunOn).To(Equal("writer-0.ch.internal"))
		Expect(drops[0].HostName).To(Equal("writer-1"))
		Expect(hostPairs(drops[0].Cluster.Hosts)).To(Equal([]string{"01-1=writer-1.ch.internal", "01-0=writer-0.ch.internal"}))
		Expect(drops[0].Cluster.ClusterName).To(Equal("logexplorer"))
		Expect(drops[0].Cluster.Credentials.Password).To(Equal("sekrit"))

		// The lease went back to the table.
		Expect(f.claimer.releases).To(Equal([]port.ReleaseRequest{
			{Table: "node-claims", Hostname: "writer-1"},
		}))
		// The A record went away.
		Expect(f.dns.deletes).To(HaveLen(1))
		Expect(f.dns.deletes[0].RecordName).To(Equal("writer-1"))
		Expect(f.dns.deletes[0].ZoneID).To(Equal("z-1"))
	})

	It("drops on a reader peer at the ceiling-offset CR slot", func() {
		st := drainer()
		f.lister.claims = []port.Claim{
			{Hostname: "writer-1"}, // self, alone among writers
			{Hostname: "reader-2"},
		}
		provider := &drainProvider{name: "aws", lister: f.lister, claimer: f.claimer, dns: f.dns}

		Expect(drain(context.Background(), st, provider, drainOptions{DeadlineSeconds: 240, Reason: drainReasonSpot})).To(Succeed())
		// Reader-2's CR slot is writerCeiling+2 = 01-6 (writer_max 4).
		drops := f.ops.dropCalls()
		Expect(drops).To(HaveLen(1))
		Expect(drops[0].RunOn).To(Equal("reader-2.ch.internal"))
		Expect(hostPairs(drops[0].Cluster.Hosts)).To(Equal([]string{"01-1=writer-1.ch.internal", "01-6=reader-2.ch.internal"}))
	})

	It("drops on the lowest live writer at its own CR slot when writer-0 is gone", func() {
		// The scale-in case the hardcoded writer-0 missed: ordinal 0
		// already terminated; the drain must reach writer-2's slot
		// (01-2), not a dead 01-0.
		st := drainer()
		f.lister.claims = []port.Claim{
			{Hostname: "writer-1"}, // self
			{Hostname: "writer-5"},
			{Hostname: "writer-2"},
		}
		provider := &drainProvider{name: "aws", lister: f.lister, claimer: f.claimer, dns: f.dns}

		Expect(drain(context.Background(), st, provider, drainOptions{DeadlineSeconds: 240, Reason: drainReasonTerminationHook})).To(Succeed())
		drops := f.ops.dropCalls()
		Expect(drops).To(HaveLen(1))
		Expect(drops[0].RunOn).To(Equal("writer-2.ch.internal"))
		Expect(hostPairs(drops[0].Cluster.Hosts)).To(Equal([]string{"01-1=writer-1.ch.internal", "01-2=writer-2.ch.internal"}))
	})

	It("attempts the full ordered sequence inside a 1-second budget", func() {
		// The budget BOUNDS the drain, never truncates it: with a
		// short budget every phase still runs in the hook's order —
		// stop queues, stop sends, peer drop, shutdown — and the ctx
		// can only cut a phase that outlives it, never skip ahead.
		// (A real chctl drop that outlives the budget is killed by
		// CommandContext mid-phase; the fake runner here is instant,
		// so this spec pins the ORDERING contract, not timing.)
		st := drainer()
		f.lister.claims = []port.Claim{
			{Hostname: "writer-0"},
			{Hostname: "writer-1"}, // self
		}
		provider := &drainProvider{name: "aws", lister: f.lister, claimer: f.claimer, dns: f.dns}

		Expect(drain(context.Background(), st, provider, drainOptions{
			DeadlineSeconds: 1,
			Reason:          drainReasonTerminationHook,
		})).To(Succeed())
		Expect(f.runner.lines()).To(Equal([]string{
			"clickhouse-client --query SYSTEM STOP REPLICATION QUEUES",
			"clickhouse-client --query SYSTEM STOP DISTRIBUTED SENDS",
			"clickhouse-client --query SYSTEM SHUTDOWN",
		}), "the short budget must not silently truncate the sequence")
		Expect(f.ops.dropCalls()).To(HaveLen(1), "the peer drop still runs inside the budget")
	})

	It("falls back to the local drop when the peer drop fails", func() {
		st := drainer()
		f.lister.claims = []port.Claim{{Hostname: "writer-0"}}
		provider := &drainProvider{name: "aws", lister: f.lister, claimer: f.claimer, dns: f.dns}
		f.ops.dropErr = fmt.Errorf("peer unreachable")

		Expect(drain(context.Background(), st, provider, drainOptions{DeadlineSeconds: 240, Reason: drainReasonSpot})).To(Succeed())
		Expect(f.ops.dropCalls()).To(HaveLen(1))
		Expect(f.runner.lines()).To(ContainElement(
			"clickhouse-client --query SYSTEM DROP REPLICA 'writer-1'"))
		// The sequence still finishes: shutdown after the fallback.
		Expect(f.runner.lines()).To(ContainElement("clickhouse-client --query SYSTEM SHUTDOWN"))
	})

	It("keeps draining with no capabilities at all (writer-0 fallback)", func() {
		st := drainer()
		provider := &bareProvider{providerName: "azure"} // no lister/claimer/dns

		Expect(drain(context.Background(), st, provider, drainOptions{DeadlineSeconds: 240, Reason: drainReasonSpot})).To(Succeed())
		// The hardcoded fallback peer still gets the drop attempt.
		drops := f.ops.dropCalls()
		Expect(drops).To(HaveLen(1))
		Expect(drops[0].RunOn).To(Equal("writer-0.ch.internal"))
		Expect(hostPairs(drops[0].Cluster.Hosts)).To(Equal([]string{"01-1=writer-1.ch.internal", "01-0=writer-0.ch.internal"}))
		// And the sequence still finishes — best-effort by contract.
		Expect(f.runner.lines()).To(ContainElement("clickhouse-client --query SYSTEM SHUTDOWN"))
	})
})

var _ = Describe("runDrain", func() {
	var f *drainFakes
	var statePath string

	BeforeEach(func() {
		var restore func()
		f, restore = drainFixtures()
		DeferCleanup(restore)

		statePath = filepath.Join(drainDir, "state.json")
		state := `{"identity":{"role":"writer","hostname":"writer-1","instance_id":"i-w1","cluster_name":"logexplorer","dns_zone":"ch.internal","cr_name":"01-1","zone_id":"z-1","lease_table":"node-claims"}}`
		if err := os.WriteFile(statePath, []byte(state), 0644); err != nil {
			Fail(err.Error())
		}
	})

	It("drains from the SSM invocation's flags", func() {
		Expect(runDrain([]string{"-state", statePath, "-reason", "termination-hook"})).To(Succeed())
		// The argv contract is pinned by the drain Describe above;
		// here the assertion is the entry WIRING — the flags reached
		// the drain: state loaded from the file (the drop names the
		// state's writer-1 identity), queues stopped, a peer drop
		// attempted on the writer-0 fallback the empty fixture lease
		// table degrades to.
		lines := f.runner.lines()
		Expect(lines).To(ContainElement("clickhouse-client --query SYSTEM STOP REPLICATION QUEUES"))
		drops := f.ops.dropCalls()
		Expect(drops).NotTo(BeEmpty(), "the entry reached the peer-drop phase")
		Expect(drops[0].RunOn).To(Equal("writer-0.ch.internal"), "the fallback peer")
		Expect(drops[0].HostName).To(Equal("writer-1"), "the identity reloaded from the state file")
	})

	It("defaults the reason to termination-hook", func() {
		// The SSM document invokes `chinit drain` bare besides -state;
		// the default must be the hook (spot is spot-watch's reason).
		Expect(runDrain([]string{"-state", statePath})).To(Succeed())
	})

	It("exits with the missing-state contract when state is absent", func() {
		missing := filepath.Join(drainDir, "missing-state.json")
		err := runDrain([]string{"-state", missing})
		Expect(err).To(HaveOccurred())
		// The error tells the SSM log reader BOTH halves: why the
		// drain declined and what happens next.
		Expect(err.Error()).To(ContainSubstring("never settled identity"))
		Expect(err.Error()).To(ContainSubstring("weekly dead-replica GC"))
		Expect(err.Error()).To(ContainSubstring(missing))
	})

	It("rejects an unknown reason", func() {
		err := runDrain([]string{"-state", statePath, "-reason", "yolo"})
		Expect(err).To(MatchError(ContainSubstring("invalid -reason")))
	})

	It("rejects a non-positive timeout", func() {
		err := runDrain([]string{"-state", statePath, "-timeout", "0"})
		Expect(err).To(MatchError(ContainSubstring("invalid -timeout")))
	})
})
