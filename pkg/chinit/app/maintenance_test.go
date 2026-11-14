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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This suite pins the writer-ASG maintenance contract (A5):
//
//   - the ordinal-0 gate: the maintenance timers install on EVERY
//     writer, but with ASG writers "writer-0" is a lease ordinal, not
//     a machine — the subcommand must no-op exit 0 on any node that
//     does not hold the settled identity writer-{prefix}0 (readers
//     and non-0 writers take the same skip path, so a manual SSM run
//     cannot fire the sweep from the wrong node);
//   - the two-pass dead-replica GC with its safety contract: the
//     reader pass ALWAYS runs; the writer pass runs only with
//     liveness data (active lease claims) and passes every live
//     writer as -exclude so a mid-boot re-join never loses its
//     registration.

// maintSpec is the bundle-spec fixture the gate and the writer pass
// read (the writer prefix and, for lease-less nodes, the claimer
// table). Carries a custom-ish default shape plus the claimer table.
const maintSpec = `role: writer
identity_mode: lease
cluster_name: logexplorer
writer_max: 4
writer_hostname_prefix: writer-
reader_max: 10
reader_hostname_prefix: reader-
dns_zone_name: ch.internal
claimer_table_name: node-claims
`

// maintDir is this suite's scratch dir — never /var/lib.
var maintDir = filepath.Join(os.TempDir(), "chinit-app-maintenance-test")

// maintFixtures installs the spec + password seams and returns the
// restore func (DeferCleanup).
func maintFixtures() func() {
	if err := os.MkdirAll(maintDir, 0755); err != nil {
		Fail(err.Error())
	}
	specPath := filepath.Join(maintDir, "chinit.yaml")
	if err := os.WriteFile(specPath, []byte(maintSpec), 0644); err != nil {
		Fail(err.Error())
	}
	passwordPath := filepath.Join(maintDir, "admin-password")
	if err := os.WriteFile(passwordPath, []byte("sekrit\n"), 0644); err != nil {
		Fail(err.Error())
	}
	origSpec, origPassword := inventorySpecPath, adminPasswordPath
	inventorySpecPath = func() string { return specPath }
	adminPasswordPath = func() string { return passwordPath }
	return func() {
		inventorySpecPath = origSpec
		adminPasswordPath = origPassword
	}
}

// maintState builds a settled-identity nodeState.
func maintState(role model.Role, hostname string) *nodeState {
	return &nodeState{Identity: model.NodeIdentity{
		Role:        role,
		Hostname:    hostname,
		ClusterName: "logexplorer",
		DNSZone:     "ch.internal",
		CRName:      "01-0",
		LeaseTable:  "node-claims",
	}}
}

// captureStderr redirects os.Stderr into a file for the duration of
// run and returns what was written — the WARNING lines the specs
// assert are the operator's journal contract (fmt.Fprintf resolves
// os.Stderr at call time, so the swap is honored).
//
// Self-sufficient on the scratch dir: Ginkgo shuffles top-level
// container order per run, so a Describe outside the maintenance
// suite (the logging specs) may run BEFORE the maintenance
// BeforeEach has created maintDir — OpenFile would fail on a fresh
// runner (the chinit-v0.1.8 release-gate flake). MkdirAll is
// idempotent and keeps the helper order-independent.
func captureStderr(run func()) string {
	if err := os.MkdirAll(maintDir, 0755); err != nil {
		Fail(err.Error())
	}
	logPath := filepath.Join(maintDir, "stderr.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		Fail(err.Error())
	}
	orig := os.Stderr
	os.Stderr = f
	defer func() { os.Stderr = orig }()
	run()
	if err := f.Close(); err != nil {
		Fail(err.Error())
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		Fail(err.Error())
	}
	return string(data)
}

var _ = Describe("gateMaintenanceJob", func() {
	BeforeEach(func() {
		DeferCleanup(maintFixtures())
	})

	DescribeTable("the ordinal-0 designation",
		func(role model.Role, hostname string, wantSkip bool) {
			Expect(gateMaintenanceJob(maintState(role, hostname))).To(Equal(wantSkip))
		},
		Entry("the writer holding ordinal 0 runs the jobs", model.RoleWriter, "writer-0", false),
		Entry("a non-0 writer skips (the ordinal is a lease, not a machine)", model.RoleWriter, "writer-1", true),
		Entry("a high-ordinal writer skips", model.RoleWriter, "writer-3", true),
		Entry("a reader skips even at ordinal 0 (SSM manual run)", model.RoleReader, "reader-0", true),
		Entry("a keeper-shaped node skips", model.RoleKeeper, "keeper-0", true),
	)

	It("honors the spec's custom writer prefix", func() {
		// The designation is prefix+0 of the prefix the BOOT used —
		// "writer-0" must not match when the stack leased "ingest-N".
		specPath := filepath.Join(maintDir, "chinit-prefix.yaml")
		if err := os.WriteFile(specPath, []byte(`role: writer
identity_mode: lease
cluster_name: logexplorer
writer_max: 2
writer_hostname_prefix: ingest-
dns_zone_name: ch.internal
`), 0644); err != nil {
			Fail(err.Error())
		}
		orig := inventorySpecPath
		inventorySpecPath = func() string { return specPath }
		defer func() { inventorySpecPath = orig }()

		Expect(gateMaintenanceJob(maintState(model.RoleWriter, "ingest-0"))).To(BeFalse(),
			"the ingest-0 holder runs the jobs")
		Expect(gateMaintenanceJob(maintState(model.RoleWriter, "writer-0"))).To(BeTrue(),
			"the default-prefixed name does not match a custom-prefix stack")
	})

	It("defaults to the model prefix when the bundle spec is unreadable", func() {
		orig := inventorySpecPath
		inventorySpecPath = func() string { return "/nonexistent/chinit.yaml" }
		defer func() { inventorySpecPath = orig }()

		Expect(gateMaintenanceJob(maintState(model.RoleWriter, "writer-0"))).To(BeFalse(),
			"the default writer- prefix holds the designation")
	})
})

var _ = Describe("deadReplicaGc", func() {
	var ops *fakeClusterOps
	var lister *fakeJobLister

	BeforeEach(func() {
		DeferCleanup(maintFixtures())
		origOps := clusterOps
		ops = &fakeClusterOps{}
		clusterOps = ops
		DeferCleanup(func() { clusterOps = origOps })
		lister = &fakeJobLister{}
	})

	Context("the ordinal-0 holder with liveness data", func() {
		It("runs the reader pass then the guarded writer pass", func() {
			lister.claims = []chinitport.Claim{
				{Hostname: "writer-0"},
				{Hostname: "writer-1", InstanceID: "i-w1"}, // mid-boot re-join, live lease
				{Hostname: "reader-2"},
			}
			st := maintState(model.RoleWriter, "writer-0")

			Expect(deadReplicaGc(context.Background(), st, lister)).To(Succeed())

			// Reader pass first (self as the detection peer, no
			// pattern = the reader-* default); writer pass second
			// with the writer pattern and one exclude per LIVE
			// writer claim (sorted).
			calls := ops.gcCalls()
			Expect(calls).To(HaveLen(2))
			Expect(calls[0].RunOn).To(Equal("writer-0"))
			Expect(calls[0].NamePattern).To(Equal(""))
			Expect(calls[0].ExcludeHosts).To(BeEmpty())
			Expect(calls[1].RunOn).To(Equal("writer-0"))
			Expect(calls[1].NamePattern).To(Equal("writer-*"))
			Expect(calls[1].ExcludeHosts).To(Equal([]string{"writer-0", "writer-1"}))
			// The lease table listed once, from the identity.
			Expect(lister.tables).To(Equal([]string{"node-claims"}))
		})

		// NOTE: the reader-exclusion contract (a live reader claim
		// never pads the writer pass's excludes) is pinned by the
		// exact-argv spec above — its Equal asserts the full writer
		// pass argv with reader-2's claim live but ABSENT from the
		// excludes, so a reader-padding regression fails it without a
		// dedicated spec re-asserting the same value.
	})

	Context("no liveness data", func() {
		It("skips the writer pass with a warning; the reader pass still runs", func() {
			// The GC safety contract's fail-safe: without liveness data
			// the writer pass is too dangerous to guess at.
			st := maintState(model.RoleWriter, "writer-0")

			Expect(deadReplicaGc(context.Background(), st, nil)).To(Succeed())
			calls := ops.gcCalls()
			Expect(calls).To(HaveLen(1), "exactly one gc call — the reader pass")
			Expect(calls[0].NamePattern).To(BeEmpty())
		})

		It("skips the writer pass when the claims scan fails", func() {
			lister.err = fmt.Errorf("lease table down")
			st := maintState(model.RoleWriter, "writer-0")

			Expect(deadReplicaGc(context.Background(), st, lister)).To(Succeed())
			calls := ops.gcCalls()
			Expect(calls).To(HaveLen(1))
			Expect(calls[0].NamePattern).To(BeEmpty())
		})

		It("skips the writer pass when no lease table is known", func() {
			// A lease-less identity (static writer) with a spec that
			// names no claimer table: no table to list, no writer pass.
			specPath := filepath.Join(maintDir, "chinit-notable.yaml")
			if err := os.WriteFile(specPath, []byte(`role: writer
identity_mode: static
cluster_name: logexplorer
dns_zone_name: ch.internal
`), 0644); err != nil {
				Fail(err.Error())
			}
			orig := inventorySpecPath
			inventorySpecPath = func() string { return specPath }
			defer func() { inventorySpecPath = orig }()

			st := maintState(model.RoleWriter, "writer-0")
			st.Identity.LeaseTable = "" // static node holds no lease

			Expect(deadReplicaGc(context.Background(), st, lister)).To(Succeed())
			Expect(lister.tables).To(BeEmpty(), "no table known — the lister is never consulted")
			calls := ops.gcCalls()
			Expect(calls).To(HaveLen(1))
			Expect(calls[0].NamePattern).To(BeEmpty())
		})
	})

	Context("pass failures", func() {
		It("fails the run when the reader pass fails", func() {
			ops.gcErrs = []error{fmt.Errorf("connection refused")}

			st := maintState(model.RoleWriter, "writer-0")
			err := deadReplicaGc(context.Background(), st, lister)
			Expect(err).To(MatchError(ContainSubstring("reader GC pass failed")))
		})

		It("fails the run when the writer pass fails but the reader pass ran", func() {
			// First call (reader pass) succeeds; second (writer pass) fails.
			ops.gcErrs = []error{nil, fmt.Errorf("peer unreachable")}

			lister.claims = []chinitport.Claim{{Hostname: "writer-0"}}
			st := maintState(model.RoleWriter, "writer-0")
			err := deadReplicaGc(context.Background(), st, lister)
			Expect(err).To(MatchError(ContainSubstring("writer GC pass failed")))
		})
	})
})

var _ = Describe("runDeadReplicaGc / runOrphanedGc (the CLI entries)", func() {
	var ops *fakeClusterOps
	var lister *fakeJobLister
	var statePath string

	writeMaintState := func(role model.Role, hostname string) {
		state := fmt.Sprintf(`{"identity":{"role":%q,"hostname":%q,"cluster_name":"logexplorer","dns_zone":"ch.internal","cr_name":"01-0","lease_table":"node-claims"}}`, role, hostname)
		if err := os.WriteFile(statePath, []byte(state), 0644); err != nil {
			Fail(err.Error())
		}
	}

	BeforeEach(func() {
		DeferCleanup(maintFixtures())
		ops = &fakeClusterOps{}
		lister = &fakeJobLister{}
		// The cluster-op seam: the entry calls the in-process port,
		// not a binary — the fake records the typed request.
		origOps := clusterOps
		clusterOps = ops
		// The provider seam (drain suite's pattern): the entry builds
		// its lister through buildProvider, and the fake keeps the
		// registry's real AWS provider (IMDS credential retries) out
		// of the test clock.
		origBuild := buildProvider
		buildProvider = func(string, string) (chinitport.Provider, error) {
			return &drainProvider{name: "aws", lister: lister}, nil
		}
		DeferCleanup(func() {
			clusterOps = origOps
			buildProvider = origBuild
		})

		statePath = filepath.Join(maintDir, "state.json")
	})

	It("no-ops exit 0 on a node that is not the ordinal-0 holder", func() {
		// The entries' only gate is gateMaintenanceJob — the same
		// function the gate DescribeTable above pins across the role
		// matrix (writer-1/3, reader-0, keeper-0), so one entry-level
		// witness per CLI entry suffices here; a per-role entry spec
		// would re-assert the gate the table already proves.
		writeMaintState(model.RoleWriter, "writer-1")
		Expect(runDeadReplicaGc([]string{"-state", statePath})).To(Succeed())
		Expect(runOrphanedGc([]string{"-state", statePath})).To(Succeed())
		Expect(ops.calls).To(BeEmpty(), "neither maintenance job reaches the cluster port")
	})

	It("runs both passes on the ordinal-0 holder with live claims", func() {
		// The entry contract: state file → ordinal-0 gate →
		// gcLivelinessFrom through the buildProvider seam → the
		// two-pass sweep. The exact argv is pinned by the
		// deadReplicaGc specs above; here the assertion is the entry
		// WIRING — two chctl passes, the second one the guarded
		// writer sweep with the live-writer excludes.
		lister.claims = []chinitport.Claim{
			{Hostname: "writer-0"},
			{Hostname: "writer-1"},
		}
		writeMaintState(model.RoleWriter, "writer-0")
		Expect(runDeadReplicaGc([]string{"-state", statePath})).To(Succeed())
		calls := ops.gcCalls()
		Expect(calls).To(HaveLen(2), "reader pass then writer pass")
		Expect(calls[1].NamePattern).To(Equal("writer-*"))
		Expect(calls[1].ExcludeHosts).To(Equal([]string{"writer-0", "writer-1"}))
		// The claims came from the entry's own provider build, not a
		// direct lister handoff.
		Expect(lister.tables).To(Equal([]string{"node-claims"}))
	})

	It("runs the reader pass only when the entry's provider has no lister (WARNING in the log)", func() {
		// The ordinal-0 holder whose cloud cannot list leases (the
		// capability-absent shape): gcLivelinessFrom answers nil, the
		// writer pass is skipped WITH the WARNING the operator reads
		// in the journal, and the reader pass still ran — the GC
		// safety contract's fail-safe at the ENTRY level (the core
		// deadReplicaGc specs pin the same pass with a nil lister
		// handed in directly).
		origBuild := buildProvider
		buildProvider = func(string, string) (chinitport.Provider, error) {
			return &bareProvider{providerName: "azure"}, nil
		}
		DeferCleanup(func() { buildProvider = origBuild })

		writeMaintState(model.RoleWriter, "writer-0")
		log := captureStderr(func() {
			Expect(runDeadReplicaGc([]string{"-state", statePath})).To(Succeed())
		})
		calls := ops.gcCalls()
		Expect(calls).To(HaveLen(1), "exactly one gc call — the reader pass")
		Expect(calls[0].NamePattern).To(BeEmpty())
		Expect(log).To(ContainSubstring("WARNING"))
		Expect(log).To(ContainSubstring("skipping the writer GC pass"))
		Expect(log).To(ContainSubstring("no lease-listing capability"))
	})

	It("runs the orphaned census on the ordinal-0 holder", func() {
		writeMaintState(model.RoleWriter, "writer-0")
		Expect(runOrphanedGc([]string{"-state", statePath})).To(Succeed())
		Expect(ops.calls).To(BeEmpty(), "orphaned-gc is a documented no-op; it reaches no cluster port")
	})
})
