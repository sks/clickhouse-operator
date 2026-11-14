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
	"errors"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This suite pins the JOB inventory's live-claims path (A3): when the
// settled identity holds a lease, schema-sync's -host flags come from
// the ACTIVE claims (the same contract remote_servers renders), not
// the bundle's static spec lists. host_inventory_test.go (A2) owns the
// static slot arithmetic; these specs own the SOURCE selection and the
// live path's own arithmetic parity.

// fakeJobLister is the fake LeaseLister injected through
// specInventoryWithSource's factory seam.
type fakeJobLister struct {
	claims []port.Claim
	err    error
	tables []string
}

func (f *fakeJobLister) ListActiveClaims(_ context.Context, req port.ListClaimsRequest) ([]port.Claim, error) {
	f.tables = append(f.tables, req.Table)
	if f.err != nil {
		return nil, f.err
	}
	return f.claims, nil
}

// leasedNodeState is a settled lease identity (the shape a schema-sync
// unit reloads from the state file) plus the bundle spec the legacy
// path would parse. The bundle fields sit deliberately below the
// ceilings so a regression to spec-list enumeration fails the
// content assertions.
func leasedNodeState() *nodeState {
	return &nodeState{Identity: model.NodeIdentity{
		Role:        model.RoleReader,
		Hostname:    "reader-2",
		InstanceID:  "i-r2",
		ClusterName: "logexplorer",
		DNSZone:     "ch.internal",
		CRName:      "01-6",
		LeaseTable:  "node-claims",
	}}
}

// seedInventorySpec redirects the bundle-spec seam at a temp file with
// the given chinit.yaml content, so the suite controls the legacy
// path's source (the fixed /var/lib path is never touched). Returns
// the restore func for DeferCleanup.
func seedInventorySpec(content string) func() {
	return seedInventorySpecAt(filepath.Join(inventorySpecDir, "chinit.yaml"), func() {
		if err := os.MkdirAll(inventorySpecDir, 0755); err != nil {
			Fail(err.Error())
		}
		if err := os.WriteFile(filepath.Join(inventorySpecDir, "chinit.yaml"), []byte(content), 0644); err != nil {
			Fail(err.Error())
		}
	})
}

// seedInventorySpecAt points the seam at an arbitrary path (the
// missing-file degradation cases).
func seedInventorySpecAt(path string, prepare ...func()) func() {
	original := inventorySpecPath
	for _, p := range prepare {
		p()
	}
	inventorySpecPath = func() string { return path }
	return func() { inventorySpecPath = original }
}

// inventorySpecDir is this suite's stand-in for the bundle directory
// (outside /var/lib; per-suite).
var inventorySpecDir = filepath.Join(os.TempDir(), "chinit-app-spec-inventory-test")

// listerFactoryFor returns the factory shape production uses: the fake
// lister under a cloud name (and the "no capability" nil answer for
// the azure-shaped cases).
func listerFactoryFor(lister port.LeaseLister, cloudName string) func(string) (port.LeaseLister, string) {
	return func(string) (port.LeaseLister, string) { return lister, cloudName }
}

var _ = Describe("specInventory live-claims path", func() {
	// The bundle spec carries the ceilings the live path's slot
	// arithmetic needs (writer_max) — seeded fresh per spec so the
	// legacy-path assertions and live-path assertions share one
	// fixture definition.
	const seededSpec = `role: reader
identity_mode: lease
cluster_name: logexplorer
writer_max: 4
writer_count: 2
writer_nodes: [writer-0, writer-1]
reader_max: 10
reader_count: 3
reader_hostname_prefix: reader-
dns_zone_name: ch.internal
`

	BeforeEach(func() {
		restore := seedInventorySpec(seededSpec)
		DeferCleanup(restore)
	})

	Context("node holds a lease and the cloud can list it", func() {
		It("builds -host flags from ACTIVE claims with ceiling arithmetic", func() {
			// Mid-scale fleet: writers 0 and 2 live (1 vacant), readers
			// 0 and 2 live. Scan order shuffled on purpose — flags are
			// slot-sorted.
			lister := &fakeJobLister{claims: []port.Claim{
				{Hostname: "reader-2", InstanceID: "i-r2"},
				{Hostname: "writer-2", InstanceID: "i-w2"},
				{Hostname: "writer-0", InstanceID: "i-w0"},
				{Hostname: "reader-0", InstanceID: "i-r0"},
			}}
			st := leasedNodeState()

			out := st.specInventoryWithSource(listerFactoryFor(lister, "aws"))
			Expect(out).To(Equal([]string{
				"01-0=writer-0.ch.internal",
				"01-2=writer-2.ch.internal",
				// Readers offset by the writer ceiling (4).
				"01-4=reader-0.ch.internal",
				"01-6=reader-2.ch.internal",
			}))
			Expect(lister.tables).To(Equal([]string{"node-claims"}), "the table is the identity's LeaseTable")
		})

		It("includes THIS node's flag (self is a member)", func() {
			lister := &fakeJobLister{claims: []port.Claim{
				{Hostname: "reader-2", InstanceID: "i-r2"},
			}}
			st := leasedNodeState()

			out := st.specInventoryWithSource(listerFactoryFor(lister, "aws"))
			Expect(out).To(ContainElement("01-6=reader-2.ch.internal"))
		})

		It("drops out-of-ceiling stale claims and foreign rows", func() {
			lister := &fakeJobLister{claims: []port.Claim{
				{Hostname: "writer-0", InstanceID: "i-w0"},
				{Hostname: "writer-9", InstanceID: "i-w9"}, // beyond WriterMax 4
				{Hostname: "backup-0", InstanceID: "i-b0"}, // not an ASG claim
			}}
			st := leasedNodeState()

			out := st.specInventoryWithSource(listerFactoryFor(lister, "aws"))
			Expect(out).To(Equal([]string{"01-0=writer-0.ch.internal"}))
		})

		It("keeps the live result stable when the static spec drifts", func() {
			// The whole point of the live path: the bundle's spec lists
			// can be stale (the hourly unit reads a bundle seeded at
			// boot); the lease table is the source of truth. Changing
			// the lister changes the flags; the (absent) spec never
			// gets a chance to disagree because the live path never
			// reads it.
			lister := &fakeJobLister{claims: []port.Claim{
				{Hostname: "writer-0", InstanceID: "i-w0"},
				{Hostname: "reader-0", InstanceID: "i-r0"},
			}}
			st := leasedNodeState()

			out := st.specInventoryWithSource(listerFactoryFor(lister, "aws"))
			Expect(out).To(Equal([]string{
				"01-0=writer-0.ch.internal",
				"01-4=reader-0.ch.internal",
			}))

			lister.claims = []port.Claim{
				{Hostname: "writer-1", InstanceID: "i-w1"},
				{Hostname: "reader-0", InstanceID: "i-r0"},
			}
			Expect(st.specInventoryWithSource(listerFactoryFor(lister, "aws"))).To(Equal([]string{
				"01-1=writer-1.ch.internal",
				"01-4=reader-0.ch.internal",
			}))
		})
	})

	Context("lease table listing fails", func() {
		It("falls back to the static spec with a loud warning", func() {
			lister := &fakeJobLister{err: errors.New("lease table down")}
			st := leasedNodeState()

			out := st.specInventoryWithSource(listerFactoryFor(lister, "aws"))
			// The static enumeration's slot arithmetic (the full
			// reserved 01-0..01-13 layout) is owned by
			// host_inventory_test.go's specs over hostInventory —
			// the same function, fixture ceilings and DNS zone this
			// seeded spec carries. Here the assertion is the
			// DEGRADATION contract: the static path answered with the
			// full reserved enumeration (14 entries, vacant slots
			// included) instead of an empty or partial inventory.
			Expect(out).To(HaveLen(14))
			// The lister WAS consulted before the fallback.
			Expect(lister.tables).To(Equal([]string{"node-claims"}))
		})
	})

	Context("cloud lacks the capability", func() {
		It("falls back to static without calling any lister", func() {
			st := leasedNodeState()

			out := st.specInventoryWithSource(listerFactoryFor(nil, "azure"))
			Expect(out).To(HaveLen(14), "the seeded spec's static enumeration, with its reserved (possibly vacant) slots")
		})
	})

	Context("static node (no lease in the identity)", func() {
		It("never consults the lease table", func() {
			st := leasedNodeState()
			st.Identity.LeaseTable = "" // keeper/backup shape

			lister := &fakeJobLister{claims: []port.Claim{{Hostname: "writer-0"}}}
			out := st.specInventoryWithSource(listerFactoryFor(lister, "aws"))
			Expect(out).To(HaveLen(14), "the static path, per the seeded spec")
			Expect(lister.tables).To(BeEmpty(), "a static node has no lease table to list")
		})
	})

	Context("lease live but the bundle spec is unreadable", func() {
		It("degrades to an empty inventory with a warning, never guessed slots", func() {
			// A missing spec file: loadInventorySpec errors, so there
			// is no writer ceiling to compute reader slots from and no
			// static enumeration either — guessing slots would be worse
			// than the empty inventory chctl reports loudly.
			restore := seedInventorySpecAt("/nonexistent/chinit.yaml")
			defer restore()
			lister := &fakeJobLister{claims: []port.Claim{
				{Hostname: "writer-0", InstanceID: "i-w0"},
				{Hostname: "reader-0", InstanceID: "i-r0"},
			}}
			st := leasedNodeState()

			out := st.specInventoryWithSource(listerFactoryFor(lister, "aws"))
			Expect(out).To(BeEmpty())
			Expect(lister.tables).To(BeEmpty(), "no ceiling to compute slots from — the lister is not even consulted")
		})
	})
})
