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
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	chctlport "github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This suite pins the live-claims inventory contract (A3): the CR
// inventory renders from ACTIVE lease claims — writers 01-{ordinal},
// readers 01-{writerCeiling+ordinal} — with the static spec lists as a
// loud, warned fallback when the capability or the table is missing.
// The ceiling_test.go suite owns the identity/CRName arithmetic; these
// specs own the INVENTORY SOURCE and its slot mapping.

// fakeLeaseLister is the fake LeaseLister: canned claims (or a canned
// failure) plus a record of the table each call listed.
type fakeLeaseLister struct {
	claims []chinitport.Claim
	err    error
	tables []string
}

func (f *fakeLeaseLister) ListActiveClaims(_ context.Context, req chinitport.ListClaimsRequest) ([]chinitport.Claim, error) {
	f.tables = append(f.tables, req.Table)
	if f.err != nil {
		return nil, f.err
	}
	return f.claims, nil
}

// listerBoot is a Boot over the fake capability provider. The fake
// provider carries ONLY the lister (no claimer: settleIdentity is not
// in scope here — the ceiling suite owns it).
type listerProvider struct {
	*fakeLeaseLister
}

func (p *listerProvider) Name() string { return "aws" }

func (p *listerProvider) Metadata(context.Context) (chinitport.InstanceMetadata, error) {
	return chinitport.InstanceMetadata{}, nil
}

func (p *listerProvider) GetSecretValue(context.Context, chctlport.SecretRequest) (string, error) {
	return "", nil
}

func (p *listerProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

// inventorySpec builds the valid leased-writer shape the specs mutate.
// WriterMax 4 with only two live writers is deliberate: the ceiling is
// a RESERVATION, and a vacant slot (writer-1) must not appear in the
// inventory.
func inventoryTestSpec() model.BootSpec {
	return model.BootSpec{
		Role:             model.RoleWriter,
		IdentityMode:     model.IdentityLease,
		WriterHostPrefix: "writer-",
		WriterMax:        4,
		WriterCount:      2, // deprecated compat — must not feed slot math
		WriterNodes:      []string{"writer-0", "writer-1"},
		ReaderHostPrefix: "reader-",
		ReaderMax:        10,
		ReaderCount:      3, // legacy baseline — must not feed slot math
		ClusterName:      "logexplorer",
		DNSZoneName:      "ch.internal",
		ClaimerTableName: "node-claims",
		ConfigBundle:     model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap"},
		Disks:            model.Disks{MetadataLabel: "ch-metadata"},
		Secrets:          model.Secrets{DefaultPassword: "default-pw", AdminPassword: "admin-pw"},
	}
}

func newInventoryBoot(spec model.BootSpec, lister *fakeLeaseLister) *Boot {
	return &Boot{
		Spec:     &spec,
		Provider: &listerProvider{fakeLeaseLister: lister},
		Meta: chinitport.InstanceMetadata{
			Provider:   "aws",
			Region:     "us-east-1",
			InstanceID: "i-inventory-test",
			PrivateIP:  "10.0.1.50",
		},
		Identity: model.NodeIdentity{
			Role:        spec.Role,
			Hostname:    "writer-0",
			LeaseTable:  "node-claims",
			ClusterName: "logexplorer",
			DNSZone:     "ch.internal",
			CROrdinal:   -1,
		},
	}
}

var _ = Describe("clusterHostsWithSource", func() {
	Context("live-claims path (lease identity + listable table)", func() {
		It("maps writer and reader claims to ceiling CR slots in slot order", func() {
			// A mid-scale fleet: writers 0 and 2 live (1 vacant — scaled
			// in, lease expired and released), readers 0-1 live.
			// The scan order is deliberately shuffled: the inventory
			// must sort by CR slot, not scan order.
			lister := &fakeLeaseLister{claims: []chinitport.Claim{
				{Hostname: "reader-1", InstanceID: "i-r1", IP: "10.0.1.41"},
				{Hostname: "writer-2", InstanceID: "i-w2", IP: "10.0.1.12"},
				{Hostname: "writer-0", InstanceID: "i-w0", IP: "10.0.1.10"},
				{Hostname: "reader-0", InstanceID: "i-r0", IP: "10.0.1.40"},
			}}
			b := newInventoryBoot(inventoryTestSpec(), lister)

			hosts, note := b.clusterHostsWithSource()
			Expect(note).To(BeEmpty(), "the live path is the authoritative source — no fallback warning")
			Expect(hosts).To(Equal([]domain.HostIdentity{
				{Cloud: "aws", Region: "us-east-1", Name: "01-0", Address: "writer-0.ch.internal", Role: domain.RoleWriter},
				{Cloud: "aws", Region: "us-east-1", Name: "01-2", Address: "writer-2.ch.internal", Role: domain.RoleWriter},
				// Readers offset by the writer ceiling (4), never the
				// live writer count (2) or the deprecated WriterCount.
				{Cloud: "aws", Region: "us-east-1", Name: "01-4", Address: "reader-0.ch.internal", Role: domain.RoleReader},
				{Cloud: "aws", Region: "us-east-1", Name: "01-5", Address: "reader-1.ch.internal", Role: domain.RoleReader},
			}))
			// The table name comes from the settled identity's
			// LeaseTable (the table this node claimed from).
			Expect(lister.tables).To(Equal([]string{"node-claims"}))
		})

		It("includes THIS node's claim (self is a cluster member)", func() {
			lister := &fakeLeaseLister{claims: []chinitport.Claim{
				{Hostname: "writer-0", InstanceID: "i-inventory-test", IP: "10.0.1.50"},
			}}
			b := newInventoryBoot(inventoryTestSpec(), lister)

			hosts, _ := b.clusterHostsWithSource()
			// remote_servers and the -host inventories include self —
			// the boot's own CR slot must never be filtered out.
			Expect(hosts).To(Equal([]domain.HostIdentity{
				{Cloud: "aws", Region: "us-east-1", Name: "01-0", Address: "writer-0.ch.internal", Role: domain.RoleWriter},
			}))
		})

		It("drops out-of-ceiling stale claims and foreign rows", func() {
			// writer-4 exceeds WriterMax 4 (a stale row from a shrunk
			// max); backup-0 is not an ASG claim at all.
			lister := &fakeLeaseLister{claims: []chinitport.Claim{
				{Hostname: "writer-0", InstanceID: "i-w0", IP: "10.0.1.10"},
				{Hostname: "writer-4", InstanceID: "i-w4", IP: "10.0.1.14"},
				{Hostname: "backup-0", InstanceID: "i-b0", IP: "10.0.1.30"},
			}}
			b := newInventoryBoot(inventoryTestSpec(), lister)

			hosts, _ := b.clusterHostsWithSource()
			Expect(hosts).To(HaveLen(1))
			Expect(hosts[0].Name).To(Equal("01-0"))
		})

		It("honors custom hostname prefixes", func() {
			spec := inventoryTestSpec()
			spec.WriterHostPrefix = "ingest-"
			spec.ReaderHostPrefix = "query-"
			lister := &fakeLeaseLister{claims: []chinitport.Claim{
				{Hostname: "ingest-0", InstanceID: "i0", IP: "10.0.1.10"},
				{Hostname: "query-0", InstanceID: "i1", IP: "10.0.1.40"},
			}}
			b := newInventoryBoot(spec, lister)

			hosts, _ := b.clusterHostsWithSource()
			Expect(hosts).To(Equal([]domain.HostIdentity{
				{Cloud: "aws", Region: "us-east-1", Name: "01-0", Address: "ingest-0.ch.internal", Role: domain.RoleWriter},
				{Cloud: "aws", Region: "us-east-1", Name: "01-4", Address: "query-0.ch.internal", Role: domain.RoleReader},
			}))
		})

		It("falls back to static spec lists with a loud warning when the listing fails", func() {
			// A lease-table outage must not wedge the boot: static
			// inventory (with its vacant slots) + a warning naming the
			// failure — the degraded source is visible in the log.
			lister := &fakeLeaseLister{err: errors.New("lease table down")}
			b := newInventoryBoot(inventoryTestSpec(), lister)

			hosts, note := b.clusterHostsWithSource()
			Expect(note).To(ContainSubstring("WARNING"))
			Expect(note).To(ContainSubstring("live-claims discovery"))
			Expect(note).To(ContainSubstring("static spec"))
			// The static enumeration is the warned-about source: the
			// reserved writers (WriterMax ordinals) + readers to
			// ReaderMax — including writer-1's VACANT slot.
			Expect(hosts).To(HaveLen(14))
			Expect(hosts).To(ContainElement(domain.HostIdentity{
				Cloud: "aws", Region: "us-east-1", Name: "01-1", Address: "writer-1.ch.internal", Role: domain.RoleWriter,
			}))
		})
	})

	Context("capability nil (Azure interim shape)", func() {
		It("enumerates from static spec lists with the no-capability warning", func() {
			spec := inventoryTestSpec()
			// A provider without the lister capability: the fake
			// carries the claimer shape only (the port's degradation
			// path — AsLeaseLister probes for LeaseLister).
			b := newInventoryBoot(spec, nil)
			b.Provider = &noListerProvider{}

			hosts, note := b.clusterHostsWithSource()
			Expect(note).To(ContainSubstring("WARNING: cloud azure has no lease-listing capability"))
			Expect(hosts).To(HaveLen(14))
			Expect(hosts).To(ContainElement(domain.HostIdentity{
				Cloud: "azure", Region: "us-east-1", Name: "01-13", Address: "reader-9.ch.internal", Role: domain.RoleReader,
			}))
		})
	})

	Context("static identity (keepers/backup/static writers)", func() {
		It("uses the static WriterNodes list without a warning", func() {
			spec := inventoryTestSpec()
			spec.IdentityMode = model.IdentityStatic
			spec.WriterMax = 0
			spec.ReaderMax = 0
			b := newInventoryBoot(spec, nil)

			hosts, note := b.clusterHostsWithSource()
			Expect(note).To(BeEmpty(), "static nodes are real machines — the reserved list IS the member list")
			Expect(hosts).To(HaveLen(5)) // 2 static writers + 3 baseline readers
			Expect(hosts).To(Equal([]domain.HostIdentity{
				{Cloud: "aws", Region: "us-east-1", Name: "01-0", Address: "writer-0.ch.internal", Role: domain.RoleWriter},
				{Cloud: "aws", Region: "us-east-1", Name: "01-1", Address: "writer-1.ch.internal", Role: domain.RoleWriter},
				{Cloud: "aws", Region: "us-east-1", Name: "01-2", Address: "reader-0.ch.internal", Role: domain.RoleReader},
				{Cloud: "aws", Region: "us-east-1", Name: "01-3", Address: "reader-1.ch.internal", Role: domain.RoleReader},
				{Cloud: "aws", Region: "us-east-1", Name: "01-4", Address: "reader-2.ch.internal", Role: domain.RoleReader},
			}))
		})
	})
})

// noListerProvider is the capability-absent provider shape (Azure
// interim): mandatory capabilities only.
type noListerProvider struct{}

func (p *noListerProvider) Name() string { return "azure" }

func (p *noListerProvider) Metadata(context.Context) (chinitport.InstanceMetadata, error) {
	return chinitport.InstanceMetadata{}, nil
}

func (p *noListerProvider) GetSecretValue(context.Context, chctlport.SecretRequest) (string, error) {
	return "", nil
}

func (p *noListerProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}
