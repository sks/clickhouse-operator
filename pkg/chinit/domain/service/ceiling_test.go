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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This suite pins the writer-ASG identity/CR-naming contract: CR slots
// are keyed on fixed ceilings (the ASG maxima), so scaling either role
// never re-keys the other role's CR names. The plain tests alongside
// (boot_test.go) keep the legacy shapes; these specs own the ceiling
// arithmetic A3's live-claims inventory and C5's Terraform template
// build against.

// recordingClaimer is the fake OrdinalClaimer: it records every claim
// request settleIdentity builds and hands back a canned ordinal.
type recordingClaimer struct {
	requests []port.ClaimRequest
	// ordinal is the canned claim result.
	ordinal int
}

func (c *recordingClaimer) ClaimOrdinal(_ context.Context, req port.ClaimRequest) (int, error) {
	c.requests = append(c.requests, req)
	return c.ordinal, nil
}

func (c *recordingClaimer) RenewLease(_ context.Context, _ port.RenewRequest) error {
	return nil
}

func (c *recordingClaimer) ReleaseLease(_ context.Context, _ port.ReleaseRequest) error {
	return nil
}

// fakeLeaseProvider is the minimal port.Provider carrying the
// recording claimer. settleIdentity touches only the claim capability
// (the DNS registrar is absent on purpose — registration is skipped,
// which is the no-capability degradation the port models).
type fakeLeaseProvider struct {
	*recordingClaimer
}

func (p *fakeLeaseProvider) Name() string { return "aws" }

func (p *fakeLeaseProvider) Metadata(context.Context) (port.InstanceMetadata, error) {
	return port.InstanceMetadata{}, nil
}

func (p *fakeLeaseProvider) GetSecretValue(context.Context, port.SecretRequest) (string, error) {
	return "", nil
}

func (p *fakeLeaseProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

// ceilingSpec is the valid writer-lease shape the specs mutate. The
// legacy count fields sit deliberately BELOW the ceilings so a
// regression to count-based arithmetic fails the Count/offset
// assertions.
func ceilingSpec() model.BootSpec {
	return model.BootSpec{
		Role:             model.RoleWriter,
		IdentityMode:     model.IdentityLease,
		WriterHostPrefix: "writer-",
		WriterMax:        4,
		WriterCount:      2,
		WriterNodes:      []string{"writer-0", "writer-1"},
		ClusterName:      "logexplorer",
		DNSZoneName:      "ch.internal",
		ClaimerTableName: "node-claims",
		ConfigBundle:     model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap"},
		Disks:            model.Disks{MetadataLabel: "ch-metadata"},
		Secrets:          model.Secrets{DefaultPassword: "default-pw", AdminPassword: "admin-pw"},
	}
}

// newSettleBoot wires a Boot over the fake provider with the hostname
// seam stubbed — settleIdentity must never rename the machine the
// suite runs on.
func newSettleBoot(spec model.BootSpec, claimer *recordingClaimer) *Boot {
	b := &Boot{
		Spec:     &spec,
		Provider: &fakeLeaseProvider{recordingClaimer: claimer},
		Meta: port.InstanceMetadata{
			Provider:   "aws",
			Region:     "us-east-1",
			InstanceID: "i-ceiling-test",
			PrivateIP:  "10.0.1.50",
		},
	}
	b.SetHostname = func(context.Context, string) error { return nil }
	return b
}

var _ = Describe("CRName", func() {
	Context("writer", func() {
		It("renders 01-<ordinal> from the writer's own ordinal", func() {
			b := &Boot{Spec: &model.BootSpec{
				Role:        model.RoleWriter,
				WriterMax:   4,
				WriterCount: 2,
			}}
			b.Identity.CROrdinal = 3
			name, ordinal := b.CRName()
			Expect(name).To(Equal("01-3"))
			Expect(ordinal).To(Equal(3))
		})

		It("renders no CR name for an unset ordinal", func() {
			// A claim/derivation that never settled must not render a
			// bogus 01--1 slot — the empty name is the every-consumer
			// signal that this node holds no CR membership yet.
			b := &Boot{Spec: &model.BootSpec{Role: model.RoleWriter, WriterMax: 4}}
			b.Identity.CROrdinal = -1
			name, ordinal := b.CRName()
			Expect(name).To(BeEmpty())
			Expect(ordinal).To(Equal(-1))
		})
	})

	Context("reader with a writer ceiling set", func() {
		It("offsets the slot by WriterMax, not the live writer count", func() {
			b := &Boot{Spec: &model.BootSpec{
				Role:        model.RoleReader,
				WriterMax:   4,
				WriterCount: 2,
			}}
			b.Identity.CROrdinal = 1
			name, ordinal := b.CRName()
			Expect(name).To(Equal("01-5"))
			Expect(ordinal).To(Equal(5))
		})

		It("keeps the slot stable across writer scale events inside the ceiling", func() {
			// The exact regression this change kills: the old code keyed
			// the reader offset on WriterCount, so a writer scale-out
			// (2 -> 3) silently re-keyed every reader CR slot.
			spec := &model.BootSpec{Role: model.RoleReader, WriterMax: 4, WriterCount: 2}
			before := &Boot{Spec: spec}
			before.Identity.CROrdinal = 1
			nameBefore, _ := before.CRName()

			spec.WriterCount = 3
			after := &Boot{Spec: spec}
			after.Identity.CROrdinal = 1
			nameAfter, _ := after.CRName()

			Expect(nameBefore).To(Equal("01-5"))
			Expect(nameAfter).To(Equal("01-5"))
		})
	})

	Context("reader on the interim static-writer path (WriterMax unset)", func() {
		It("falls back to the WriterCount offset", func() {
			b := &Boot{Spec: &model.BootSpec{
				Role:        model.RoleReader,
				WriterCount: 2,
			}}
			b.Identity.CROrdinal = 1
			name, ordinal := b.CRName()
			Expect(name).To(Equal("01-3"))
			Expect(ordinal).To(Equal(3))
		})
	})

	Context("keepers and the backup replica", func() {
		It("carry no CR name (they are not CR members)", func() {
			keeper := &Boot{Spec: &model.BootSpec{Role: model.RoleKeeper}}
			keeper.Identity.CROrdinal = 0
			name, ordinal := keeper.CRName()
			Expect(name).To(BeEmpty())
			Expect(ordinal).To(Equal(-1))

			backup := &Boot{Spec: &model.BootSpec{Role: model.RoleBackup}}
			name, ordinal = backup.CRName()
			Expect(name).To(BeEmpty())
			Expect(ordinal).To(Equal(-1))
		})
	})
})

var _ = Describe("settleIdentity", func() {
	Context("writer lease claim", func() {
		It("passes the writer prefix and the WriterMax ceiling to the claimer", func() {
			spec := ceilingSpec()
			claimer := &recordingClaimer{ordinal: 1}
			b := newSettleBoot(spec, claimer)

			Expect(b.settleIdentity(context.Background())).To(Succeed())
			Expect(claimer.requests).To(HaveLen(1))
			req := claimer.requests[0]
			Expect(req.Prefix).To(Equal("writer-"))
			Expect(req.Count).To(Equal(4), "the claim sweeps the writers ASG max, not the baseline count")
			Expect(req.Table).To(Equal("node-claims"))
			Expect(b.Identity.Hostname).To(Equal("writer-1"))
			Expect(b.Identity.CROrdinal).To(Equal(1))
			Expect(b.Identity.CRName).To(Equal("01-1"))
			Expect(b.Identity.LeaseTable).To(Equal("node-claims"))
		})

		It("defaults the prefix to writer- when the spec omits writer_hostname_prefix", func() {
			spec := ceilingSpec()
			spec.WriterHostPrefix = ""
			claimer := &recordingClaimer{ordinal: 0}
			b := newSettleBoot(spec, claimer)

			Expect(b.settleIdentity(context.Background())).To(Succeed())
			Expect(claimer.requests[0].Prefix).To(Equal("writer-"))
			Expect(b.Identity.Hostname).To(Equal("writer-0"))
		})
	})

	Context("reader lease claim", func() {
		It("passes the reader prefix and the ReaderMax ceiling to the claimer", func() {
			spec := ceilingSpec()
			spec.Role = model.RoleReader
			spec.ReaderHostPrefix = "reader-"
			spec.ReaderMax = 10
			spec.ReaderCount = 3 // legacy baseline — must NOT be the claim ceiling
			spec.WriterMax = 4   // reader CR slots offset by the writer ceiling
			claimer := &recordingClaimer{ordinal: 2}
			b := newSettleBoot(spec, claimer)

			Expect(b.settleIdentity(context.Background())).To(Succeed())
			Expect(claimer.requests).To(HaveLen(1))
			req := claimer.requests[0]
			Expect(req.Prefix).To(Equal("reader-"))
			Expect(req.Count).To(Equal(10), "the claim ceiling is the readers ASG max, not reader_count")
			Expect(b.Identity.Hostname).To(Equal("reader-2"))
			// The settled CR slot: writer ceiling (4) + reader ordinal (2).
			Expect(b.Identity.CRName).To(Equal("01-6"))
			Expect(b.Identity.CROrdinal).To(Equal(6))
		})

		It("defaults the prefix to reader- when the spec omits reader_hostname_prefix", func() {
			spec := ceilingSpec()
			spec.Role = model.RoleReader
			spec.ReaderHostPrefix = ""
			spec.ReaderMax = 10
			claimer := &recordingClaimer{ordinal: 4}
			b := newSettleBoot(spec, claimer)

			Expect(b.settleIdentity(context.Background())).To(Succeed())
			Expect(claimer.requests[0].Prefix).To(Equal("reader-"))
			Expect(b.Identity.Hostname).To(Equal("reader-4"))
		})
	})

	Context("non-ASG roles demanding lease identity", func() {
		It("is fatal for a keeper without claiming anything", func() {
			// Lease identity is an ASG-member property; keepers and the
			// backup replica are static EC2s outside the CR. A Fatal
			// (not a retryable error) keeps the boot from burning the
			// claim window on a config bug.
			spec := ceilingSpec()
			spec.Role = model.RoleKeeper
			claimer := &recordingClaimer{}
			b := newSettleBoot(spec, claimer)

			err := b.settleIdentity(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(fail.IsFatal(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("only valid for writer/reader"))
			Expect(claimer.requests).To(BeEmpty())
		})
	})
})
