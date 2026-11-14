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
	"bytes"
	"context"
	"fmt"
	"log/slog"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	chctlport "github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins the settle-identity blind-spot fixes:
//
//   - the ZoneID second persist: the FIRST persist runs before the DNS
//     block resolves the zone id, so without a second persist after
//     the DNS block the state file never carries zone_id and the
//     drain's DNS release (keyed on the state's ZoneID) is dead code
//     for every node;
//   - the empty-lease-table Fatal: a lease spec with no table name
//     used to sweep ordinals against DynamoDB table "" — a silent,
//     undebuggable claim failure inside the ASG's boot loop.
//
// The fakes come from ceiling_test.go (recordingClaimer,
// fakeLeaseProvider) and inventory_test.go (the DNS-capable provider
// shape) — these specs add only the recorders the new behavior needs.

// recordingPersister captures every PersistIdentity call.
type recordingPersister struct {
	calls []model.NodeIdentity
}

// dnsRegistrarProvider adds the DNS capability to the recording claimer:
// settleIdentity's DNS block resolves the zone and upserts the A record
// through it.
type dnsRegistrarProvider struct {
	recorder *recordingClaimer
	zoneID   string
	upserts  []chinitport.ARecordRequest
}

func (p *dnsRegistrarProvider) Name() string { return "aws" }

func (p *dnsRegistrarProvider) Metadata(context.Context) (chinitport.InstanceMetadata, error) {
	return chinitport.InstanceMetadata{}, nil
}

func (p *dnsRegistrarProvider) GetSecretValue(context.Context, chctlport.SecretRequest) (string, error) {
	return "", nil
}

func (p *dnsRegistrarProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

// Claim capability delegates to the recording claimer.
func (p *dnsRegistrarProvider) ClaimOrdinal(ctx context.Context, req chinitport.ClaimRequest) (int, error) {
	return p.recorder.ClaimOrdinal(ctx, req)
}

func (p *dnsRegistrarProvider) RenewLease(ctx context.Context, req chinitport.RenewRequest) error {
	return p.recorder.RenewLease(ctx, req)
}

func (p *dnsRegistrarProvider) ReleaseLease(ctx context.Context, req chinitport.ReleaseRequest) error {
	return p.recorder.ReleaseLease(ctx, req)
}

// DNS capability: a fixed zone id and a recording upsert.
func (p *dnsRegistrarProvider) ResolveZoneID(context.Context, string) (string, error) {
	return p.zoneID, nil
}

func (p *dnsRegistrarProvider) UpsertARecord(_ context.Context, req chinitport.ARecordRequest) error {
	p.upserts = append(p.upserts, req)
	return nil
}

func (p *dnsRegistrarProvider) DeleteARecord(context.Context, chinitport.ARecordRequest) error {
	return nil
}

// captureSlog swaps the package default logger for a buffer-backed one
// and returns the restore func plus the buffer. The handler writes
// level+message so specs can assert on records without the default
// logger's timestamp noise.
func captureSlog() (*bytes.Buffer, func()) {
	buf := &bytes.Buffer{}
	orig := slog.Default()
	// Both levels to one buffer: the specs assert Warn/Info presence,
	// not the stream split (the split handler is the app package's
	// concern; the domain logs through the default logger).
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return buf, func() { slog.SetDefault(orig) }
}

var _ = Describe("settleIdentity blind-spot contracts", func() {
	// bootWithPersister wires the leased-writer boot with the DNS
	// capability and a recording persister.
	bootWithPersister := func(claimer *recordingClaimer, zoneID string) (*Boot, *recordingPersister, *dnsRegistrarProvider) {
		persister := &recordingPersister{}
		dns := &dnsRegistrarProvider{recorder: claimer, zoneID: zoneID}
		spec := ceilingSpec()
		b := &Boot{
			Spec:            &spec,
			Provider:        dns,
			PersistIdentity: func(id model.NodeIdentity) { persister.calls = append(persister.calls, id) },
			Meta: chinitport.InstanceMetadata{
				Provider:   "aws",
				Region:     "us-east-1",
				InstanceID: "i-blindspot-test",
				PrivateIP:  "10.0.1.51",
			},
		}
		b.SetHostname = func(context.Context, string) error { return nil }
		return b, persister, dns
	}

	Context("the ZoneID second persist", func() {
		It("persists identity again after the DNS block enriches it with the zone id", func() {
			claimer := &recordingClaimer{ordinal: 1}
			b, persister, dns := bootWithPersister(claimer, "z-abc123")

			Expect(b.settleIdentity(context.Background())).To(Succeed())
			Expect(dns.upserts).To(HaveLen(1), "the A record was registered")

			// THE regression: the first persist carries an EMPTY zone
			// id (it runs before the DNS block); the second persist
			// must carry the resolved one — the drain releases the A
			// record through the state file's zone_id.
			Expect(persister.calls).To(HaveLen(2),
				"identity must persist twice: pre-DNS and post-DNS enrichment")
			Expect(persister.calls[0].ZoneID).To(BeEmpty(),
				"the first (pre-DNS) persist cannot know the zone id")
			Expect(persister.calls[1].ZoneID).To(Equal("z-abc123"),
				"the second persist must carry the resolved zone id — the drain's DNS release reads it from the state")
			Expect(persister.calls[1].Hostname).To(Equal("writer-1"))
		})

		It("persists once when the DNS block does not run (no capability)", func() {
			// The no-capability shape (the ceiling suite's
			// fakeLeaseProvider): no DNS registrar, so no enrichment —
			// the single pre-DNS persist stands.
			claimer := &recordingClaimer{ordinal: 0}
			persister := &recordingPersister{}
			spec := ceilingSpec()
			b := &Boot{
				Spec:            &spec,
				Provider:        &fakeLeaseProvider{recordingClaimer: claimer},
				PersistIdentity: func(id model.NodeIdentity) { persister.calls = append(persister.calls, id) },
				Meta: chinitport.InstanceMetadata{
					Provider:   "aws",
					Region:     "us-east-1",
					InstanceID: "i-nocap-test",
					PrivateIP:  "10.0.1.52",
				},
			}
			b.SetHostname = func(context.Context, string) error { return nil }

			Expect(b.settleIdentity(context.Background())).To(Succeed())
			Expect(persister.calls).To(HaveLen(1))
			Expect(persister.calls[0].ZoneID).To(BeEmpty())
		})
	})

	Context("the empty-lease-table Fatal", func() {
		It("is fatal, naming the missing spec field, before any claim attempt", func() {
			spec := ceilingSpec()
			spec.ClaimerTableName = ""
			claimer := &recordingClaimer{ordinal: 0}
			b := &Boot{
				Spec:     &spec,
				Provider: &fakeLeaseProvider{recordingClaimer: claimer},
				Meta: chinitport.InstanceMetadata{
					Provider:   "aws",
					Region:     "us-east-1",
					InstanceID: "i-empty-table-test",
					PrivateIP:  "10.0.1.53",
				},
			}
			b.SetHostname = func(context.Context, string) error { return nil }

			err := b.settleIdentity(context.Background())
			Expect(err).To(HaveOccurred())
			Expect(fail.IsFatal(err)).To(BeTrue(),
				"a spec missing claimer_table_name is a config bug — retrying masks it")
			Expect(err.Error()).To(ContainSubstring("claimer_table_name"))
			Expect(err.Error()).To(ContainSubstring("CHINIT_LEASE_TABLE"))
			Expect(claimer.requests).To(BeEmpty(),
				"the claim must never run against an empty table name")
		})
	})

	Context("claim-path slog records", func() {
		It("logs the claim and its outcome on the critical path", func() {
			buf, restore := captureSlog()
			DeferCleanup(restore)

			claimer := &recordingClaimer{ordinal: 2}
			b, _, _ := bootWithPersister(claimer, "z-abc123")

			Expect(b.settleIdentity(context.Background())).To(Succeed())
			out := buf.String()
			Expect(out).To(ContainSubstring("identity: claiming ordinal lease"))
			Expect(out).To(ContainSubstring("hostname_prefix=writer-"))
			Expect(out).To(ContainSubstring("lease_table=node-claims"))
			Expect(out).To(ContainSubstring("identity: lease claimed"))
			Expect(out).To(ContainSubstring("hostname=writer-2"))
			Expect(out).To(ContainSubstring("identity: DNS A record registered"))
		})
	})

	Context("the DNS failure paths stay best-effort with loud warnings", func() {
		It("warns on registration failure and keeps the zone id in the identity", func() {
			// A DNS registrar whose upsert fails: the boot continues, the
			// warning lands in the log, and the ZoneID still persists —
			// the drain may retry the release.
			claimer := &recordingClaimer{ordinal: 0}
			persister := &recordingPersister{}
			failing := &failingUpsertProvider{recorder: claimer, zoneID: "z-xyz"}
			spec := ceilingSpec()
			b := &Boot{
				Spec:            &spec,
				Provider:        failing,
				PersistIdentity: func(id model.NodeIdentity) { persister.calls = append(persister.calls, id) },
				Meta: chinitport.InstanceMetadata{
					Provider:   "aws",
					Region:     "us-east-1",
					InstanceID: "i-dnsfail-test",
					PrivateIP:  "10.0.1.54",
				},
			}
			b.SetHostname = func(context.Context, string) error { return nil }

			buf, restore := captureSlog()
			DeferCleanup(restore)

			Expect(b.settleIdentity(context.Background())).To(Succeed())
			Expect(buf.String()).To(ContainSubstring("WARNING: DNS registration failed"))
			Expect(persister.calls).To(HaveLen(2))
			Expect(persister.calls[1].ZoneID).To(Equal("z-xyz"))
		})
	})

	Context("the state-file round trip", func() {
		It("carries zone_id through the engine persister (the drain's DNS release reads it)", func() {
			// The blind spot's user-visible symptom: state.json never
			// carried zone_id, so `chinit drain`'s DNS release was
			// dead code for every node. Pin the FULL chain: the
			// second persist must reach the file, and the file's
			// identity must carry the zone id the drain keys on.
			store, err := NewFileStateStore(GinkgoT().TempDir() + "/state.json")
			Expect(err).NotTo(HaveOccurred())
			engine := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1")
			persist := engine.IdentityPersister("aws")
			Expect(persist).NotTo(BeNil())

			// The pre-DNS persist (empty zone id) then the DNS-block
			// persist — exactly the sequence settleIdentity drives.
			persist(model.NodeIdentity{
				Role:        model.RoleWriter,
				Hostname:    "writer-1",
				InstanceID:  "i-rt",
				PrivateIP:   "10.0.1.11",
				Region:      "us-east-1",
				ClusterName: "logexplorer",
				DNSZone:     "ch.internal",
				CRName:      "01-1",
				CROrdinal:   1,
				LeaseTable:  "node-claims",
			})
			persist(model.NodeIdentity{
				Role:        model.RoleWriter,
				Hostname:    "writer-1",
				InstanceID:  "i-rt",
				PrivateIP:   "10.0.1.11",
				Region:      "us-east-1",
				ClusterName: "logexplorer",
				DNSZone:     "ch.internal",
				CRName:      "01-1",
				CROrdinal:   1,
				ZoneID:      "z-rt-123",
				LeaseTable:  "node-claims",
			})

			loaded, err := store.Load()
			Expect(err).NotTo(HaveOccurred())
			Expect(loaded.Identity).NotTo(BeNil())
			Expect(loaded.Identity.Hostname).To(Equal("writer-1"))
			Expect(loaded.Identity.ZoneID).To(Equal("z-rt-123"),
				"the state file must carry the DNS block's zone id — the drain's DeleteARecord is gated on it")
		})
	})
})

// failingUpsertProvider is the DNS shape whose record upsert errors (the
// `|| echo WARNING` degradation the bash script carried).
type failingUpsertProvider struct {
	recorder *recordingClaimer
	zoneID   string
}

func (p *failingUpsertProvider) Name() string { return "aws" }

func (p *failingUpsertProvider) Metadata(context.Context) (chinitport.InstanceMetadata, error) {
	return chinitport.InstanceMetadata{}, nil
}

func (p *failingUpsertProvider) GetSecretValue(context.Context, chctlport.SecretRequest) (string, error) {
	return "", nil
}

func (p *failingUpsertProvider) FetchBundle(context.Context, model.ConfigBundle, string) ([]string, error) {
	return nil, nil
}

func (p *failingUpsertProvider) ClaimOrdinal(ctx context.Context, req chinitport.ClaimRequest) (int, error) {
	return p.recorder.ClaimOrdinal(ctx, req)
}

func (p *failingUpsertProvider) RenewLease(ctx context.Context, req chinitport.RenewRequest) error {
	return p.recorder.RenewLease(ctx, req)
}

func (p *failingUpsertProvider) ReleaseLease(ctx context.Context, req chinitport.ReleaseRequest) error {
	return p.recorder.ReleaseLease(ctx, req)
}

func (p *failingUpsertProvider) ResolveZoneID(context.Context, string) (string, error) {
	return p.zoneID, nil
}

func (p *failingUpsertProvider) UpsertARecord(context.Context, chinitport.ARecordRequest) error {
	return fmt.Errorf("route53 throttled")
}

func (p *failingUpsertProvider) DeleteARecord(context.Context, chinitport.ARecordRequest) error {
	return nil
}
