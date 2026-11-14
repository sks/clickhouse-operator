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
	"encoding/json"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins `chinit lease-renew` — the every-5-minute timer the
// units matrix installs on BOTH lease-mode roles (writers since the
// writer-ASG rollout). The subcommand renews through the SETTLED
// identity's lease table and hostname; it never re-derives them.

// leaseRenewDir is this suite's scratch dir (state files) — never
// /var/lib.
var leaseRenewDir = filepath.Join(os.TempDir(), "chinit-app-lease-renew-test")

// writeLeaseState seeds a state file with the given settled identity.
func writeLeaseState(identity model.NodeIdentity) string {
	data, err := json.Marshal(nodeState{Identity: identity})
	Expect(err).NotTo(HaveOccurred())
	path := filepath.Join(leaseRenewDir, "state.json")
	Expect(os.WriteFile(path, data, 0644)).To(Succeed())
	return path
}

var _ = Describe("runLeaseRenew", func() {
	var claimer *drainClaimer

	// fixtures wires the provider seam at a full-capability fake and
	// the claimer records what the renew drove. The subcommand shells
	// nothing out (a provider call, not a chctl call), so no exec
	// recorder is needed.
	fixtures := func() func() {
		claimer = &drainClaimer{}
		origBuild := buildProvider
		buildProvider = func(string, string) (chinitport.Provider, error) {
			return &drainProvider{name: "aws", claimer: claimer}, nil
		}
		Expect(os.MkdirAll(leaseRenewDir, 0755)).To(Succeed())
		return func() { buildProvider = origBuild }
	}

	BeforeEach(func() {
		DeferCleanup(fixtures())
	})

	// The renew is identity-mode gated, never role gated: BOTH leased
	// roles' timers fire this one subcommand, and it must carry the
	// SETTLED identity (hostname, instance, table the boot claimed
	// from) — never a role assumption. The writer entry is the
	// writer-ASG addition; the reader entry pins that nothing
	// special-cased it back.
	DescribeTable("renews through the claimer with the settled identity",
		func(identity model.NodeIdentity) {
			statePath := writeLeaseState(identity)

			Expect(runLeaseRenew([]string{"-state", statePath})).To(Succeed())
			Expect(claimer.renews).To(Equal([]chinitport.RenewRequest{
				{
					Table:        identity.LeaseTable,
					Hostname:     identity.Hostname,
					InstanceID:   identity.InstanceID,
					LeaseSeconds: 900,
				},
			}))
		},
		Entry("a writer holding a lease (the writer-ASG timer)", model.NodeIdentity{
			Role:        model.RoleWriter,
			Hostname:    "writer-1",
			InstanceID:  "i-w1",
			ClusterName: "logexplorer",
			DNSZone:     "ch.internal",
			CRName:      "01-1",
			LeaseTable:  "node-claims",
		}),
		Entry("a reader holding a lease (the pre-writer timer)", model.NodeIdentity{
			Role:        model.RoleReader,
			Hostname:    "reader-3",
			InstanceID:  "i-r3",
			ClusterName: "logexplorer",
			DNSZone:     "ch.internal",
			CRName:      "01-7",
			LeaseTable:  "node-claims",
		}),
	)

	Context("refusal paths", func() {
		It("is fatal when the identity holds no lease table", func() {
			statePath := writeLeaseState(model.NodeIdentity{
				Role:        model.RoleWriter,
				Hostname:    "writer-0",
				ClusterName: "logexplorer",
			})

			err := runLeaseRenew([]string{"-state", statePath})
			Expect(err).To(HaveOccurred())
			Expect(fail.IsFatal(err)).To(BeTrue(), "a node with no lease cannot renew — retrying masks a state bug")
			Expect(err.Error()).To(ContainSubstring("holds no lease"))
		})

		It("is fatal when the cloud has no lease capability", func() {
			statePath := writeLeaseState(model.NodeIdentity{
				Role:       model.RoleWriter,
				Hostname:   "writer-1",
				LeaseTable: "node-claims",
			})
			origBuild := buildProvider
			buildProvider = func(string, string) (chinitport.Provider, error) {
				return &bareProvider{providerName: "azure"}, nil
			}
			DeferCleanup(func() { buildProvider = origBuild })

			err := runLeaseRenew([]string{"-state", statePath})
			Expect(err).To(HaveOccurred())
			Expect(fail.IsFatal(err)).To(BeTrue())
			Expect(err.Error()).To(ContainSubstring("no lease capability"))
		})
	})
})
