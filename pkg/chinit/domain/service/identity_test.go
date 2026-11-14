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
	"encoding/json"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// This file pins the identity-persistence contract: the persister
// merges the settled identity into the state file at claim time (a
// later critical failure must not lose it — the job subcommands read
// it there), and the run's per-step saves must never clobber it.

var _ = Describe("Engine identity persistence", func() {
	It("saves the settled identity immediately, with the cloud and spec hash", func() {
		store, err := NewFileStateStore(GinkgoT().TempDir() + "/state.json")
		Expect(err).NotTo(HaveOccurred())
		engine := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1")

		persist := engine.IdentityPersister("aws")
		Expect(persist).NotTo(BeNil(), "a store-backed engine must expose the persister")

		// Identity settles mid-run (the settle step's contract).
		persist(model.NodeIdentity{
			Role:        model.RoleReader,
			Hostname:    "reader-3",
			InstanceID:  "i-abc",
			PrivateIP:   "10.0.1.13",
			Region:      "us-east-1",
			ClusterName: "logexplorer",
			DNSZone:     "ch.internal",
			CRName:      "01-3",
			CROrdinal:   3,
			LeaseTable:  "reader-ordinals",
		})

		loaded, err := store.Load()
		Expect(err).NotTo(HaveOccurred())
		Expect(loaded.Identity).NotTo(BeNil())
		Expect(loaded.Identity.Hostname).To(Equal("reader-3"))
		Expect(loaded.Identity.CRName).To(Equal("01-3"))
		Expect(loaded.Identity.Cloud).To(Equal("aws"))
		Expect(loaded.SpecHash).To(Equal("hash-1"))
	})

	It("exposes no persister without a store (unit tests and dry runs never write identity)", func() {
		engine := NewEngine(nil, nil)
		Expect(engine.IdentityPersister("aws")).To(BeNil())
	})

	It("keeps the identity through per-step records (the shared-state clobber bug)", func() {
		// The regression: Run loaded state before settle, then the
		// per-step Save overwrote the identity the persister wrote
		// mid-run — the subcommands then read "no settled identity"
		// from a state.json whose node-env proved the settle ran.
		path := filepath.Join(GinkgoT().TempDir(), "state.json")
		store, err := NewFileStateStore(path)
		Expect(err).NotTo(HaveOccurred())
		engine := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1")
		persist := engine.IdentityPersister("aws")

		// Run's start-of-run load (empty file → fresh state), then a
		// step records and saves — twice, with the persist between.
		run := func() {
			var state *modelState
			if loaded, err := store.Load(); err == nil && loaded != nil {
				state = loaded
			}
			if state == nil {
				state = &modelState{}
			}
			engine.shared = state
			state.recordStep(stepRecord{Name: "fetch", Status: "done"})
			Expect(store.Save(state)).To(Succeed())
		}
		run()
		persist(model.NodeIdentity{Hostname: "writer-0"})
		run()

		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		var st modelState
		Expect(json.Unmarshal(data, &st)).To(Succeed())
		Expect(st.Identity).NotTo(BeNil())
		Expect(st.Identity.Hostname).To(Equal("writer-0"),
			"identity clobbered by step records: %+v", st.Identity)
	})

	It("invalidates done-step records when the bundle hash changes; the same hash resumes", func() {
		store, err := NewFileStateStore(GinkgoT().TempDir() + "/state.json")
		Expect(err).NotTo(HaveOccurred())

		step := func() Step {
			return StepFunc("install_static_configs", Critical, onceFatal, func(context.Context) error { return nil })
		}

		// First run completes one step under hash-1.
		Expect(NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1").Run(context.Background(), []Step{step()}).Fatality).NotTo(HaveOccurred())

		// Same hash: the prior record is a valid resume — skipped.
		res := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1").Run(context.Background(), []Step{step()})
		Expect(res.Steps[0].Skipped).To(BeTrue(), "same bundle hash: the done step is skipped")

		// New hash: the prior record is stale — the step re-runs.
		res = NewEngine(nil, store).WithIdentity(nil, "aws", "hash-2").Run(context.Background(), []Step{step()})
		Expect(res.Steps[0].Skipped).To(BeFalse(), "a new bundle hash must invalidate the done record")
	})
})
