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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/system"
)

// This suite pins the recurring-unit matrix for writer ASG (A5): which
// units install for which role/identity-mode combination, and the
// descriptions the timers log. chinitUnits is the pure assembler —
// installUnits only writes what it returns, so the matrix specs need
// no filesystem.
//
// The contracts:
//   - lease-renew is IDENTITY-mode gated, never role gated: lease
//     writers and lease readers both install it (a role gate would
//     let a leased writer's 15-min TTL expire and another instance
//     claim its ordinal mid-flight).
//   - the maintenance units install on EVERY writer: with ASG writers
//     the ordinal-0 designation is a lease, not a machine, so the
//     unit set must survive ordinal handoffs. The subcommands no-op
//     on non-ordinal-0 holders (the app package's specs pin that
//     gate); readers never install them.

// unitNames renders the assembler's answer as a name list.
func unitNames(units []system.UnitFile) []string {
	out := make([]string, 0, len(units))
	for _, u := range units {
		out = append(out, u.Name)
	}
	return out
}

var _ = Describe("chinitUnits", func() {
	// baseSpec is the leased-writer shape; the matrix mutates role and
	// identity mode.
	baseSpec := func() model.BootSpec {
		spec := ceilingSpec()
		spec.IdentityMode = model.IdentityLease
		return spec
	}

	Context("writer, lease identity", func() {
		It("installs lease-renew and the maintenance units alongside the shared server units", func() {
			b := &Boot{Spec: &model.BootSpec{}}
			*b.Spec = baseSpec()
			Expect(b.Spec.Role).To(Equal(model.RoleWriter))

			names := unitNames(b.chinitUnits())
			Expect(names).To(ContainElements(
				"lease-renew.service",
				"lease-renew.timer",
				"dead-replica-gc.service",
				"dead-replica-gc.timer",
				"orphaned-data-gc.service",
				"orphaned-data-gc.timer",
				"schema-sync.service",
				"replica-sync.service",
			))
		})

		It("describes the maintenance jobs as ordinal-0-gated", func() {
			// The unit description is what `systemctl list-units` and
			// the boot log print — the operator's first clue why a
			// non-0 writer's timer ran without dropping anything.
			b := &Boot{Spec: &model.BootSpec{}}
			*b.Spec = baseSpec()
			for _, u := range b.chinitUnits() {
				switch u.Name {
				case "dead-replica-gc.service", "orphaned-data-gc.service":
					Expect(u.Content).To(ContainSubstring("ordinal-0 writer only"))
				}
			}
		})
	})

	Context("writer, static identity (the legacy writer path)", func() {
		It("installs the maintenance units but no lease-renew", func() {
			// Static writers keep the writer-0 machine contract (no
			// lease to renew) — the maintenance units still install
			// and the hostname gate still holds (writer-0 is the
			// machine's name in this mode).
			spec := baseSpec()
			spec.IdentityMode = model.IdentityStatic
			spec.WriterMax = 0 // static writers carry no ceiling
			b := &Boot{Spec: &spec}

			names := unitNames(b.chinitUnits())
			Expect(names).To(ContainElements("dead-replica-gc.service", "orphaned-data-gc.service"))
			Expect(names).NotTo(ContainElement("lease-renew.service"))
			Expect(names).NotTo(ContainElement("lease-renew.timer"))
		})
	})

	Context("reader, lease identity", func() {
		It("installs lease-renew but never the maintenance units", func() {
			spec := baseSpec()
			spec.Role = model.RoleReader
			spec.WriterMax = 0
			spec.ReaderMax = 10
			spec.ReaderCount = 3
			b := &Boot{Spec: &spec}

			names := unitNames(b.chinitUnits())
			Expect(names).To(ContainElements("lease-renew.service", "lease-renew.timer"))
			Expect(names).NotTo(ContainElement("dead-replica-gc.service"))
			Expect(names).NotTo(ContainElement("orphaned-data-gc.service"))
		})

		It("describes lease-renew as node-scoped, not reader-scoped", func() {
			// The writer-ASG rollout made the lease a both-roles
			// mechanism; the description must not claim reader-only
			// (the log would tell the operator the wrong story when a
			// writer's timer fires).
			spec := baseSpec()
			spec.Role = model.RoleReader
			spec.WriterMax = 0
			spec.ReaderMax = 10
			b := &Boot{Spec: &spec}
			for _, u := range b.chinitUnits() {
				if u.Name == "lease-renew.service" {
					Expect(u.Content).To(ContainSubstring("Description=Node ordinal lease renewal"))
				}
				if u.Name == "lease-renew.timer" {
					Expect(u.Content).To(ContainSubstring("Renew the ordinal lease every 5 minutes"))
					Expect(u.Content).NotTo(ContainSubstring("reader ordinal lease"))
				}
			}
		})
	})

	Context("keeper", func() {
		It("installs only the keeper-health probe", func() {
			spec := baseSpec()
			spec.Role = model.RoleKeeper
			spec.IdentityMode = model.IdentityStatic
			b := &Boot{Spec: &spec}

			names := unitNames(b.chinitUnits())
			Expect(names).To(Equal([]string{"keeper-health.service"}))
		})
	})

	Context("backup replica", func() {
		It("installs nothing", func() {
			spec := baseSpec()
			spec.Role = model.RoleBackup
			spec.IdentityMode = model.IdentityStatic
			b := &Boot{Spec: &spec}

			Expect(unitNames(b.chinitUnits())).To(BeEmpty())
		})
	})
})
