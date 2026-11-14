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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This suite pins the job inventory's CR-slot arithmetic — the same
// contract service.CRName() lands and A3's live-claims discovery
// builds on: readers offset by the writer slot CEILING (the writers
// ASG max), so scaling writers never re-keys reader CR slots; writers
// enumerate the reserved ordinals when leased, the explicit node list
// when static.
var _ = Describe("hostInventory", func() {
	Context("leased writers (writer_max set)", func() {
		It("reserves writer slots 01-0..01-{max-1} and offsets readers by max", func() {
			out := hostInventory(inventorySpec{
				WriterMax:        4,
				ReaderMax:        10,
				ReaderHostPrefix: "reader-",
				DNSZoneName:      "ch.internal",
			})
			Expect(out).To(HaveLen(14))
			Expect(out).To(ContainElements(
				"01-0=writer-0.ch.internal",
				"01-3=writer-3.ch.internal",
				// Readers offset by the writer ceiling (4), not any
				// live count.
				"01-4=reader-0.ch.internal",
				"01-5=reader-1.ch.internal",
				"01-13=reader-9.ch.internal",
			))
		})

		It("keys reader slots on the ceiling, never the legacy writer count fields", func() {
			// The regression this contract kills: the legacy inventory
			// offset readers by the live writer list length, so a writer
			// scale-out re-keyed every reader CR slot. A leased spec may
			// still carry the deprecated WriterCount/WriterNodes compat
			// fields — the inventory must ignore both.
			leased := inventorySpec{
				WriterMax:        4,
				WriterCount:      2,
				WriterNodes:      []string{"writer-0", "writer-1"},
				ReaderMax:        2,
				ReaderHostPrefix: "reader-",
				DNSZoneName:      "ch.internal",
			}
			out := hostInventory(leased)
			Expect(out).To(Equal([]string{
				"01-0=writer-0.ch.internal",
				"01-1=writer-1.ch.internal",
				"01-2=writer-2.ch.internal",
				"01-3=writer-3.ch.internal",
				"01-4=reader-0.ch.internal",
				"01-5=reader-1.ch.internal",
			}))

			// The compat fields drifting (a writer scale event recorded
			// in the static fields) must not move a single reader slot.
			leased.WriterCount = 3
			leased.WriterNodes = []string{"writer-0", "writer-1", "writer-2"}
			Expect(hostInventory(leased)).To(Equal(out))
		})

		It("honors a custom writer_hostname_prefix", func() {
			out := hostInventory(inventorySpec{
				WriterMax:        2,
				WriterHostPrefix: "ingest-",
				ReaderMax:        1,
				ReaderHostPrefix: "reader-",
				DNSZoneName:      "ch.internal",
			})
			Expect(out).To(Equal([]string{
				"01-0=ingest-0.ch.internal",
				"01-1=ingest-1.ch.internal",
				"01-2=reader-0.ch.internal",
			}))
		})
	})

	Context("static writers (writer_max unset)", func() {
		It("uses the WriterNodes list and offsets readers by its length", func() {
			out := hostInventory(inventorySpec{
				WriterNodes:      []string{"writer-0", "writer-1", "writer-2"},
				ReaderCount:      2,
				ReaderHostPrefix: "reader-",
				DNSZoneName:      "ch.internal",
			})
			Expect(out).To(Equal([]string{
				"01-0=writer-0.ch.internal",
				"01-1=writer-1.ch.internal",
				"01-2=writer-2.ch.internal",
				"01-3=reader-0.ch.internal",
				"01-4=reader-1.ch.internal",
			}))
		})

		It("keeps the legacy reader_count fallback when reader_max is unset", func() {
			// Pre-writer-ASG reader specs carry reader_count only; the
			// interim inventory must still enumerate them.
			out := hostInventory(inventorySpec{
				WriterNodes:      []string{"writer-0"},
				ReaderCount:      2,
				ReaderHostPrefix: "reader-",
				DNSZoneName:      "ch.internal",
			})
			Expect(out).To(Equal([]string{
				"01-0=writer-0.ch.internal",
				"01-1=reader-0.ch.internal",
				"01-2=reader-1.ch.internal",
			}))
		})
	})

	Context("prefix defaults", func() {
		It("defaults both prefixes when the spec omits them", func() {
			out := hostInventory(inventorySpec{
				WriterMax:   1,
				ReaderMax:   1,
				DNSZoneName: "ch.internal",
			})
			Expect(out).To(Equal([]string{
				"01-0=writer-0.ch.internal",
				"01-1=reader-0.ch.internal",
			}))
		})
	})
})
