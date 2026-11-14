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

package model_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"gopkg.in/yaml.v3"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// baselineSpec is the valid shape every table entry mutates one field
// at a time. This table is the spec-validation gate's single home: the
// service package's specs consume the validated spec (settleIdentity,
// renders), so field coverage folds here rather than duplicating the
// same Validate() call across suites.
func baselineSpec() model.BootSpec {
	return model.BootSpec{
		Role:         model.RoleWriter,
		IdentityMode: model.IdentityStatic,
		ClusterName:  "logexplorer",
		ConfigBundle: model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap"},
		Disks:        model.Disks{MetadataLabel: "ch-metadata"},
		Secrets:      model.Secrets{DefaultPassword: "default-pw", AdminPassword: "admin-pw"},
	}
}

var _ = Describe("BootSpec.Validate", func() {
	// The writer-ASG contract lives here: lease identity requires the
	// role's ordinal claim ceiling (the ASG max), and a reader at
	// scale-out past its baseline is valid because ReaderMax — not
	// ReaderCount — is the ceiling.
	DescribeTable("accepts and rejects spec shapes",
		func(mutate func(*model.BootSpec), wantErrField string) {
			spec := baselineSpec()
			if mutate != nil {
				mutate(&spec)
			}
			err := spec.Validate()
			if wantErrField == "" {
				Expect(err).NotTo(HaveOccurred())
				return
			}
			Expect(err).To(HaveOccurred())
			var specErr *model.SpecError
			Expect(err).To(BeAssignableToTypeOf(specErr))
			specErr, ok := err.(*model.SpecError)
			Expect(ok).To(BeTrue())
			Expect(specErr.Field).To(Equal(wantErrField))
			Expect(err.Error()).To(ContainSubstring("chinit.yaml: " + wantErrField))
		},
		Entry("baseline static spec is valid", nil, ""),
		Entry("valid writer lease spec (writer_max set)",
			func(s *model.BootSpec) {
				s.IdentityMode = model.IdentityLease
				s.WriterMax = 4
			}, ""),
		Entry("writer lease spec without writer_max is invalid",
			func(s *model.BootSpec) {
				s.IdentityMode = model.IdentityLease
				s.WriterMax = 0
			}, "writer_max"),
		Entry("reader lease spec without reader_max is invalid",
			func(s *model.BootSpec) {
				s.Role = model.RoleReader
				s.IdentityMode = model.IdentityLease
				s.ReaderMax = 0
			}, "reader_max"),
		// Cross-role isolation: the OTHER role's ceiling being present
		// must never satisfy this role's lease gate — a writer spec
		// that carries reader_max (the Terraform render writes both
		// keys into every role's spec) still needs writer_max, and
		// the mirror holds for readers.
		Entry("writer lease spec with reader_max set but writer_max absent is invalid",
			func(s *model.BootSpec) {
				s.IdentityMode = model.IdentityLease
				s.WriterMax = 0
				s.ReaderMax = 10
			}, "writer_max"),
		Entry("reader lease spec with writer_max set but reader_max absent is invalid",
			func(s *model.BootSpec) {
				s.Role = model.RoleReader
				s.IdentityMode = model.IdentityLease
				s.ReaderMax = 0
				s.WriterMax = 4
			}, "reader_max"),
		Entry("reader lease spec with reader_max but zero reader_count is valid (max is the ceiling)",
			func(s *model.BootSpec) {
				s.Role = model.RoleReader
				s.IdentityMode = model.IdentityLease
				s.ReaderMax = 10
				s.ReaderCount = 0
			}, ""),
		// An empty hostname prefix is ALLOWED at validation: prefixes
		// default at claim/inventory time (WriterPrefix()/ReaderPrefix),
		// so the Terraform template can omit the key entirely.
		Entry("writer lease spec with an empty writer_hostname_prefix is valid",
			func(s *model.BootSpec) {
				s.IdentityMode = model.IdentityLease
				s.WriterHostPrefix = ""
				s.WriterMax = 4
			}, ""),
		Entry("static writer spec stays valid (interim path)",
			func(s *model.BootSpec) {
				s.IdentityMode = model.IdentityStatic
				s.WriterCount = 2
				s.WriterNodes = []string{"writer-0", "writer-1"}
			}, ""),
		Entry("reader lease with negative reader_max is invalid",
			func(s *model.BootSpec) {
				s.Role = model.RoleReader
				s.IdentityMode = model.IdentityLease
				s.ReaderMax = -1
			}, "reader_max"),
		Entry("writer lease with negative writer_max is invalid",
			func(s *model.BootSpec) {
				s.IdentityMode = model.IdentityLease
				s.WriterMax = -1
			}, "writer_max"),
		Entry("unknown role is invalid", func(s *model.BootSpec) { s.Role = "coordinator" }, "role"),
		Entry("missing role is invalid", func(s *model.BootSpec) { s.Role = "" }, "role"),
		Entry("missing identity mode is invalid", func(s *model.BootSpec) { s.IdentityMode = "" }, "identity_mode"),
		Entry("unknown identity mode is invalid", func(s *model.BootSpec) { s.IdentityMode = "dns" }, "identity_mode"),
		Entry("empty config bucket is invalid", func(s *model.BootSpec) { s.ConfigBundle.Bucket = "" }, "config_bundle.bucket"),
		Entry("empty metadata disk label is invalid", func(s *model.BootSpec) { s.Disks.MetadataLabel = "" }, "disks.metadata_label"),
		Entry("empty cluster name is invalid", func(s *model.BootSpec) { s.ClusterName = "" }, "cluster_name"),
		Entry("empty default-password secret is invalid", func(s *model.BootSpec) { s.Secrets.DefaultPassword = "" }, "secrets.default_password"),
	)
})

var _ = Describe("BootSpec prefix and ceiling defaults", func() {
	// Where the empty-prefix allowance materializes: the accessors
	// default the claim/inventory prefixes, and the ceilings fall back
	// to the legacy counts for static specs. settleIdentity's claim
	// specs (the service suite) pin the prefix default at claim time;
	// these specs pin the accessor contract itself.
	It("defaults the writer and reader prefixes when the spec omits them", func() {
		spec := model.BootSpec{}
		Expect(spec.WriterPrefix()).To(Equal(model.DefaultWriterHostPrefix))
		Expect(spec.ReaderPrefix()).To(Equal(model.DefaultReaderHostPrefix))

		spec.WriterHostPrefix = "ingest-"
		spec.ReaderHostPrefix = "query-"
		Expect(spec.WriterPrefix()).To(Equal("ingest-"))
		Expect(spec.ReaderPrefix()).To(Equal("query-"))
	})

	It("keys the writer ceiling on writer_max, falling back to the legacy count", func() {
		spec := model.BootSpec{WriterMax: 4, WriterCount: 2}
		Expect(spec.WriterCeiling()).To(Equal(4), "the ASG max reserves the CR slots")

		spec.WriterMax = 0
		Expect(spec.WriterCeiling()).To(Equal(2), "static specs keep the baseline count")
	})

	It("keys the reader ceiling on reader_max, falling back to the baseline count", func() {
		spec := model.BootSpec{ReaderMax: 10, ReaderCount: 3}
		Expect(spec.ReaderCeiling()).To(Equal(10), "a scale-out past the baseline must still be listable")

		spec.ReaderMax = 0
		Expect(spec.ReaderCeiling()).To(Equal(3))
	})
})

var _ = Describe("BootSpec YAML round-trip", func() {
	It("preserves the writer hostname prefix and role ceilings", func() {
		spec := baselineSpec()
		spec.IdentityMode = model.IdentityLease
		spec.WriterHostPrefix = "writer-"
		spec.WriterMax = 4
		spec.ReaderMax = 10

		data, err := yaml.Marshal(spec)
		Expect(err).NotTo(HaveOccurred())
		var loaded model.BootSpec
		Expect(yaml.Unmarshal(data, &loaded)).To(Succeed())

		Expect(loaded.WriterHostPrefix).To(Equal("writer-"))
		Expect(loaded.WriterMax).To(Equal(4))
		Expect(loaded.ReaderMax).To(Equal(10))
		Expect(loaded.Validate()).To(Succeed())
	})

	It("parses the documented yaml key names from a rendered chinit.yaml", func() {
		// The Terraform template writes these keys verbatim; a tag
		// drift here breaks every writer boot after the ASG rollout.
		const doc = `
role: writer
identity_mode: lease
writer_hostname_prefix: writer-
writer_max: 4
reader_max: 10
cluster_name: logexplorer
dns_zone_name: ch.internal
config_bundle:
  bucket: data-bucket
  prefix: bootstrap
disks:
  metadata_label: ch-metadata
secrets:
  default_password: default-pw
  admin_password: admin-pw
`
		var spec model.BootSpec
		Expect(yaml.Unmarshal([]byte(doc), &spec)).To(Succeed())
		Expect(spec.WriterHostPrefix).To(Equal("writer-"))
		Expect(spec.WriterMax).To(Equal(4))
		Expect(spec.ReaderMax).To(Equal(10))
		Expect(spec.Validate()).To(Succeed())
	})
})
