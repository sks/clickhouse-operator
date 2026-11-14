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
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// This file pins the CLI surface: subcommand dispatch and the boot
// flag contract. The subcommands' own behavior lives in their spec
// files (drain_test.go, maintenance_test.go, spec_inventory_test.go,
// lease_renew_test.go).

var _ = Describe("Run (subcommand dispatch)", func() {
	It("errors with usage when no command is given", func() {
		Expect(Run(nil)).To(HaveOccurred())
	})

	It("errors on an unknown command, naming it", func() {
		err := Run([]string{"teleport"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("teleport"))
	})

	It("prints the version", func() {
		// The stamp bootstrap scripts compare against the release tag.
		Expect(Run([]string{"version"})).To(Succeed())
	})
})

var _ = Describe("parseBootFlags", func() {
	It("defaults the spec path to the bootstrap bundle location and detects the cloud", func() {
		flags, err := parseBootFlags([]string{})
		Expect(err).NotTo(HaveOccurred())
		Expect(flags.specPath).To(Equal("/var/lib/clickhouse-bootstrap/chinit.yaml"))
		Expect(flags.cloud).To(BeEmpty())
		Expect(flags.dryRun).To(BeFalse())
	})

	It("rejects stray positionals as usage errors, not silently dropped args", func() {
		_, err := parseBootFlags([]string{"unexpected"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unexpected"))
	})
})

var _ = Describe("boot dry-run", func() {
	// writeSpec seeds a chinit.yaml the table below mutates.
	writeSpec := func(content string) string {
		path := filepath.Join(GinkgoT().TempDir(), "chinit.yaml")
		Expect(os.WriteFile(path, []byte(content), 0644)).To(Succeed())
		return path
	}

	const validSpec = `role: writer
identity_mode: static
cluster_name: logexplorer
config_bundle:
  bucket: data-bucket
  prefix: bootstrap
disks:
  metadata_label: ch-metadata
secrets:
  default_password: default-secret-name
  admin_password: admin-secret-name
`

	It("errors cleanly when the spec file is missing", func() {
		err := Run([]string{"boot", "-spec", "/nonexistent/chinit.yaml"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("/nonexistent/chinit.yaml"))
	})

	It("validates the spec and prints the step plan without touching the system", func() {
		spec := writeSpec(validSpec)
		// -cloud pins the provider (a dev machine has no DMI/IMDS —
		// detection is exercised in the detect package's tests).
		dir := GinkgoT().TempDir()
		Expect(Run([]string{"boot", "-spec", spec, "-cloud", "aws", "-dry-run", "-state", filepath.Join(dir, "state.json")})).To(Succeed())
	})

	It("names the invalid field when the spec is malformed", func() {
		// role present, identity_mode missing → SpecError naming it.
		spec := writeSpec(`role: writer
cluster_name: logexplorer
config_bundle:
  bucket: data-bucket
disks:
  metadata_label: ch-metadata
secrets:
  default_password: x
`)
		err := Run([]string{"boot", "-spec", spec, "-dry-run"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("identity_mode"))
	})
})
