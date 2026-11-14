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

package system

import (
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("WriteFile", func() {
	It("converges: identical content skips the write, different content rewrites with the mode", func() {
		path := filepath.Join(GinkgoT().TempDir(), "nested", "unit.service")

		Expect(WriteFile(path, "first", 0644)).To(Succeed())
		data, err := os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("first"))

		// Same content: no error, content intact.
		Expect(WriteFile(path, "first", 0644)).To(Succeed())

		// Different content: rewritten.
		Expect(WriteFile(path, "second", 0600)).To(Succeed())
		data, err = os.ReadFile(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(Equal("second"))

		info, err := os.Stat(path)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Mode().Perm()).To(Equal(os.FileMode(0600)))
	})
})

var _ = Describe("UnitInstaller", func() {
	It("writes units under the injected dir with the exact ExecStart content", func() {
		// Golden-file stability for the ExecStart contract the units
		// carry: chinit itself, never embedded bash.
		dir := GinkgoT().TempDir()
		installer := NewUnitInstaller(dir)

		written, err := installer.InstallUnits([]UnitFile{
			{
				Name: "schema-sync.service",
				Content: `[Unit]
Description=ClickHouse schema auto-propagation
[Service]
Type=oneshot
ExecStart=/usr/local/bin/chinit schema-sync -retries 3
`,
			},
			{
				Name: "schema-sync.timer",
				Content: `[Timer]
OnCalendar=hourly
`,
			},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(written).To(HaveLen(2))

		data, err := os.ReadFile(filepath.Join(dir, "schema-sync.service"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(ContainSubstring("ExecStart=/usr/local/bin/chinit schema-sync -retries 3"),
			"the ExecStart must call chinit — embedded bash in units is the retired era")
	})

	It("creates the clickhouse-server drop-in directory on demand", func() {
		dir := GinkgoT().TempDir()
		installer := NewUnitInstaller(dir)
		Expect(installer.InstallDropin("limits.conf", "[Service]\nLimitNOFILE=262144\n")).To(Succeed())
		data, err := os.ReadFile(filepath.Join(dir, "clickhouse-server.service.d", "limits.conf"))
		Expect(err).NotTo(HaveOccurred())
		Expect(string(data)).To(ContainSubstring("LimitNOFILE=262144"))
	})
})
