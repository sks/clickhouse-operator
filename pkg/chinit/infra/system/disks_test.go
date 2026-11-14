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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// This file pins the disk mount contracts: mount by LABEL (NVMe device
// renaming), first-boot format with the label, the metadata-Critical/
// cache-degrade split, fstab idempotence, XML validation and the
// golden-AMI binary gate.

// fakeRunner records invocations and scripts outcomes. results keys on
// the joined command line; every unmatched call succeeds (mkdir, chown,
// test -b …) unless listed in failures.
type fakeRunner struct {
	calls    []string
	results  map[string]string
	failures map[string]struct{}
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{
		results:  map[string]string{},
		failures: map[string]struct{}{},
	}
}

func (r *fakeRunner) Run(_ context.Context, cmd string, args ...string) (string, error) {
	line := cmd + " " + strings.Join(args, " ")
	r.calls = append(r.calls, line)
	if _, bad := r.failures[line]; bad {
		// grep -q/test produce NO output on failure — callers key on
		// the error, and "" keeps the fake faithful to the real tools.
		return "", fmt.Errorf("%s: exit 1", line)
	}
	if out, ok := r.results[line]; ok {
		return out, nil
	}
	return "", nil
}

// script wires a canned response for one command line.
func (r *fakeRunner) script(line, out string) *fakeRunner {
	r.results[line] = out
	return r
}

// fail marks one command line as failing.
func (r *fakeRunner) fail(line string) *fakeRunner {
	r.failures[line] = struct{}{}
	return r
}

// invoked reports whether a command line ran at least once.
func (r *fakeRunner) invoked(line string) bool {
	for _, c := range r.calls {
		if c == line {
			return true
		}
	}
	return false
}

func specFor(role model.Role) model.BootSpec {
	return model.BootSpec{
		Role: role,
		Disks: model.Disks{
			MetadataLabel: "ch-metadata",
			CacheLabel:    "ch-cache",
			KeeperLabel:   "ch-keeper",
		},
	}
}

var _ = Describe("DiskManager.MountDisk", func() {
	It("formats a blank device with the label on first boot, then mounts it", func() {
		// no label yet; blank disk: no signature; not mounted yet
		r := newFakeRunner().
			script("blkid -l -o device -t LABEL=ch-metadata", "").
			fail("blkid /dev/sdb").
			fail("mountpoint -q /var/lib/clickhouse")
		d := NewDiskManager(r)

		res, err := d.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "/dev/sdb")
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Device).To(Equal("/dev/sdb"))
		Expect(r.invoked("mkfs.ext4 -F -L ch-metadata /dev/sdb")).To(BeTrue(), "first boot must format with the label")
		Expect(r.invoked("mount /dev/sdb /var/lib/clickhouse")).To(BeTrue())
	})

	It("mounts an existing labeled filesystem by label on repeat boot (no reformat)", func() {
		// label resolved post-rename; has fs with right label; not
		// mounted yet — the NVMe-renaming survival contract.
		r := newFakeRunner().
			script("blkid -l -o device -t LABEL=ch-metadata", "/dev/nvme3n1").
			script("blkid /dev/nvme3n1", `LABEL="ch-metadata" TYPE="ext4"`).
			fail("mountpoint -q /var/lib/clickhouse")
		d := NewDiskManager(r)

		_, err := d.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "/dev/nvme3n1")
		Expect(err).NotTo(HaveOccurred())
		Expect(r.invoked("mkfs.ext4 -F -L ch-metadata /dev/nvme3n1")).To(BeFalse(), "repeat boot must NOT reformat")
		Expect(r.invoked("mount /dev/nvme3n1 /var/lib/clickhouse")).To(BeTrue())
	})

	It("is Fatal on a missing device, naming the label", func() {
		// No device, no mount — the ASG must replace the node.
		d := NewDiskManager(newFakeRunner())
		_, err := d.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "")
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue(), "a missing metadata disk is a config-level failure, not a transient one")
		Expect(err.Error()).To(ContainSubstring("ch-metadata"))
	})

	It("appends the LABEL= fstab entry exactly once across convergent runs", func() {
		fstabAppend := "sh -c echo 'LABEL=ch-metadata /var/lib/clickhouse ext4 defaults,nofail 0 2' >> /etc/fstab"

		// Absent entry: grep fails → the append runs.
		r := newFakeRunner().
			script("blkid -l -o device -t LABEL=ch-metadata", "/dev/sdb").
			script("blkid /dev/sdb", `LABEL="ch-metadata" TYPE="ext4"`).
			fail("mountpoint -q /var/lib/clickhouse").
			fail("grep -q LABEL=ch-metadata /etc/fstab")
		d := NewDiskManager(r)
		_, err := d.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "/dev/sdb")
		Expect(err).NotTo(HaveOccurred())
		Expect(r.invoked(fstabAppend)).To(BeTrue())

		// Present entry + active mount: neither the append nor the
		// mount runs again.
		r2 := newFakeRunner().
			script("blkid -l -o device -t LABEL=ch-metadata", "/dev/sdb").
			script("blkid /dev/sdb", `LABEL="ch-metadata" TYPE="ext4"`)
		d2 := NewDiskManager(r2)
		_, err = d2.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "/dev/sdb")
		Expect(err).NotTo(HaveOccurred())
		Expect(r2.invoked(fstabAppend)).To(BeFalse(), "an existing fstab entry must not be duplicated")
		Expect(r2.invoked("mount /dev/sdb /var/lib/clickhouse")).To(BeFalse(), "an active mount must not be re-mounted")
	})
})

var _ = Describe("DiskManager.MountAll", func() {
	It("degrades a cache mount failure to a metadata-disk subdir; metadata failure is Fatal", func() {
		// Only the CACHE mount fails (its mountpoint probe reports
		// unmounted and the mount errors); the metadata path succeeds.
		By("cache mount failure degrades")
		r := newFakeRunner().
			script("blkid -l -o device -t LABEL=ch-metadata", "/dev/sdb").
			script("blkid /dev/sdb", `LABEL="ch-metadata" TYPE="ext4"`).
			script("blkid -l -o device -t LABEL=ch-cache", "/dev/sdc").
			script("blkid /dev/sdc", `LABEL="ch-cache" TYPE="ext4"`).
			fail("mountpoint -q /var/lib/clickhouse/disks").
			fail("mount /dev/sdc /var/lib/clickhouse/disks")
		d := NewDiskManager(r)

		mounted, err := d.MountAll(context.Background(), specFor(model.RoleWriter), nil)
		Expect(err).NotTo(HaveOccurred(), "a cache failure must not fail the boot")
		Expect(mounted).To(HaveLen(1), "only the metadata disk is mounted: %v", mounted)
		Expect(r.invoked("mkdir -p /var/lib/clickhouse/disks")).To(BeTrue(), "cache falls back to a metadata-disk subdir")

		By("metadata mount failure is fatal")
		r2 := newFakeRunner().
			script("blkid -l -o device -t LABEL=ch-metadata", "/dev/sdb").
			script("blkid /dev/sdb", `LABEL="ch-metadata" TYPE="ext4"`).
			fail("mountpoint -q /var/lib/clickhouse").
			fail("mount /dev/sdb /var/lib/clickhouse")
		d2 := NewDiskManager(r2)

		_, err = d2.MountAll(context.Background(), specFor(model.RoleWriter), nil)
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue())
	})
})

var _ = Describe("ValidateXML", func() {
	It("accepts well-formed fragments and fails empty, missing and malformed ones", func() {
		dir := GinkgoT().TempDir()

		good := filepath.Join(dir, "good.xml")
		Expect(writeTestFile(good, `<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>`)).To(Succeed())
		Expect(ValidateXML(good)).To(Succeed())

		empty := filepath.Join(dir, "empty.xml")
		Expect(writeTestFile(empty, "   ")).To(Succeed())
		err := ValidateXML(empty)
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue())

		broken := filepath.Join(dir, "broken.xml")
		Expect(writeTestFile(broken, `<clickhouse><unclosed></clickhouse>`)).To(Succeed())
		err = ValidateXML(broken)
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue(), "a broken fragment makes clickhouse-server refuse to start")

		Expect(ValidateXML(filepath.Join(dir, "missing.xml"))).To(HaveOccurred())
	})
})

var _ = Describe("OSTuner.RequireBinaries", func() {
	It("is Fatal on a missing golden-AMI binary and names every missing component", func() {
		// LookPath finds nothing on a stripped PATH.
		GinkgoT().Setenv("PATH", "/nonexistent")
		tuner := NewOSTuner(newFakeRunner())
		err := tuner.RequireBinaries(context.Background(), []string{"clickhouse-server", "chinit-helper"})
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue())
		Expect(err.Error()).To(ContainSubstring("clickhouse-server"))
		Expect(err.Error()).To(ContainSubstring("chinit-helper"))
		Expect(err.Error()).To(ContainSubstring("golden AMI"))
	})
})

// writeTestFile is the test-local file writer.
func writeTestFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}
