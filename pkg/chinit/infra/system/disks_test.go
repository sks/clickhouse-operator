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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

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

// TestMountDisk_FirstBootFormatsWithLabel: a blank device (blkid
// unrecognized) gets mkfs.ext4 -L, then mounts by the resolved label.
func TestMountDisk_FirstBootFormatsWithLabel(t *testing.T) {
	// no label yet; blank disk: no signature; not mounted yet
	r := newFakeRunner().
		script("blkid -l -o device -t LABEL=ch-metadata", "").
		fail("blkid /dev/sdb").
		fail("mountpoint -q /var/lib/clickhouse")
	d := NewDiskManager(r)

	res, err := d.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "/dev/sdb")
	require.NoError(t, err)
	require.Equal(t, "/dev/sdb", res.Device)
	require.True(t, r.invoked("mkfs.ext4 -F -L ch-metadata /dev/sdb"), "first boot must format with the label")
	require.True(t, r.invoked("mount /dev/sdb /var/lib/clickhouse"))
}

// TestMountDisk_RepeatBootMountsByLabel: an existing labeled filesystem
// skips mkfs and mounts the label-resolved device (survives NVMe
// renaming).
func TestMountDisk_RepeatBootMountsByLabel(t *testing.T) {
	// label resolved post-rename; has fs with right label; not mounted yet
	r := newFakeRunner().
		script("blkid -l -o device -t LABEL=ch-metadata", "/dev/nvme3n1").
		script("blkid /dev/nvme3n1", `LABEL="ch-metadata" TYPE="ext4"`).
		fail("mountpoint -q /var/lib/clickhouse")
	d := NewDiskManager(r)

	_, err := d.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "/dev/nvme3n1")
	require.NoError(t, err)
	require.False(t, r.invoked("mkfs.ext4 -F -L ch-metadata /dev/nvme3n1"), "repeat boot must NOT reformat")
	require.True(t, r.invoked("mount /dev/nvme3n1 /var/lib/clickhouse"))
}

// TestMountDisk_MissingDeviceIsFatal: no device, no mount — Fatal (the
// ASG must replace the node, and the message says which label).
func TestMountDisk_MissingDeviceIsFatal(t *testing.T) {
	d := NewDiskManager(newFakeRunner())
	_, err := d.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "")
	require.Error(t, err)
	require.True(t, fail.IsFatal(err), "a missing metadata disk is a config-level failure, not a transient one")
	require.Contains(t, err.Error(), "ch-metadata")
}

// TestMountAll_MetadataCritical_CacheDegrades: cache mount failure
// falls back to the directory on the metadata disk; metadata failure
// is fatal.
func TestMountAll_MetadataCritical_CacheDegrades(t *testing.T) {
	t.Run("cache mount failure degrades", func(t *testing.T) {
		// Only the CACHE mount fails (its mountpoint probe reports
		// unmounted and the mount errors); the metadata path succeeds.
		r := newFakeRunner().
			script("blkid -l -o device -t LABEL=ch-metadata", "/dev/sdb").
			script("blkid /dev/sdb", `LABEL="ch-metadata" TYPE="ext4"`).
			script("blkid -l -o device -t LABEL=ch-cache", "/dev/sdc").
			script("blkid /dev/sdc", `LABEL="ch-cache" TYPE="ext4"`).
			fail("mountpoint -q /var/lib/clickhouse/disks").
			fail("mount /dev/sdc /var/lib/clickhouse/disks")
		d := NewDiskManager(r)

		mounted, err := d.MountAll(context.Background(), specFor(model.RoleWriter), nil)
		require.NoError(t, err, "a cache failure must not fail the boot")
		require.Len(t, mounted, 1, "only the metadata disk is mounted: %v", mounted)
		require.True(t, r.invoked("mkdir -p /var/lib/clickhouse/disks"), "cache falls back to a metadata-disk subdir")
	})
	t.Run("metadata mount failure is fatal", func(t *testing.T) {
		r := newFakeRunner().
			script("blkid -l -o device -t LABEL=ch-metadata", "/dev/sdb").
			script("blkid /dev/sdb", `LABEL="ch-metadata" TYPE="ext4"`).
			fail("mountpoint -q /var/lib/clickhouse").
			fail("mount /dev/sdb /var/lib/clickhouse")
		d := NewDiskManager(r)

		_, err := d.MountAll(context.Background(), specFor(model.RoleWriter), nil)
		require.Error(t, err)
		require.True(t, fail.IsFatal(err))
	})
}

// TestMountDisk_FstabIdempotent: the LABEL= fstab entry is appended
// exactly once across convergent runs (absent → append; present → skip).
func TestMountDisk_FstabIdempotent(t *testing.T) {
	// Absent entry: grep fails → the append runs.
	r := newFakeRunner().
		script("blkid -l -o device -t LABEL=ch-metadata", "/dev/sdb").
		script("blkid /dev/sdb", `LABEL="ch-metadata" TYPE="ext4"`).
		fail("mountpoint -q /var/lib/clickhouse").
		fail("grep -q LABEL=ch-metadata /etc/fstab")
	d := NewDiskManager(r)
	_, err := d.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "/dev/sdb")
	require.NoError(t, err)
	require.True(t, r.invoked("sh -c echo 'LABEL=ch-metadata /var/lib/clickhouse ext4 defaults,nofail 0 2' >> /etc/fstab"))

	// Present entry + active mount: neither the append nor the mount
	// runs again.
	r2 := newFakeRunner().
		script("blkid -l -o device -t LABEL=ch-metadata", "/dev/sdb").
		script("blkid /dev/sdb", `LABEL="ch-metadata" TYPE="ext4"`)
	d2 := NewDiskManager(r2)
	_, err = d2.MountDisk(context.Background(), "ch-metadata", "/var/lib/clickhouse", "/dev/sdb")
	require.NoError(t, err)
	require.False(t, r2.invoked("sh -c echo 'LABEL=ch-metadata /var/lib/clickhouse ext4 defaults,nofail 0 2' >> /etc/fstab"),
		"an existing fstab entry must not be duplicated")
	require.False(t, r2.invoked("mount /dev/sdb /var/lib/clickhouse"), "an active mount must not be re-mounted")
}

// TestValidateXML: well-formed fragments pass; empty, missing and
// malformed files fail (the malformed case must be Fatal — a broken
// fragment makes clickhouse-server refuse to start).
func TestValidateXML(t *testing.T) {
	dir := t.TempDir()

	good := dir + "/good.xml"
	require.NoError(t, writeFile(good, `<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>`))
	require.NoError(t, ValidateXML(good))

	empty := dir + "/empty.xml"
	require.NoError(t, writeFile(empty, "   "))
	err := ValidateXML(empty)
	require.Error(t, err)
	require.True(t, fail.IsFatal(err))

	broken := dir + "/broken.xml"
	require.NoError(t, writeFile(broken, `<clickhouse><unclosed></clickhouse>`))
	err = ValidateXML(broken)
	require.Error(t, err)
	require.True(t, fail.IsFatal(err))

	require.Error(t, ValidateXML(dir+"/missing.xml"))
}

// TestRequireBinaries_FatalOnMissing: a missing golden-AMI binary is
// Fatal and names every missing component.
func TestRequireBinaries_FatalOnMissing(t *testing.T) {
	// LookPath finds nothing on a stripped PATH.
	t.Setenv("PATH", "/nonexistent")
	tuner := NewOSTuner(newFakeRunner())
	err := tuner.RequireBinaries(context.Background(), []string{"clickhouse-server", "chctl"})
	require.Error(t, err)
	require.True(t, fail.IsFatal(err))
	require.Contains(t, err.Error(), "clickhouse-server")
	require.Contains(t, err.Error(), "chctl")
	require.Contains(t, err.Error(), "golden AMI")
}

// writeFile is the test-local file writer.
func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0644)
}
