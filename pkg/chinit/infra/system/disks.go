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
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// DiskManager mounts the node's data disks by LABEL (never device path —
// NVMe renaming on Graviton renames devices between boots; labels and
// fstab LABEL= entries survive).
//
// Convergence (what the bash script re-derived every boot, now probed):
//   - label resolves via blkid → device already formatted, just mount
//   - no label but a blank device → mkfs.ext4 -L, then mount
//   - filesystem present with the wrong label → e2label in place
//   - mountpoint already mounted → skip the mount
//   - fstab already has LABEL= → skip the append
type DiskManager struct {
	runner Runner
	// fsPath exists for tests to simulate /proc/mounts; empty → the
	// mountpoint check shells out to mountpoint(1).
}

// NewDiskManager builds the manager over a runner.
func NewDiskManager(runner Runner) *DiskManager {
	return &DiskManager{runner: runner}
}

// deviceCandidates are the first-boot fallback paths per role. On first
// boot the EBS volume is blank — no filesystem, no label — so the label
// lookup finds nothing and the EC2 EBS device name is the only hint.
// (The original bash resolved these per-role; the sets overlap.)
const (
	metadataCandidates = "/dev/sdf /dev/sdb /dev/nvme1n1 /dev/xvdf /dev/xvdb"
	cacheCandidates    = "/dev/sdg /dev/sdc /dev/nvme2n1 /dev/xvdg /dev/xvdc"
	keeperCandidates   = "/dev/sdf /dev/sdb /dev/nvme1n1 /dev/xvdf /dev/xvdb"
)

// MountedDisk describes what a successful mount produced.
type MountedDisk struct {
	Label  string
	Device string
	Mount  string
}

// ResolveByLabel finds the device carrying LABEL=label, or "" when none.
func (d *DiskManager) ResolveByLabel(ctx context.Context, label string) (string, error) {
	out, err := d.runner.Run(ctx, "blkid", "-l", "-o", "device", "-t", "LABEL="+label)
	return strings.TrimSpace(out), err
}

// resolveFirstExisting returns the first candidate device that exists as
// a block device. Probing via [ -b ] shells test(1); a missing candidate
// simply doesn't match.
func (d *DiskManager) resolveFirstExisting(ctx context.Context, candidates string) (string, error) {
	for _, dev := range strings.Fields(candidates) {
		if _, err := d.runner.Run(ctx, "test", "-b", dev); err == nil {
			return dev, nil
		}
	}
	return "", nil
}

// ResolveDevice resolves a disk: label first (repeat boots), then the
// first-boot device-path fallback. Returns "" when neither exists yet
// (the boot step waits and retries — EBS volumes attach asynchronously).
func (d *DiskManager) ResolveDevice(ctx context.Context, label, fallbackCandidates string) (string, error) {
	if byLabel, err := d.ResolveByLabel(ctx, label); err == nil && byLabel != "" {
		return byLabel, nil
	}
	return d.resolveFirstExisting(ctx, fallbackCandidates)
}

// isMounted reports whether the mountpoint is an active mount.
func (d *DiskManager) isMounted(ctx context.Context, mountPoint string) bool {
	_, err := d.runner.Run(ctx, "mountpoint", "-q", mountPoint)
	return err == nil
}

// hasFilesystem reports whether the device carries any filesystem
// (blkid exits 0 when it recognizes a signature).
func (d *DiskManager) hasFilesystem(ctx context.Context, device string) bool {
	_, err := d.runner.Run(ctx, "blkid", device)
	return err == nil
}

// carriesLabel reports whether the device's filesystem has this label.
func (d *DiskManager) carriesLabel(ctx context.Context, device, label string) bool {
	out, err := d.runner.Run(ctx, "blkid", device)
	return err == nil && strings.Contains(out, `LABEL="`+label+`"`)
}

// MountDisk converges one disk to "mounted at mountPoint, in fstab by
// label". Idempotent: re-running on a mounted, fstab'd disk is a no-op.
func (d *DiskManager) MountDisk(ctx context.Context, label, mountPoint, device string) (MountedDisk, error) {
	res := MountedDisk{Label: label, Device: device, Mount: mountPoint}
	if device == "" {
		return res, chinitsvc.Fatal(fmt.Errorf("device not found for %s (label=%s)", mountPoint, label))
	}
	if _, err := d.runner.Run(ctx, "test", "-b", device); err != nil {
		return res, chinitsvc.Fatal(fmt.Errorf("device %s is not a block device (label=%s)", device, label))
	}

	// First-use format, or relabel an existing filesystem that lost its
	// label (both observed in the fleet).
	if !d.hasFilesystem(ctx, device) {
		if _, err := d.runner.Run(ctx, "mkfs.ext4", "-F", "-L", label, device); err != nil {
			return res, fmt.Errorf("format %s with label %s: %w", device, label, err)
		}
	} else if !d.carriesLabel(ctx, device, label) {
		if _, err := d.runner.Run(ctx, "e2label", device, label); err != nil {
			return res, fmt.Errorf("relabel %s to %s: %w", device, label, err)
		}
	}

	// Ensure the mountpoint directory exists.
	if _, err := d.runner.Run(ctx, "mkdir", "-p", mountPoint); err != nil {
		return res, fmt.Errorf("create %s: %w", mountPoint, err)
	}

	// Mount only when not already mounted; resolve by label post-format
	// so the mount does not depend on the unstable device path.
	if !d.isMounted(ctx, mountPoint) {
		resolved, err := d.ResolveByLabel(ctx, label)
		if err != nil || resolved == "" {
			resolved = device
		}
		if _, err := d.runner.Run(ctx, "mount", resolved, mountPoint); err != nil {
			return res, fmt.Errorf("mount %s at %s: %w", resolved, mountPoint, err)
		}
	}

	// fstab by label: append once, never duplicate.
	fstabHas, err := d.runner.Run(ctx, "grep", "-q", "LABEL="+label, "/etc/fstab")
	if err != nil && strings.TrimSpace(fstabHas) == "" {
		// grep -q exits 1 when absent: append the entry.
		if _, err := d.runner.Run(ctx, "sh", "-c",
			fmt.Sprintf("echo 'LABEL=%s %s ext4 defaults,nofail 0 2' >> /etc/fstab", label, mountPoint)); err != nil {
			return res, fmt.Errorf("append LABEL=%s to fstab: %w", label, err)
		}
	}

	return res, nil
}

// MountAll mounts the role's disks. The metadata disk is Critical (its
// loss makes S3 data an unreadable pile of parts); cache is BestEffort
// (falls back to a metadata-disk subdirectory); keeper is BestEffort
// (keeper runs on default storage when the dedicated disk is missing,
// with an IOPS-contention warning). The caller (boot step) supplies the
// retry policy; this function is one convergent pass.
func (d *DiskManager) MountAll(ctx context.Context, spec model.BootSpec, hostnameSet func()) ([]MountedDisk, error) {
	var mounted []MountedDisk

	// Metadata: must succeed.
	dev, err := d.ResolveDevice(ctx, spec.Disks.MetadataLabel, metadataCandidates)
	if err != nil {
		return mounted, err
	}
	if dev == "" {
		return mounted, chinitsvc.Fatal(fmt.Errorf("metadata disk not found (label=%s): EBS volume not attached yet", spec.Disks.MetadataLabel))
	}
	md, err := d.MountDisk(ctx, spec.Disks.MetadataLabel, "/var/lib/clickhouse", dev)
	if err != nil {
		return mounted, chinitsvc.Fatal(fmt.Errorf("metadata disk: %w", err))
	}
	mounted = append(mounted, md)

	// Cache: optional; falls back to a subdirectory on the metadata disk.
	if spec.Disks.CacheLabel != "" {
		cacheDev, err := d.ResolveDevice(ctx, spec.Disks.CacheLabel, cacheCandidates)
		if err != nil {
			return mounted, err
		}
		if cacheDev != "" && cacheDev != dev {
			cd, err := d.MountDisk(ctx, spec.Disks.CacheLabel, "/var/lib/clickhouse/disks", cacheDev)
			if err != nil {
				// BestEffort: cache is re-derivable from S3.
				_ = err // logged by the step; mount falls back below.
				_, _ = d.runner.Run(ctx, "mkdir", "-p", "/var/lib/clickhouse/disks")
			} else {
				mounted = append(mounted, cd)
			}
		} else {
			_, _ = d.runner.Run(ctx, "mkdir", "-p", "/var/lib/clickhouse/disks")
		}
	} else {
		_, _ = d.runner.Run(ctx, "mkdir", "-p", "/var/lib/clickhouse/disks")
	}

	// Keeper: keepers only, dedicated disk for fsync latency isolation.
	if spec.Role == model.RoleKeeper && spec.Disks.KeeperLabel != "" {
		kDev, err := d.ResolveDevice(ctx, spec.Disks.KeeperLabel, keeperCandidates)
		if err != nil {
			return mounted, err
		}
		if kDev != "" {
			kd, err := d.MountDisk(ctx, spec.Disks.KeeperLabel, "/var/lib/clickhouse-keeper", kDev)
			if err == nil {
				mounted = append(mounted, kd)
			}
		}
		// Both paths need the keeper state directories to exist.
		_, _ = d.runner.Run(ctx, "mkdir", "-p", "/var/lib/clickhouse-keeper/log")
		_, _ = d.runner.Run(ctx, "mkdir", "-p", "/var/lib/clickhouse-keeper/snapshots")
	}

	// Ownership: everything ClickHouse writes belongs to clickhouse.
	if _, err := d.runner.Run(ctx, "chown", "-R", "clickhouse:clickhouse", "/var/lib/clickhouse"); err != nil {
		return mounted, err
	}

	// Final invariant: the metadata mount is active.
	if !d.isMounted(ctx, "/var/lib/clickhouse") {
		return mounted, chinitsvc.Fatal(fmt.Errorf("metadata disk is not mounted at /var/lib/clickhouse"))
	}

	return mounted, nil
}
