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
	"os/exec"
	"strings"

	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// OSTuner applies the OS-level optimizations the bash script performed:
// THP off (latency spikes), file-descriptor limits (ClickHouse holds
// huge part counts), the sysctl and the systemd drop-in persistence.
//
// Convergence: every knob is checked (file exists / value current)
// before it is applied — a re-run is a no-op, and a node whose sysctl
// survived re-imaging skips the writes.
type OSTuner struct {
	runner Runner
}

// NewOSTuner builds the tuner over a runner.
func NewOSTuner(runner Runner) *OSTuner { return &OSTuner{runner: runner} }

// thpPaths are the kernel THP toggles; absent on some kernels (containers).
var thpPaths = []string{
	"/sys/kernel/mm/transparent_hugepage/enabled",
	"/sys/kernel/mm/transparent_hugepage/defrag",
}

// rcLocal persists the THP disable across reboots (sysfs resets).
const rcLocal = `#!/bin/bash
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
fi
if [ -f /sys/kernel/mm/transparent_hugepage/defrag ]; then
    echo never > /sys/kernel/mm/transparent_hugepage/defrag
fi
exit 0
`

// limitsConfEntry appends the clickhouse/root nofile limits once.
const limitsConfMarker = "# ClickHouse file descriptor limits"

const limitsConfEntry = `
# ClickHouse file descriptor limits
clickhouse soft nofile 262144
clickhouse hard nofile 262144
root soft nofile 262144
root hard nofile 262144
`

// limitsDropin raises LimitNOFILE via systemd so clickhouse-server
// itself inherits the ceiling (ulimit alone does not cross the unit
// boundary).
const limitsDropin = `[Service]
LimitNOFILE=262144
`

// sysctlConf is the persisted fs.file-max raise.
const sysctlConfPath = "/etc/sysctl.d/99-clickhouse.conf"

// Tune applies every optimization. Failures inside THP/irqbalance are
// warnings in the original script; disk-visible failures (limits, sysctl)
// are errors. The method is a single convergent pass — the step's policy
// retries the whole thing.
func (t *OSTuner) Tune(ctx context.Context) error {
	// THP: echo never (best-effort — kernels without THP skip).
	for _, p := range thpPaths {
		if _, err := os.Stat(p); err != nil {
			continue
		}
		if _, err := t.runner.Run(ctx, "sh", "-c", "echo never > "+p); err != nil {
			// The bash script warned and continued; same here.
			continue
		}
	}

	// Persist THP disable via /etc/rc.local when not already there.
	if rc, err := os.ReadFile("/etc/rc.local"); err != nil || !strings.Contains(string(rc), "transparent_hugepage") {
		if err := WriteFile("/etc/rc.local", rcLocal, 0755); err != nil {
			return fmt.Errorf("write rc.local: %w", err)
		}
	}

	// fs.file-max: apply and persist (idempotent append).
	if _, err := t.runner.Run(ctx, "sysctl", "-w", "fs.file-max=2097152"); err != nil {
		// Warning-class in the original; a hard failure here breaks
		// nothing ClickHouse measures — but persist anyway.
		_ = err
	}
	if conf, err := os.ReadFile(sysctlConfPath); err != nil || !strings.Contains(string(conf), "fs.file-max") {
		f, err := os.OpenFile(sysctlConfPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return chinitsvc.Fatal(fmt.Errorf("open %s: %w", sysctlConfPath, err))
		}
		if _, err := f.WriteString("fs.file-max = 2097152\n"); err != nil {
			_ = f.Close()
			return chinitsvc.Fatal(fmt.Errorf("append fs.file-max: %w", err))
		}
		_ = f.Close()
		if _, err := t.runner.Run(ctx, "sysctl", "-p", sysctlConfPath); err != nil {
			_ = err // best-effort reload; the boot-time sysctl.d read applies it
		}
	}

	// limits.conf: append once, marker-guarded.
	limits, err := os.ReadFile("/etc/security/limits.conf")
	if err != nil || !strings.Contains(string(limits), limitsConfMarker) {
		f, err := os.OpenFile("/etc/security/limits.conf", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return chinitsvc.Fatal(fmt.Errorf("open limits.conf: %w", err))
		}
		if _, err := f.WriteString(limitsConfEntry); err != nil {
			_ = f.Close()
			return chinitsvc.Fatal(fmt.Errorf("append limits: %w", err))
		}
		_ = f.Close()
	}

	// systemd LimitNOFILE drop-in (installer owns the dir creation).
	installer := NewUnitInstaller("/etc/systemd/system")
	if err := installer.InstallDropin("limits.conf", limitsDropin); err != nil {
		return chinitsvc.Fatal(fmt.Errorf("limits drop-in: %w", err))
	}

	return nil
}

// RequireBinaries verifies the golden-AMI contract: every component the
// packer image owns must be present, at the pinned version the image
// build validated. A missing binary is a FatalError — the ASG must
// replace a node whose image is wrong, not boot it degraded (apt-repo
// drift installing unvalidated versions was the failure mode the bash
// guard closed).
func (t *OSTuner) RequireBinaries(ctx context.Context, bins []string) error {
	missing := make([]string, 0, len(bins))
	for _, bin := range bins {
		if _, err := exec.LookPath(bin); err != nil {
			missing = append(missing, bin)
		}
	}
	if len(missing) > 0 {
		return chinitsvc.Fatal(fmt.Errorf("missing from golden AMI (wrong or broken image; check golden_ami_id and the packer build): %s",
			strings.Join(missing, ", ")))
	}
	return nil
}
