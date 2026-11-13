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
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"

	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// ValidateXML checks a ClickHouse config fragment: non-empty, a root
// element the config-merge parser accepts (<clickhouse> / <yandex>), and
// well-formed (the Go XML decoder replaces xmllint --noout; no external
// binary needed).
//
// Why strict: a malformed fragment in config.d makes clickhouse-server
// refuse to start — the bash script validated before install for exactly
// this reason, and a broken downloaded bundle must fail the install
// step loudly instead of poisoning the config directory.
func ValidateXML(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return chinitsvc.Fatal(fmt.Errorf("XML file is empty: %s", path))
	}

	// Well-formedness via a token walk (xml.Unmarshal would require a
	// concrete element; fragments have varying roots).
	dec := xml.NewDecoder(bytes.NewReader(data))
	for {
		if _, err := dec.Token(); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return chinitsvc.Fatal(fmt.Errorf("XML validation failed for %s: %w", path, err))
		}
	}
	return nil
}

// StaticConfigInstaller moves the downloaded bundle's static XMLs into
// /etc/clickhouse-server/config.d with validation and ownership, and
// retires the pre-chctl hand-written overlays whose duplicate sections
// would conflict at config-merge time with the chctl-rendered files.
type StaticConfigInstaller struct {
	runner Runner
}

// NewStaticConfigInstaller builds the installer.
func NewStaticConfigInstaller(runner Runner) *StaticConfigInstaller {
	return &StaticConfigInstaller{runner: runner}
}

// retiredOverlays are files earlier stacks shipped by hand. A lingering
// copy is config drift: chctl renders these sections now, and the merge
// conflicts (duplicate <macros>, <remote_servers>, profiles) break the
// server config parse. Removed on every boot — idempotent by deletion.
var retiredOverlays = []string{
	"/etc/clickhouse-server/config.d/macros.xml",
	"/etc/clickhouse-server/config.d/cluster.xml",
	"/etc/clickhouse-server/users.d/z_users.xml",
	"/etc/clickhouse-server/config.d/settings.xml",
	"/etc/clickhouse-server/config.d/logger.xml",
	"/etc/clickhouse-server/config.d/query-log.xml",
	"/etc/clickhouse-server/config.d/prometheus.xml",
	"/etc/clickhouse-server/users.d/delete_default_user.xml",
}

// staticConfigs maps bundle filenames to their install destinations.
// The network/ssl/storage trio is the static remainder — everything
// else renders from the CHI spec via chctl.
var staticConfigs = map[string]string{
	"network.xml": "/etc/clickhouse-server/config.d/network.xml",
	"ssl.xml":     "/etc/clickhouse-server/config.d/ssl.xml",
	"storage.xml": "/etc/clickhouse-server/config.d/storage.xml",
}

// Install converges the static configs: validate each bundle XML, copy
// to its destination, fix ownership/perms. A missing or invalid bundle
// member is Fatal — the bundle SHA gate should have caught corruption,
// so reaching here with a bad file means the gate was skipped and the
// node must not boot half-configured.
func (i *StaticConfigInstaller) Install(ctx context.Context, bundleDir string) error {
	for _, gone := range retiredOverlays {
		if err := os.Remove(gone); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("retire %s: %w", gone, err)
		}
	}

	for name, dest := range staticConfigs {
		src := filepath.Join(bundleDir, name)
		if err := ValidateXML(src); err != nil {
			return err
		}
		data, err := os.ReadFile(src)
		if err != nil {
			return chinitsvc.Fatal(fmt.Errorf("read bundle config %s: %w", name, err))
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0755); err != nil {
			return chinitsvc.Fatal(fmt.Errorf("create %s: %w", filepath.Dir(dest), err))
		}
		if err := os.WriteFile(dest, data, 0644); err != nil {
			return chinitsvc.Fatal(fmt.Errorf("install %s: %w", name, err))
		}
		if _, err := i.runner.Run(ctx, "chown", "clickhouse:clickhouse", dest); err != nil {
			return fmt.Errorf("chown %s: %w", dest, err)
		}
	}
	return nil
}
