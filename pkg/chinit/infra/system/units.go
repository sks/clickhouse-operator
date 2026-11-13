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
	"fmt"
	"os"
	"path/filepath"
)

// UnitFile describes one systemd unit chinit owns. Unit content is data
// (not heredocs): chinit subcommands are the ExecStart, so the unit is a
// thin wrapper — and golden-file tests compare it byte-for-byte.
type UnitFile struct {
	// Name is the unit filename ("schema-sync.service").
	Name string
	// Content is the full unit body.
	Content string
}

// WriteFile converges one file: create parent dirs, write only when the
// content differs (preserving mtimes when nothing changed), chmod.
func WriteFile(path, content string, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return fmt.Errorf("create dir %s: %w", filepath.Dir(path), err)
	}
	if existing, err := os.ReadFile(path); err == nil && string(existing) == content {
		if err := os.Chmod(path, mode); err != nil {
			return fmt.Errorf("chmod %s: %w", path, err)
		}
		return nil
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		return fmt.Errorf("chmod %s: %w", path, err)
	}
	return nil
}

// UnitInstaller writes systemd units and drop-ins under the system dirs.
type UnitInstaller struct {
	// UnitDir is /etc/systemd/system (injectable for tests).
	UnitDir string
	// DropinDir is the clickhouse-server drop-in directory.
	DropinDir string
}

// NewUnitInstaller builds an installer rooted at the given unit dir
// (production: /etc/systemd/system) with the clickhouse-server drop-in
// directory beside it.
func NewUnitInstaller(unitDir string) *UnitInstaller {
	return &UnitInstaller{
		UnitDir:   unitDir,
		DropinDir: filepath.Join(unitDir, "clickhouse-server.service.d"),
	}
}

// InstallUnits writes every unit and returns the paths written. After a
// successful call the boot step runs `systemctl daemon-reload` via the
// runner (kept out of the installer so tests never shell systemctl).
func (u *UnitInstaller) InstallUnits(units []UnitFile) ([]string, error) {
	var written []string
	for _, unit := range units {
		path := filepath.Join(u.UnitDir, unit.Name)
		if err := WriteFile(path, unit.Content, 0644); err != nil {
			return written, fmt.Errorf("unit %s: %w", unit.Name, err)
		}
		written = append(written, path)
	}
	return written, nil
}

// InstallDropin writes one clickhouse-server drop-in.
func (u *UnitInstaller) InstallDropin(name, content string) error {
	if err := WriteFile(filepath.Join(u.DropinDir, name), content, 0644); err != nil {
		return fmt.Errorf("drop-in %s: %w", name, err)
	}
	return nil
}
