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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWriteFile_Converges: identical content skips the write (mtime
// stability), different content rewrites, mode applied both ways.
func TestWriteFile_Converges(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "unit.service")

	require.NoError(t, WriteFile(path, "first", 0644))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "first", string(data))

	// Same content: no error, content intact.
	require.NoError(t, WriteFile(path, "first", 0644))

	// Different content: rewritten.
	require.NoError(t, WriteFile(path, "second", 0600))
	data, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "second", string(data))

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

// TestUnitInstaller_WritesUnits: units land under the injected unit dir
// with the exact content (golden-file stability for the ExecStart
// contract the units carry).
func TestUnitInstaller_WritesUnits(t *testing.T) {
	dir := t.TempDir()
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
	require.NoError(t, err)
	require.Len(t, written, 2)

	data, err := os.ReadFile(filepath.Join(dir, "schema-sync.service"))
	require.NoError(t, err)
	require.Contains(t, string(data), "ExecStart=/usr/local/bin/chinit schema-sync -retries 3",
		"the ExecStart must call chinit — embedded bash in units is the retired era")
}

// TestUnitInstaller_InstallDropin: the clickhouse-server drop-in
// directory is created on demand.
func TestUnitInstaller_InstallDropin(t *testing.T) {
	dir := t.TempDir()
	installer := NewUnitInstaller(dir)
	require.NoError(t, installer.InstallDropin("limits.conf", "[Service]\nLimitNOFILE=262144\n"))
	data, err := os.ReadFile(filepath.Join(dir, "clickhouse-server.service.d", "limits.conf"))
	require.NoError(t, err)
	require.Contains(t, string(data), "LimitNOFILE=262144")
}
