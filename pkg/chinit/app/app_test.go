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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestUsage_NoCommand: bare `chinit` prints usage and errors.
func TestRun_NoCommand(t *testing.T) {
	require.Error(t, Run(nil))
}

// TestRun_UnknownCommand: unknown commands error (and usage went to
// stderr — the error text names the command).
func TestRun_UnknownCommand(t *testing.T) {
	err := Run([]string{"teleport"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "teleport")
}

// TestRun_Version: the version command succeeds and stamps the build
// version (bootstrap scripts compare against the release tag).
func TestRun_Version(t *testing.T) {
	require.NoError(t, Run([]string{"version"}))
}

// TestParseBootFlags_Defaults: the spec path defaults to the bootstrap
// bundle location; the cloud flag defaults to empty (detect).
func TestParseBootFlags_Defaults(t *testing.T) {
	flags, err := parseBootFlags([]string{})
	require.NoError(t, err)
	require.Equal(t, "/var/lib/clickhouse-bootstrap/chinit.yaml", flags.specPath)
	require.Empty(t, flags.cloud)
	require.False(t, flags.dryRun)
}

// TestParseBootFlags_RejectsPositionals: stray positionals are usage
// errors, not silently dropped args.
func TestParseBootFlags_RejectsPositionals(t *testing.T) {
	_, err := parseBootFlags([]string{"unexpected"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected")
}

// TestRun_BootDryRun: -dry-run validates the spec and prints the plan
// without touching the system. A missing spec file must be a clean
// error naming the path (not a panic).
func TestRun_BootDryRun(t *testing.T) {
	t.Run("missing spec errors cleanly", func(t *testing.T) {
		err := Run([]string{"boot", "-spec", "/nonexistent/chinit.yaml"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "/nonexistent/chinit.yaml")
	})

	t.Run("valid spec prints the step plan", func(t *testing.T) {
		dir := t.TempDir()
		spec := dir + "/chinit.yaml"
		content := `role: writer
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
		require.NoError(t, os.WriteFile(spec, []byte(content), 0644))
		// -cloud pins the provider (a dev machine has no DMI/IMDS —
		// detection is exercised in the detect package's tests).
		err := Run([]string{"boot", "-spec", spec, "-cloud", "aws", "-dry-run", "-state", dir + "/state.json"})
		require.NoError(t, err)
	})

	t.Run("invalid spec names the field", func(t *testing.T) {
		dir := t.TempDir()
		spec := dir + "/chinit.yaml"
		// role present, identity_mode missing → SpecError naming it.
		content := `role: writer
cluster_name: logexplorer
config_bundle:
  bucket: data-bucket
disks:
  metadata_label: ch-metadata
secrets:
  default_password: x
`
		require.NoError(t, os.WriteFile(spec, []byte(content), 0644))
		err := Run([]string{"boot", "-spec", spec, "-dry-run"})
		require.Error(t, err)
		require.Contains(t, err.Error(), "identity_mode")
	})
}
