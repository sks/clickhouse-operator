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
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/stretchr/testify/require"
)

// checkSpec is a minimal CHI the render pipeline accepts end-to-end.
const checkSpec = `
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: drift-check
spec:
  configuration:
    clusters:
      - name: logexplorer
        layout: { shardsCount: 1, replicasCount: 1 }
    profiles:
      default/async_insert: "1"
`

func writeCheckSpec(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "chi.yaml")
	require.NoError(t, os.WriteFile(path, []byte(checkSpec), 0o600))
	return path
}

func TestCheckRenderDriftIdenticalContentInSync(t *testing.T) {
	// The core contract: a tree the render produced byte-for-byte is
	// IN SYNC — the refresher's no-op case.
	rendered := []port.RenderedFile{
		{Group: "common", Path: "chop-generated-remote_servers.xml", Content: "<a/>"},
		{Group: "users", Path: "chop-generated-users.xml", Content: "<b/>"},
	}
	outDir := t.TempDir()
	usersDir := t.TempDir()
	for _, file := range rendered {
		dir := outDir
		if file.Group == "users" {
			dir = usersDir
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, file.Path), []byte(file.Content), 0o640))
	}

	verdicts, inSync := checkRenderDrift(rendered, outDir, usersDir)
	require.True(t, inSync)
	require.Len(t, verdicts, 2)
	require.Equal(t, []fileVerdict{
		{Path: filepath.Join(outDir, "chop-generated-remote_servers.xml"), Status: verdictInSync},
		{Path: filepath.Join(usersDir, "chop-generated-users.xml"), Status: verdictInSync},
	}, verdicts)
}

func TestCheckRenderDriftChangedFileDrifted(t *testing.T) {
	// A hand-edited installed file is DRIFTED — byte-compare, not XML
	// equivalence: an operator's stray newline is still drift.
	rendered := []port.RenderedFile{
		{Group: "common", Path: "settings.xml", Content: "<settings/>\n"},
	}
	outDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outDir, "settings.xml"), []byte("<settings/>\n<!-- tuned by hand -->\n"), 0o640))

	verdicts, inSync := checkRenderDrift(rendered, outDir, "")
	require.False(t, inSync)
	require.Equal(t, verdictDrifted, verdicts[0].Status)
}

func TestCheckRenderDriftMissingInstalledFileIsDrift(t *testing.T) {
	// A missing installed file counts as drift (verdict MISSING), never
	// as in sync: a wiped config.d must not report a clean tree.
	rendered := []port.RenderedFile{
		{Group: "common", Path: "settings.xml", Content: "<settings/>\n"},
	}
	outDir := t.TempDir()

	verdicts, inSync := checkRenderDrift(rendered, outDir, "")
	require.False(t, inSync)
	require.Equal(t, []fileVerdict{
		{Path: filepath.Join(outDir, "settings.xml"), Status: verdictMissing},
	}, verdicts)

	// The missing verdict surfaces through runCheck with exit code 4.
	err := runCheck(rendered, outDir, "", os.Stdout)
	require.Error(t, err)
	var drift *driftError
	require.ErrorAs(t, err, &drift)
	require.Equal(t, exitConfigDrift, drift.ExitCode())
	require.Contains(t, err.Error(), "1 of 1")
}

func TestCheckRenderDriftUsersDirComparedSeparately(t *testing.T) {
	// users-group files compare against -users-dir, common against
	// -out: identical names in both directories must not cross-match.
	rendered := []port.RenderedFile{
		{Group: "common", Path: "settings.xml", Content: "common"},
		{Group: "users", Path: "settings.xml", Content: "users"},
	}
	outDir := t.TempDir()
	usersDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outDir, "settings.xml"), []byte("common"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(usersDir, "settings.xml"), []byte("stale"), 0o640))

	verdicts, inSync := checkRenderDrift(rendered, outDir, usersDir)
	require.False(t, inSync)
	require.Equal(t, verdictInSync, verdicts[0].Status)
	require.Equal(t, filepath.Join(outDir, "settings.xml"), verdicts[0].Path)
	require.Equal(t, verdictDrifted, verdicts[1].Status)
	require.Equal(t, filepath.Join(usersDir, "settings.xml"), verdicts[1].Path)
}

func TestCheckRenderDriftUsersDirDefaultsToOutDir(t *testing.T) {
	// The empty -users-dir default must match writeFiles: users-group
	// files land in -out when the flag is omitted.
	rendered := []port.RenderedFile{
		{Group: "users", Path: "users.xml", Content: "u"},
	}
	outDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outDir, "users.xml"), []byte("u"), 0o640))

	verdicts, inSync := checkRenderDrift(rendered, outDir, "")
	require.True(t, inSync)
	require.Equal(t, filepath.Join(outDir, "users.xml"), verdicts[0].Path)
}

func TestCheckRenderDriftUnreadableDestinationIsDrift(t *testing.T) {
	// A destination that exists but cannot be read (a directory in the
	// file's place) is drift, not silence — the refresher must see the
	// node as out of sync, never as clean.
	rendered := []port.RenderedFile{
		{Group: "common", Path: "settings.xml", Content: "x"},
	}
	outDir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(outDir, "settings.xml"), 0o755))

	verdicts, inSync := checkRenderDrift(rendered, outDir, "")
	require.False(t, inSync)
	require.Equal(t, verdictDrifted, verdicts[0].Status)
}

func TestRunCheckPrintsVerdictLinesAndDriftSummary(t *testing.T) {
	// The output contract: one "<STATUS> <path>" line per file, in
	// render order — machine-greppable (MISSING/DRIFTED names the file)
	// and readable in a boot log.
	rendered := []port.RenderedFile{
		{Group: "common", Path: "a.xml", Content: "a"},
		{Group: "common", Path: "b.xml", Content: "b"},
		{Group: "users", Path: "c.xml", Content: "c"},
	}
	outDir := t.TempDir()
	usersDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outDir, "a.xml"), []byte("a"), 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(usersDir, "c.xml"), []byte("tampered"), 0o640))

	var out bytes.Buffer
	err := runCheck(rendered, outDir, usersDir, &out)
	require.Error(t, err)
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	require.Len(t, lines, 3)
	require.Equal(t, "IN SYNC "+filepath.Join(outDir, "a.xml"), lines[0])
	require.Equal(t, "MISSING "+filepath.Join(outDir, "b.xml"), lines[1])
	require.Equal(t, "DRIFTED "+filepath.Join(usersDir, "c.xml"), lines[2])

	var drift *driftError
	require.ErrorAs(t, err, &drift)
	require.Equal(t, 2, drift.drifted)
	require.Equal(t, exitConfigDrift, drift.ExitCode())
}

func TestRunCheckInSyncReturnsNil(t *testing.T) {
	// Exit 0 path: every verdict line still prints (the refresher logs
	// them), but the error is nil — no drift summary on stdout's tail.
	rendered := []port.RenderedFile{{Group: "common", Path: "a.xml", Content: "a"}}
	outDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outDir, "a.xml"), []byte("a"), 0o640))

	var out bytes.Buffer
	require.NoError(t, runCheck(rendered, outDir, "", &out))
	require.Equal(t, []string{"IN SYNC " + filepath.Join(outDir, "a.xml")},
		strings.Split(strings.TrimRight(out.String(), "\n"), "\n"))
}

func TestRunConfigCheckEndToEndInSync(t *testing.T) {
	// End-to-end: a real render, then -check against it — the exact
	// refresher flow (render at boot; check on every refresh tick).
	spec := writeCheckSpec(t)
	outDir := t.TempDir()
	usersDir := t.TempDir()
	require.NoError(t, runConfig([]string{
		"render", "-spec", spec, "-out", outDir, "-users-dir", usersDir,
	}))

	require.NoError(t, runConfig([]string{
		"render", "-spec", spec, "-out", outDir, "-users-dir", usersDir, "-check",
	}))
}

func TestRunConfigCheckEndToEndDriftedAndMissing(t *testing.T) {
	// End-to-end drift: a tampered users file and a deleted common file
	// both surface, and the returned error is the exit-4 drift error.
	spec := writeCheckSpec(t)
	outDir := t.TempDir()
	usersDir := t.TempDir()
	require.NoError(t, runConfig([]string{
		"render", "-spec", spec, "-out", outDir, "-users-dir", usersDir,
	}))

	usersFiles, err := os.ReadDir(usersDir)
	require.NoError(t, err)
	require.NotEmpty(t, usersFiles)
	tampered := filepath.Join(usersDir, usersFiles[0].Name())
	require.NoError(t, os.WriteFile(tampered, []byte("<tampered/>\n"), 0o640))
	commonFiles, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.NotEmpty(t, commonFiles)
	require.NoError(t, os.Remove(filepath.Join(outDir, commonFiles[0].Name())))

	err = runConfig([]string{
		"render", "-spec", spec, "-out", outDir, "-users-dir", usersDir, "-check",
	})
	require.Error(t, err)
	var drift *driftError
	require.ErrorAs(t, err, &drift)
	require.Equal(t, exitConfigDrift, drift.ExitCode())

	// The check wrote nothing: the deleted file stays deleted, the
	// tampered file keeps its bytes (installing would be -check's exact
	// opposite).
	_, statErr := os.Stat(filepath.Join(outDir, commonFiles[0].Name()))
	require.True(t, os.IsNotExist(statErr))
	data, readErr := os.ReadFile(tampered)
	require.NoError(t, readErr)
	require.Equal(t, "<tampered/>\n", string(data))
}

func TestRunConfigCheckMutuallyExclusiveWithDryRun(t *testing.T) {
	// -check implies no writes (the -dry-run guarantee); passing both is
	// the caller asking twice for the same no-op.
	err := runConfig([]string{
		"render", "-spec", writeCheckSpec(t), "-out", t.TempDir(), "-check", "-dry-run",
	})
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestRunConfigCheckSingleFileRefused(t *testing.T) {
	// The single-file drift check is the render-node surface: a
	// -single-file render never installs per-group files to -out, so a
	// per-group comparison would report everything MISSING by
	// definition. Refused as a usage error, never a false verdict.
	err := runConfig([]string{
		"render", "-spec", writeSpecFile(t, appKeeperSpec), "-keeper", "-host", "0-0",
		"-single-file", filepath.Join(t.TempDir(), "keeper_config.xml"), "-check",
	})
	require.ErrorContains(t, err, "-check with -single-file is not supported here")
}

func TestUsageMentionsCheckFlag(t *testing.T) {
	// The flag must appear in the help (docs update is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "[-check]")
	require.Contains(t, out.String(), "IN SYNC / DRIFTED <file> / MISSING <file>")
}
