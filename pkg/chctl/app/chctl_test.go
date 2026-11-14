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
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/service"

	"github.com/stretchr/testify/require"
)

func TestPrintGcResultTSVShape(t *testing.T) {
	// The bash timer piped stdout line-by-line: dropped hosts must be
	// one TSV row each with no extra decoration, in detection order.
	var stdout, stderr bytes.Buffer
	printGcResult(&stdout, &stderr, &service.ReplicaGcResult{
		Stale:   []string{"reader-1", "reader-2", "writer-0"},
		Dropped: []string{"reader-1", "reader-2"},
		Failed:  []string{"reader-3"},
	})

	require.Equal(t, "reader-1\nreader-2\n", stdout.String())
	require.True(t, strings.HasPrefix(stderr.String(), `gc: drop failed for "reader-3"`+"\n"))
	require.Contains(t, stderr.String(), "gc: 3 stale replica(s) detected, 2 dropped, 1 failed\n")
}

func TestPrintGcResultEmptyIsSilentOnStdout(t *testing.T) {
	// A fresh cluster: no drops, no stdout output for the timer to log.
	var stdout, stderr bytes.Buffer
	printGcResult(&stdout, &stderr, &service.ReplicaGcResult{})

	require.Empty(t, stdout.String())
	require.Equal(t, "gc: 0 stale replica(s) detected, 0 dropped, 0 failed\n", stderr.String())
}

func TestUsageMentionsReplicaGc(t *testing.T) {
	// The top-level help must document the new subcommand (docs update
	// is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "chctl replica gc -on <peer>")
}

func TestUsageMentionsRenderInventoryFlags(t *testing.T) {
	// The render command's cloud inventory must appear in the help
	// (docs update is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "[-host name=address]...")
	require.Contains(t, out.String(), "[-replica name=cloudname]")
}

func TestParseRenderHosts(t *testing.T) {
	// Bare form: the live harness's host selector, unchanged.
	name, hosts, err := parseRenderHosts([]string{"0-0"})
	require.NoError(t, err)
	require.Equal(t, "0-0", name)
	require.Empty(t, hosts)

	// Inventory form (repeatable): CR name → cloud address.
	name, hosts, err = parseRenderHosts([]string{"0-0=writer-0.ip", "0-1=reader-0.ip"})
	require.NoError(t, err)
	require.Empty(t, name)
	require.Equal(t, []domain.HostIdentity{
		{Name: "0-0", Address: "writer-0.ip"},
		{Name: "0-1", Address: "reader-0.ip"},
	}, hosts)

	// Mixed: selector plus inventory — the render-everything case.
	name, hosts, err = parseRenderHosts([]string{"0-0", "0-1=reader-0.ip"})
	require.NoError(t, err)
	require.Equal(t, "0-0", name)
	require.Equal(t, []domain.HostIdentity{{Name: "0-1", Address: "reader-0.ip"}}, hosts)

	// Malformed and duplicate-selector inputs are rejected.
	_, _, err = parseRenderHosts([]string{"0-0="})
	require.ErrorContains(t, err, "name=address or a bare host name")
	_, _, err = parseRenderHosts([]string{"=writer-0.ip"})
	require.ErrorContains(t, err, "name=address or a bare host name")
	_, _, err = parseRenderHosts([]string{"0-0", "0-1"})
	require.ErrorContains(t, err, "only one bare -host selector")
}

func TestReadRemotePasswordEnvSource(t *testing.T) {
	// The env form: the value is read at render time, never argv.
	t.Setenv("CHCTL_TEST_REMOTE_PW", "env-secret")
	password, err := readRemotePassword("distuser", "CHCTL_TEST_REMOTE_PW", "")
	require.NoError(t, err)
	require.Equal(t, "env-secret", password)
}

func TestReadRemotePasswordFileSource(t *testing.T) {
	// The file form: cloud-init writes the password to a root-only file;
	// a trailing newline (echo >file) must not corrupt it.
	path := filepath.Join(t.TempDir(), "remote.pw")
	require.NoError(t, os.WriteFile(path, []byte("file-secret\n"), 0o600))
	password, err := readRemotePassword("distuser", "", path)
	require.NoError(t, err)
	require.Equal(t, "file-secret", password)
}

func TestReadRemotePasswordNoFlags(t *testing.T) {
	// Neither flag: the backwards-compatible no-credentials render.
	password, err := readRemotePassword("", "", "")
	require.NoError(t, err)
	require.Empty(t, password)
}

func TestReadRemotePasswordUsageErrors(t *testing.T) {
	// Both password sources at once: ambiguous, refuse.
	_, err := readRemotePassword("u", "VAR", "file")
	require.ErrorContains(t, err, "mutually exclusive")

	// A password source without -remote-user: half a credential pair.
	_, err = readRemotePassword("", "VAR", "")
	require.ErrorContains(t, err, "requires -remote-user")
	_, err = readRemotePassword("", "", "file")
	require.ErrorContains(t, err, "requires -remote-user")

	// -remote-user without any password source.
	_, err = readRemotePassword("u", "", "")
	require.ErrorContains(t, err, "requires -remote-password-env or -remote-password-file")

	// An unset env var must fail the render (an empty password would
	// half-authenticate against peers).
	_, err = readRemotePassword("u", "CHCTL_TEST_UNSET_PW", "")
	require.ErrorContains(t, err, "environment variable not set")

	// A missing file must fail without echoing the path.
	_, err = readRemotePassword("u", "", filepath.Join(t.TempDir(), "no-such.pw"))
	require.ErrorContains(t, err, "cannot read")
	require.NotContains(t, err.Error(), "no-such.pw", "flag value must not surface in errors")
}

func TestUsageMentionsRemoteCredentialFlags(t *testing.T) {
	// The render command's credential flags must appear in the help
	// (docs update is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "-remote-user")
	require.Contains(t, out.String(), "-remote-password-env")
	require.Contains(t, out.String(), "-remote-password-file")
}

func TestUsageMentionsUserPasswordFlag(t *testing.T) {
	// The users-group password injection flag must appear in the help
	// (docs update is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "[-user-password name=path]")
	require.Contains(t, out.String(), "-user-password injects runtime passwords")
}

func TestUsageMentionsKeeperSingleFileFlags(t *testing.T) {
	// The keeper-mode flags must appear in the help (docs update is a
	// separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "[-server-id <n>]")
	require.Contains(t, out.String(), "[-raft-id name=id]")
	require.Contains(t, out.String(), "[-single-file <path>]")
	require.Contains(t, out.String(), "-single-file merges common+host sections")
	require.Contains(t, out.String(), "-raft-id overrides the raft_configuration")
}

func writeSpecFile(t *testing.T, spec string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "keeper.yaml")
	require.NoError(t, os.WriteFile(path, []byte(spec), 0o600))
	return path
}

const appKeeperSpec = `apiVersion: clickhouse-keeper.altinity.com/v1
kind: ClickHouseKeeperInstallation
metadata:
  name: keeper-flags
spec:
  configuration:
    clusters:
      - name: keeper
        layout:
          replicasCount: 1
    settings:
      keeper_server/tcp_port: "9181"
`

func TestRunConfigSingleFileRequiresHost(t *testing.T) {
	// -single-file is host-complete: without -host the flag combination
	// must be refused before any rendering runs.
	spec := writeSpecFile(t, appKeeperSpec)
	err := runConfig([]string{"render", "-spec", spec, "-keeper", "-single-file", "/x/keeper_config.xml"})
	require.ErrorContains(t, err, "-single-file requires a keeper host")
}

func TestRunConfigServerIDRequiresHost(t *testing.T) {
	// -server-id targets the selected host's <server_id> section: with
	// no host there is nothing to shape — refuse early.
	spec := writeSpecFile(t, appKeeperSpec)
	err := runConfig([]string{"render", "-spec", spec, "-keeper", "-server-id", "1"})
	require.ErrorContains(t, err, "-server-id requires a keeper host")
}

func TestRunConfigKeeperFlagsRequireKeeperMode(t *testing.T) {
	// Against a CHI spec the keeper flags have no meaning: refuse
	// rather than silently ignoring them.
	spec := writeSpecFile(t, appKeeperSpec)
	err := runConfig([]string{"render", "-spec", spec, "-single-file", "/x/keeper_config.xml"})
	require.ErrorContains(t, err, "-single-file applies to keeper mode only")
	err = runConfig([]string{"render", "-spec", spec, "-server-id", "1"})
	require.ErrorContains(t, err, "-server-id applies to keeper mode only")
}

func TestRunConfigNegativeServerIDRejected(t *testing.T) {
	// Keeper server ids are positive integers; a negative value would
	// render an invalid <server_id> that keeper refuses at boot.
	spec := writeSpecFile(t, appKeeperSpec)
	err := runConfig([]string{"render", "-spec", spec, "-keeper", "-host", "0-0", "-server-id", "-1"})
	require.ErrorContains(t, err, "-server-id must be positive")
}

func TestParseRaftIDFlags(t *testing.T) {
	// Well-formed entries: name=id pairs, repeatable, later same-name
	// flags win (map semantics, matching the other repeatable flags).
	ids, err := parseRaftIDFlags([]string{"0-0=1", "0-1=2", "0-2=3"})
	require.NoError(t, err)
	require.Equal(t, map[string]int{"0-0": 1, "0-1": 2, "0-2": 3}, ids)

	// Empty list: nil map, the no-override render.
	ids, err = parseRaftIDFlags(nil)
	require.NoError(t, err)
	require.Nil(t, ids)

	// Malformed entries are refused — a silent skip would renumber an
	// ensemble's identity invisibly.
	_, err = parseRaftIDFlags([]string{"0-0"})
	require.ErrorContains(t, err, "must be name=id")
	_, err = parseRaftIDFlags([]string{"0-0=x"})
	require.ErrorContains(t, err, "integer id")
	_, err = parseRaftIDFlags([]string{"=1"})
	require.ErrorContains(t, err, "must be name=id")
}

func TestRunConfigRaftIDAppliesToKeeperModeOnly(t *testing.T) {
	// Against a CHI spec the raft-id mapping has no meaning: refuse
	// rather than silently ignoring it.
	spec := writeSpecFile(t, appKeeperSpec)
	err := runConfig([]string{"render", "-spec", spec, "-raft-id", "0-0=1"})
	require.ErrorContains(t, err, "-raft-id applies to keeper mode only")
}

func TestRunConfigRaftIDNegativeRejected(t *testing.T) {
	// Raft ids are positive integers (NuRaft member ids); a negative
	// value would render an invalid <server><id>.
	spec := writeSpecFile(t, appKeeperSpec)
	err := runConfig([]string{"render", "-spec", spec, "-keeper", "-host", "0-0", "-raft-id", "0-0=-1"})
	require.ErrorContains(t, err, "raft ids must be positive")
}

func TestRunConfigRaftIDAndServerIDMismatchRefused(t *testing.T) {
	// The end-to-end guard: -server-id and this node's -raft-id must
	// agree, or the operator is telling the node two different
	// identities — the exact class of misconfiguration that partitions
	// an ensemble after a re-image.
	spec := writeSpecFile(t, appKeeperSpec)
	err := runConfig([]string{
		"render",
		"-spec", spec,
		"-keeper",
		"-host", "0-0",
		"-server-id", "1",
		"-raft-id", "0-0=7",
		"-single-file", filepath.Join(t.TempDir(), "keeper_config.xml"),
	})
	require.ErrorContains(t, err, "-server-id 1 disagrees with -raft-id 0-0=7")
}

func TestRunConfigRaftIDEndToEnd(t *testing.T) {
	// The full flag wiring: -raft-id entries override the CR's
	// zero-based replica indices in BOTH output modes.
	spec := writeSpecFile(t, appKeeperSpec)
	outDir := t.TempDir()
	singlePath := filepath.Join(t.TempDir(), "keeper_config.xml")

	// Per-group mode: the raft section carries the mapped ids.
	err := runConfig([]string{
		"render",
		"-spec", spec,
		"-keeper",
		"-host", "0-0",
		"-raft-id", "0-0=1",
		"-out", outDir,
	})
	require.NoError(t, err)
	data, err := os.ReadFile(filepath.Join(outDir, "chop-generated-raft.xml"))
	require.NoError(t, err)
	require.Contains(t, string(data), "<id>1</id>", "per-group raft section must carry the mapped id")

	// Single-file mode: the merged document carries them too, agreeing
	// with -server-id for the selected host.
	err = runConfig([]string{
		"render",
		"-spec", spec,
		"-keeper",
		"-host", "0-0",
		"-server-id", "1",
		"-raft-id", "0-0=1",
		"-single-file", singlePath,
	})
	require.NoError(t, err)
	data, err = os.ReadFile(singlePath)
	require.NoError(t, err)
	merged := string(data)
	require.Contains(t, merged, "<id>1</id>")
	require.Contains(t, merged, "<server_id>1</server_id>")
}

func TestRunConfigSingleFileEndToEnd(t *testing.T) {
	// The flag wiring end-to-end: -host + -server-id + -single-file
	// produces ONE parsable file with the overridden server_id and the
	// cloud-resolved raft entry — the shape the AWS stack's keeper
	// units consume.
	spec := writeSpecFile(t, appKeeperSpec)
	singlePath := filepath.Join(t.TempDir(), "keeper_config.xml")
	outDir := t.TempDir()

	err := runConfig([]string{
		"render",
		"-spec", spec,
		"-keeper",
		"-host", "0-0",
		"-keeper-address", "0-0=keeper-0.aws.internal",
		"-server-id", "1",
		"-single-file", singlePath,
		"-out", outDir,
	})
	require.NoError(t, err)

	data, err := os.ReadFile(singlePath)
	require.NoError(t, err)
	merged := string(data)

	// Valid XML, one root.
	require.Equal(t, 1, strings.Count(merged, "<clickhouse>"))
	decoder := xml.NewDecoder(strings.NewReader(merged))
	for {
		_, err := decoder.Token()
		if err == io.EOF {
			break
		}
		require.NoError(t, err, "merged keeper_config.xml must be well-formed: %s", merged)
	}

	// The override wins over the CR's zero-based replica index.
	require.Contains(t, merged, "<server_id>1</server_id>")
	// The raft entry carries the cloud DNS name from the -host inventory.
	require.Contains(t, merged, "<hostname>keeper-0.aws.internal</hostname>")
	// And the client settings from the spec.
	require.Contains(t, merged, "<tcp_port>9181</tcp_port>")

	// -out stays empty: single-file mode skips it entirely.
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Empty(t, entries, "single-file mode must not write per-group files to -out")
}

func TestRunConfigOutDirRegression(t *testing.T) {
	// No -single-file: the flag defaults keep the per-group render
	// writing files to -out exactly as before the new flags existed.
	spec := writeSpecFile(t, appKeeperSpec)
	outDir := t.TempDir()

	err := runConfig([]string{
		"render",
		"-spec", spec,
		"-keeper",
		"-host", "0-0",
		"-out", outDir,
	})
	require.NoError(t, err)

	// Per-group files land in -out with the CR-derived server_id.
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	require.Contains(t, names, "chop-generated-server-id.xml")
	require.Contains(t, names, "chop-generated-raft.xml")
	require.Contains(t, names, "chop-generated-settings.xml")

	data, err := os.ReadFile(filepath.Join(outDir, "chop-generated-server-id.xml"))
	require.NoError(t, err)
	require.Contains(t, string(data), "<server_id>0</server_id>",
		"no -server-id flag: CR zero-based replica index must be kept")
}

func TestReadUserPasswordHashes(t *testing.T) {
	// The trailing-newline trim: cloud-init writes with printf '%s\n',
	// and hash("abc\n") != hash("abc") — an untrimmed hash silently sets
	// a different password than the stack stored.
	dir := t.TempDir()
	path := filepath.Join(dir, "admin.pw")
	require.NoError(t, os.WriteFile(path, []byte("abc\n"), 0o600))

	hashes, err := readUserPasswordHashes([]string{"admin=" + path})
	require.NoError(t, err)

	// Known vector: printf '%s' abc | sha256sum
	expected := sha256.Sum256([]byte("abc"))
	require.Equal(t, map[string]string{"admin": hex.EncodeToString(expected[:])}, hashes)

	// A CRLF-terminated file (Windows-style secret plumbing) trims too.
	require.NoError(t, os.WriteFile(path, []byte("abc\r\n"), 0o600))
	hashes, err = readUserPasswordHashes([]string{"admin=" + path})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"admin": hex.EncodeToString(expected[:])}, hashes,
		"trailing CR must be trimmed with the LF")
}

func TestReadUserPasswordHashesKnownVector(t *testing.T) {
	// Hash correctness against the independent vector the task pinned:
	// printf '%s' testpass | sha256sum → 13d249f2...9f6c.
	dir := t.TempDir()
	path := filepath.Join(dir, "default.pw")
	require.NoError(t, os.WriteFile(path, []byte("testpass\n"), 0o600))

	hashes, err := readUserPasswordHashes([]string{"default=" + path})
	require.NoError(t, err)
	require.Equal(t,
		"13d249f2cb4127b40cfa757866850278793f814ded3c587fe5889e889a7a9f6c",
		hashes["default"])
}

func TestReadUserPasswordHashesRepeatable(t *testing.T) {
	// Multiple entries: one hash per user, keyed by name.
	dir := t.TempDir()
	aPath := filepath.Join(dir, "a.pw")
	bPath := filepath.Join(dir, "b.pw")
	require.NoError(t, os.WriteFile(aPath, []byte("alpha\n"), 0o600))
	require.NoError(t, os.WriteFile(bPath, []byte("beta"), 0o600))

	hashes, err := readUserPasswordHashes([]string{"default=" + aPath, "writer=" + bPath})
	require.NoError(t, err)
	require.Len(t, hashes, 2)

	hashA := sha256.Sum256([]byte("alpha"))
	hashB := sha256.Sum256([]byte("beta"))
	require.Equal(t, hex.EncodeToString(hashA[:]), hashes["default"])
	require.Equal(t, hex.EncodeToString(hashB[:]), hashes["writer"])
}

func TestReadUserPasswordHashesUsageErrors(t *testing.T) {
	// Malformed entries are usage errors, not silent skips.
	_, err := readUserPasswordHashes([]string{"no-equals-sign"})
	require.ErrorContains(t, err, "must be name=path")
	_, err = readUserPasswordHashes([]string{"=path"})
	require.ErrorContains(t, err, "must be name=path")
	_, err = readUserPasswordHashes([]string{"name="})
	require.ErrorContains(t, err, "must be name=path")

	// An unreadable file fails the whole render — never a partial silent
	// render where some users keep the spec password while the operator
	// believes the secret-file password is live.
	_, err = readUserPasswordHashes([]string{"admin=" + filepath.Join(t.TempDir(), "missing.pw")})
	require.ErrorContains(t, err, "read")
	require.ErrorContains(t, err, "admin", "the error names the user, for cross-referencing the flag entry")

	// An empty file is refused: hashing "" would lock the account with a
	// password nobody knows, silently.
	emptyPath := filepath.Join(t.TempDir(), "empty.pw")
	require.NoError(t, os.WriteFile(emptyPath, []byte("\n"), 0o600))
	_, err = readUserPasswordHashes([]string{"admin=" + emptyPath})
	require.ErrorContains(t, err, "empty")
}

func TestReadUserPasswordHashesNeverEchoesBytes(t *testing.T) {
	// Secrets hygiene: after a password has been read into memory, a
	// later entry's failure must not echo it — the error surface stays
	// names and paths only.
	dir := t.TempDir()
	goodPath := filepath.Join(dir, "good.pw")
	require.NoError(t, os.WriteFile(goodPath, []byte("super-secret\n"), 0o600))
	bad := "admin=" + filepath.Join(dir, "missing.pw")

	// The readable file is processed first, so the password bytes are
	// in memory when the error for the missing file is built.
	_, err := readUserPasswordHashes([]string{"writer=" + goodPath, bad})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "super-secret",
		"password bytes never appear in error messages")
}

func TestPrintVersionPrintsBuildVersion(t *testing.T) {
	// The version line is machine-comparable for bootstrap scripts: it
	// starts with `chctl <version>` — the build-injected value, verbatim;
	// the Go toolchain, when available, only follows it.
	var out bytes.Buffer
	printVersion(&out)
	line := strings.TrimRight(out.String(), "\n")
	require.NotEmpty(t, line, "printVersion must write a version line")
	require.True(t, strings.HasPrefix(line, "chctl "+Version),
		"line %q must start with chctl %s", line, Version)
}

func TestRunVersionDispatch(t *testing.T) {
	// `chctl version` must dispatch without touching the filesystem or
	// the other subcommands (an unknown command must NOT print it).
	err := Run([]string{"version"})
	require.NoError(t, err)
}

func TestUsageMentionsVersionAndSchemaSyncFlags(t *testing.T) {
	// The help must list the version subcommand and the new schema sync
	// flags (docs update is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "chctl version")
	require.Contains(t, out.String(), "[-retries 2]")
	require.Contains(t, out.String(), "[-prune-stale-self]")
	require.Contains(t, out.String(), Version, "the version line carries the build version")
	// The ordering note replaced the old "positional after flags" warning.
	require.Contains(t, out.String(), "ANY order")
	require.NotContains(t, out.String(), "comes after the flags")
}

func TestSeparatePositionals(t *testing.T) {
	// Table-driven over the shapes the pre-scan must handle: positional
	// first/last, bool flags (no value), repeatable -host Var flags
	// (value-consuming), the -- terminator, a value that looks like a
	// name, and self-contained =value forms.
	cases := []struct {
		name        string
		args        []string
		flagsOnly   []string
		positionals []string
	}{
		{
			name:        "positional last",
			args:        []string{"-cluster", "x", "-secure", "reader-0"},
			flagsOnly:   []string{"-cluster", "x", "-secure"},
			positionals: []string{"reader-0"},
		},
		{
			name:        "positional first (the live finding)",
			args:        []string{"reader-0", "-cluster", "x", "-secure"},
			flagsOnly:   []string{"-cluster", "x", "-secure"},
			positionals: []string{"reader-0"},
		},
		{
			name:        "positional between flags",
			args:        []string{"-cluster", "x", "reader-0", "-secure"},
			flagsOnly:   []string{"-cluster", "x", "-secure"},
			positionals: []string{"reader-0"},
		},
		{
			name:        "repeatable -host Var flags consume their values",
			args:        []string{"reader-0", "-host", "0-0=w.ip", "-host", "0-1=r.ip", "-cluster", "x"},
			flagsOnly:   []string{"-host", "0-0=w.ip", "-host", "0-1=r.ip", "-cluster", "x"},
			positionals: []string{"reader-0"},
		},
		{
			name:        "bool flag takes no value",
			args:        []string{"-secure", "reader-0"},
			flagsOnly:   []string{"-secure"},
			positionals: []string{"reader-0"},
		},
		{
			name:        "-- terminator ends flag scanning",
			args:        []string{"-cluster", "x", "--", "-secure", "reader-0"},
			flagsOnly:   []string{"-cluster", "x", "--", "-secure", "reader-0"},
			positionals: []string{"-secure", "reader-0"},
		},
		{
			name:        "a flag value that looks like a name stays a value",
			args:        []string{"-user", "admin", "reader-0"},
			flagsOnly:   []string{"-user", "admin"},
			positionals: []string{"reader-0"},
		},
		{
			name:        "self-contained =value form",
			args:        []string{"-cluster=x", "reader-0", "-user=admin"},
			flagsOnly:   []string{"-cluster=x", "-user=admin"},
			positionals: []string{"reader-0"},
		},
		{
			name:        "double-dash flag form",
			args:        []string{"--cluster", "x", "--secure", "reader-0"},
			flagsOnly:   []string{"--cluster", "x", "--secure"},
			positionals: []string{"reader-0"},
		},
		{
			name:        "no flags at all",
			args:        []string{"reader-0", "reader-1"},
			flagsOnly:   []string{},
			positionals: []string{"reader-0", "reader-1"},
		},
		{
			name:        "int flag consumes its value",
			args:        []string{"reader-0", "-port", "8443", "-retries", "3"},
			flagsOnly:   []string{"-port", "8443", "-retries", "3"},
			positionals: []string{"reader-0"},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fs, _, _, _, _, _, _, _, err := newSchemaFlagSet("test")
			require.NoError(t, err)
			// The -retries/-prune-stale-self/-on/-name-pattern flags the
			// real command sets: bound through the same FlagSet the scan
			// reads.
			var retries int
			var prune bool
			var runOn string
			var pattern string
			fs.IntVar(&retries, "retries", 2, "")
			fs.BoolVar(&prune, "prune-stale-self", false, "")
			fs.StringVar(&runOn, "on", "", "")
			fs.StringVar(&pattern, "name-pattern", "reader-*", "")

			flagArgs, positionals := separatePositionals(fs, c.args)
			require.Equal(t, c.flagsOnly, flagArgs)
			require.Equal(t, c.positionals, positionals)

			// The pre-scan never executes flags; fs.Parse over the
			// stripped list must accept every case and bind the values.
			require.NoError(t, fs.Parse(flagArgs))
		})
	}
}

func TestSeparatePositionalsLeavesUnknownFlagsToParse(t *testing.T) {
	// A typo'd flag must keep flag-package semantics: left for fs.Parse
	// to reject with its own message, never mistaken for a positional.
	// Its value token is positional by shape (a bare word) — fs.Parse
	// rejects the unknown flag first.
	fs, _, _, _, _, _, _, _, err := newSchemaFlagSet("test")
	require.NoError(t, err)
	flagArgs, positionals := separatePositionals(fs, []string{"reader-0", "-clustr"})
	require.Equal(t, []string{"-clustr"}, flagArgs)
	require.Equal(t, []string{"reader-0"}, positionals)
	require.Error(t, fs.Parse(flagArgs))
}

func schemaTestSpec(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "chi.yaml")
	// A minimal-but-parseable CHI: the schema commands load the spec
	// AFTER flag validation, so the file only needs to exist.
	require.NoError(t, os.WriteFile(path, []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: pos
spec:
  configuration:
    clusters:
      - name: logexplorer
        layout: { shardsCount: 1, replicasCount: 1 }
`), 0o600))
	return path
}

func TestRunReplicaDropAcceptsPositionalFirst(t *testing.T) {
	// The live finding, end-to-end: `replica drop reader-0 -spec ...`
	// previously failed with "-cluster is required" (Go's flag package
	// stops at the first positional). Now the spec-read stage is reached
	// — proving the -cluster flag was parsed and bound.
	err := runReplicaDrop([]string{
		"reader-0", "-spec", schemaTestSpec(t), "-cluster", "logexplorer", "-on", "0-0",
	})
	// The failure must be past flag validation (an inventory/host
	// error), never "-cluster is required".
	require.Error(t, err)
	require.NotContains(t, err.Error(), "-cluster is required")
	require.NotContains(t, err.Error(), "-spec is required")
	require.NotContains(t, err.Error(), "-on is required")
}

func TestRunSchemaSyncFlagsAfterPositionalReachSpecRead(t *testing.T) {
	// The same pre-scan for schema sync: flags after the positional are
	// bound (the runner-build stage is reached, not flag validation).
	err := runSchema([]string{
		"sync", "0-1", "-spec", schemaTestSpec(t), "-cluster", "logexplorer",
		"-host", "0-0=writer-0.example.internal", "-secure",
	})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "-cluster is required")
	require.NotContains(t, err.Error(), "usage:")
}

func TestRunSchemaSyncRejectsRetriesBelowOne(t *testing.T) {
	// -retries 0 (or negative) is a usage error: zero passes would mean
	// "do nothing and report success", which no caller wants.
	err := runSchema([]string{
		"sync", "0-1", "-spec", schemaTestSpec(t), "-cluster", "logexplorer", "-retries", "0",
	})
	require.ErrorContains(t, err, "-retries must be at least 1")
}

func TestRunSchemaSyncExactOnePositionalValidation(t *testing.T) {
	// Both missing and multiple positionals stay usage errors under the
	// pre-scan (the positionals are counted before any spec load).
	err := runSchema([]string{"sync", "-spec", schemaTestSpec(t), "-cluster", "logexplorer"})
	require.ErrorContains(t, err, "got 0 positional argument(s)")
	err = runSchema([]string{
		"sync", "0-1", "0-2", "-spec", schemaTestSpec(t), "-cluster", "logexplorer",
	})
	require.ErrorContains(t, err, "got 2 positional argument(s)")
}

func TestRunReplicaGcRejectsStrayPositional(t *testing.T) {
	// gc is flag-only: a stray positional must fail as a usage error, not
	// be silently swallowed as a flag value (the pre-scan surfaces it).
	err := runReplicaGc([]string{
		"writer-0", "-spec", schemaTestSpec(t), "-cluster", "logexplorer", "-on", "0-0",
	})
	require.ErrorContains(t, err, "no positional argument")
	require.ErrorContains(t, err, "writer-0")
}

func TestRunReplicaGcExcludeFlagAcceptsRepeatedHosts(t *testing.T) {
	// The -exclude flag is repeatable (chinit's writer pass lists every
	// active lease claim): the pre-scan must carry BOTH the flag token
	// and its value as flag tokens — a value consumed as a positional
	// leaves -exclude without an argument and fs.Parse rejects it.
	//
	// The spec is deliberately nonexistent: runReplicaGc exits the
	// process (exit 3) once detection runs, so the spec read is the last
	// observable boundary — reaching it proves the flags parsed clean.
	err := runReplicaGc([]string{
		"-spec", "/nonexistent/chi.yaml", "-cluster", "logexplorer", "-on", "writer-0",
		"-name-pattern", "writer-*",
		"-exclude", "writer-0",
		"-exclude", "writer-1",
	})
	require.ErrorContains(t, err, "read spec")
	require.NotContains(t, err.Error(), "flag needs an argument")
	require.NotContains(t, err.Error(), "no positional argument")
}

// --- config render-node (single-invocation node render + install) ---

// renderNodeCHISpec is a minimal-but-complete CHI for the render-node
// app tests: a cluster (remote_servers), a zookeeper section (host
// group), users (both groups), settings and profiles.
const renderNodeCHISpec = `
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: render-node-app
spec:
  configuration:
    clusters:
      - name: logexplorer
        layout: { shardsCount: 1, replicasCount: 1 }
    zookeeper:
      nodes:
        - host: keeper-0.ch.internal
          port: 9181
    profiles:
      default/async_insert: "1"
    settings:
      query_log/partition_by: "toYYYYMM(event_date)"
    users:
      default/profile: readonly
      admin/profile: default
      admin/access_management: "1"
`

// writeRenderNodeSpec writes the shared fixture to a temp file.
func writeRenderNodeSpec(t *testing.T) string {
	t.Helper()
	return writeSpecFile(t, renderNodeCHISpec)
}

// renderNodeTestUser pins the numeric self-owner so the install asserts
// perms deterministically regardless of the host's accounts.
func renderNodeTestUser() string {
	return fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
}

func TestRunRenderNodeRequiresSpecAndOut(t *testing.T) {
	// -spec and -out are both required: the command's whole value is
	// the install discipline, and an implicit destination is how the
	// old multi-call flow lost track of what landed where.
	err := runConfig([]string{"render-node", "-out", t.TempDir()})
	require.ErrorContains(t, err, "-spec is required")
	err = runConfig([]string{"render-node", "-spec", writeRenderNodeSpec(t)})
	require.ErrorContains(t, err, "-out is required")
}

func TestRunRenderNodeNodeTypeValidation(t *testing.T) {
	spec := writeRenderNodeSpec(t)
	// An unknown role is a typo, not a default.
	err := runConfig([]string{"render-node", "-spec", spec, "-out", t.TempDir(), "-node-type", "writerz"})
	require.ErrorContains(t, err, "-node-type must be writer|reader|backup|keeper")
	// keeper role and -keeper must agree — a CHK spec renders keeper
	// nodes only, and a keeper spec with a server role would render a
	// server config from keeper identity data.
	err = runConfig([]string{"render-node", "-spec", spec, "-out", t.TempDir(), "-keeper", "-node-type", "writer"})
	require.ErrorContains(t, err, "a CHK spec renders keeper nodes only")
	err = runConfig([]string{"render-node", "-spec", spec, "-out", t.TempDir(), "-node-type", "keeper"})
	require.ErrorContains(t, err, "-node-type keeper requires -keeper")
}

func TestRunRenderNodeRequiresHost(t *testing.T) {
	// Without a host there is no "this node" to render — the command
	// is node-scoped by definition.
	err := runConfig([]string{"render-node", "-spec", writeRenderNodeSpec(t), "-out", t.TempDir()})
	require.ErrorContains(t, err, "render-node requires a host")
}

func TestRunRenderNodeDryRunWritesNothing(t *testing.T) {
	spec := writeRenderNodeSpec(t)
	outDir := t.TempDir()
	usersDir := t.TempDir()

	err := runConfig([]string{
		"render-node", "-spec", spec, "-host", "0-0",
		"-out", outDir, "-users-dir", usersDir,
		"-user", renderNodeTestUser(), "-dry-run",
	})
	require.NoError(t, err)

	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Empty(t, entries, "dry-run must not write to config.d")
	entries, err = os.ReadDir(usersDir)
	require.NoError(t, err)
	require.Empty(t, entries, "dry-run must not write to users.d")
}

func TestRunRenderNodeInstallsWithPerms(t *testing.T) {
	spec := writeRenderNodeSpec(t)
	outDir := t.TempDir()

	err := runConfig([]string{
		"render-node", "-spec", spec, "-host", "0-0",
		"-out", outDir, "-user", renderNodeTestUser(),
		"-remote-user", "distwriter", "-remote-password-env", "CHCTL_RN_PW",
	})
	// An unset env var must fail before anything renders; then the
	// real render with the password set.
	require.ErrorContains(t, err, "environment variable not set")

	require.NoError(t, os.Setenv("CHCTL_RN_PW", "sekret"))
	defer os.Unsetenv("CHCTL_RN_PW")
	err = runConfig([]string{
		"render-node", "-spec", spec, "-host", "0-0",
		"-out", outDir, "-user", renderNodeTestUser(),
		"-remote-user", "distwriter", "-remote-password-env", "CHCTL_RN_PW",
	})
	require.NoError(t, err)

	// -users-dir defaulted to the users.d sibling of -out; the config.d
	// and users.d files landed with the mode split.
	info, err := os.Stat(filepath.Join(outDir, "chop-generated-remote_servers.xml"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm(), "credentialed remote_servers is 0640")
	info, err = os.Stat(filepath.Join(outDir, "chop-generated-macros.xml"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
	usersDir := filepath.Join(filepath.Dir(outDir), "users.d")
	info, err = os.Stat(filepath.Join(usersDir, "z_chctl-users.xml"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm(), "users files carry password hashes: 0640")

	// The manifest rides the managed directories (the prune authority).
	require.FileExists(t, filepath.Join(outDir, ".chctl-render-node.manifest"))
	require.FileExists(t, filepath.Join(usersDir, ".chctl-render-node.manifest"))
}

// exitCoder mirrors main.go's coder: an error that dictates its own
// process exit code (the -check drift contract, 4).
type exitCoder interface {
	ExitCode() int
}

func TestRunRenderNodeCheckExitSyncAndDrift(t *testing.T) {
	spec := writeRenderNodeSpec(t)
	outDir := t.TempDir()
	args := []string{
		"render-node", "-spec", spec, "-host", "0-0",
		"-out", outDir, "-user", renderNodeTestUser(),
	}

	// Check before install: every file missing → driftError with the
	// exit-4 contract.
	err := runConfig(append(args, "-check"))
	require.Error(t, err)
	var coded exitCoder
	require.ErrorAs(t, err, &coded)
	require.Equal(t, exitConfigDrift, coded.ExitCode())

	// Install; then check with identical args: in sync, nil error.
	require.NoError(t, runConfig(args))
	require.NoError(t, runConfig(append(args, "-check")))

	// An edited installed file drifts again.
	edited := filepath.Join(outDir, "chop-generated-macros.xml")
	data, err := os.ReadFile(edited)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(edited, append(data, []byte("<!-- drift -->")...), 0o644))
	err = runConfig(append(args, "-check"))
	require.ErrorAs(t, err, &coded)
	require.Equal(t, exitConfigDrift, coded.ExitCode())
}

func TestRunRenderNodeKeeperMode(t *testing.T) {
	spec := writeSpecFile(t, appKeeperSpec)
	singlePath := filepath.Join(t.TempDir(), "keeper_config.xml")
	outDir := t.TempDir()

	err := runConfig([]string{
		"render-node", "-spec", spec, "-keeper", "-node-type", "keeper",
		"-host", "0-0", "-server-id", "1", "-single-file", singlePath,
		"-out", outDir, "-user", renderNodeTestUser(),
	})
	require.NoError(t, err)

	data, err := os.ReadFile(singlePath)
	require.NoError(t, err)
	require.Contains(t, string(data), "<server_id>1</server_id>")
	// -out untouched: keeper mode writes only the single file.
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Empty(t, entries)

	// Keeper flags against a CHI spec are refused (the mode gate).
	err = runConfig([]string{"render-node", "-spec", spec, "-host", "0-0", "-out", t.TempDir(), "-single-file", singlePath})
	require.ErrorContains(t, err, "-single-file applies to keeper mode only")
	// And keeper mode demands -single-file.
	err = runConfig([]string{"render-node", "-spec", spec, "-keeper", "-node-type", "keeper", "-host", "0-0", "-out", t.TempDir()})
	require.ErrorContains(t, err, "keeper mode requires -single-file")
}

func TestRunRenderNodeRejectsStrayPositionalAndMutuallyExclusiveModes(t *testing.T) {
	spec := writeRenderNodeSpec(t)
	// The pre-scan surfaces a stray positional as a usage error, never
	// swallows it as a flag value.
	err := runConfig([]string{"render-node", "reader-0", "-spec", spec, "-out", t.TempDir()})
	require.ErrorContains(t, err, "no positional argument")
	require.ErrorContains(t, err, "reader-0")

	// -check already writes nothing; -dry-run on top asks twice.
	err = runConfig([]string{"render-node", "-spec", spec, "-out", t.TempDir(), "-check", "-dry-run"})
	require.ErrorContains(t, err, "mutually exclusive")
}

func TestRunRenderNodeUserPasswordFlagWiresThrough(t *testing.T) {
	// The -user-password hash injection must flow into render-node's
	// users files exactly as in `config render`.
	spec := writeRenderNodeSpec(t)
	dir := t.TempDir()
	pwFile := filepath.Join(dir, "admin.pw")
	require.NoError(t, os.WriteFile(pwFile, []byte("testpass\n"), 0o600))
	outDir := t.TempDir()

	err := runConfig([]string{
		"render-node", "-spec", spec, "-host", "0-0",
		"-out", outDir, "-user", renderNodeTestUser(),
		"-user-password", "admin=" + pwFile,
	})
	require.NoError(t, err)

	data, err := os.ReadFile(filepath.Join(outDir, "..", "users.d", "chop-generated-users.xml"))
	require.NoError(t, err)
	require.Contains(t, string(data),
		"<password_sha256_hex>13d249f2cb4127b40cfa757866850278793f814ded3c587fe5889e889a7a9f6c</password_sha256_hex>")
}

func TestUsageMentionsRenderNode(t *testing.T) {
	// The help documents the new subcommand and its one-shot contract
	// (docs update is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "chctl config render-node")
	require.Contains(t, out.String(), "[-node-type writer|reader|backup|keeper]")
	require.Contains(t, out.String(), "render-node renders EVERYTHING for ONE node")
	require.Contains(t, out.String(), "-check diffs the render against the installed")
}
