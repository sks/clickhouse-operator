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
