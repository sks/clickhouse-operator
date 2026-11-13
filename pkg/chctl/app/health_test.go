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
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/service"

	"github.com/stretchr/testify/require"
)

// appHealthServer is an httptest fake for the chctl health end-to-end
// tests: it answers the driver's pool ping and serves one TSV body per
// query marker (the same wire shape the infra tests use). Markers are
// checked in a FIXED order (not map order) so a query that matches
// several markers answers deterministically — most-specific first in
// the responses map's construction.
type appHealthServer struct {
	// markers holds query substrings in match order with their TSV
	// bodies.
	markers []appHealthMarker
}

type appHealthMarker struct {
	marker string
	body   string
}

// respond registers one marker body, matched in registration order.
func (f *appHealthServer) respond(marker, body string) *appHealthServer {
	f.markers = append(f.markers, appHealthMarker{marker: marker, body: body})
	return f
}

func startAppHealthServer(t *testing.T, fake *appHealthServer) (string, int) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "text/tab-separated-values; charset=UTF-8")
		for _, m := range fake.markers {
			if strings.Contains(string(query), m.marker) {
				_, _ = w.Write([]byte(m.body))
				return
			}
		}
		_, _ = w.Write([]byte("1\nUInt8\n1\n"))
	}))
	t.Cleanup(server.Close)
	parts := strings.Split(strings.TrimPrefix(server.URL, "http://"), ":")
	port, err := strconv.Atoi(parts[1])
	require.NoError(t, err)
	return parts[0], port
}

// healthyReplicasTSV is a clean replication fan-out result.
func healthyReplicasTSV() string {
	return "host_name\tdatabase\ttable\tname\tis_readonly\treplica_is_active\n" +
		"String\tString\tString\tString\tUInt8\tUInt8\n" +
		"writer-0\tlogexplorer\tevents\twriter-0\t0\t1\n"
}

// startAppKeeper boots a TCP listener answering mntr with a leader
// payload (the keeper probe tests' wire-server approach).
func startAppKeeper(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer func() { _ = conn.Close() }()
				buf := make([]byte, 4)
				if _, err := conn.Read(buf); err != nil {
					return
				}
				if string(buf) == "mntr" {
					_, _ = conn.Write([]byte(
						"zk_version\t2.8.5\nzk_server_state\tleader\nzk_synced_followers\t2\nzk_voting_followers\t3\n"))
				}
			}(conn)
		}
	}()
	return listener.Addr().String()
}

// healthSpec writes a CHI spec declaring one .sql file whose CREATEs
// name the expected tables; the -cluster name matches.
func healthSpec(t *testing.T, cluster string, sql string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "chi.yaml")
	var b strings.Builder
	b.WriteString("apiVersion: clickhouse.altinity.com/v1\n")
	b.WriteString("kind: ClickHouseInstallation\n")
	b.WriteString("metadata:\n")
	b.WriteString("  name: health\n")
	b.WriteString("spec:\n")
	b.WriteString("  configuration:\n")
	b.WriteString("    clusters:\n")
	b.WriteString("      - name: " + cluster + "\n")
	b.WriteString("        layout: { shardsCount: 1, replicasCount: 1 }\n")
	b.WriteString("    files:\n")
	b.WriteString("      init.sql: |\n")
	for _, line := range strings.Split(strings.TrimRight(sql, "\n"), "\n") {
		b.WriteString("        " + line + "\n")
	}
	require.NoError(t, os.WriteFile(path, []byte(b.String()), 0o600))
	return path
}

func TestRunHealthHealthyClusterExitZero(t *testing.T) {
	// The stack's gate: healthy cluster, all checks pass, exit 0. The
	// human output is the runbook-pasteable shape (one line per check
	// + summary).
	fake := &appHealthServer{}
	fake.respond("WHERE name = 'writer-0'", "database\ttable\tname\tis_readonly\treplica_is_active\n"+
		"String\tString\tString\tUInt8\tUInt8\n"+
		"logexplorer\tevents\twriter-0\t0\t1\n")
	fake.respond("clusterAllReplicas", healthyReplicasTSV())
	fake.respond("FROM system.tables", "database\tname\n"+
		"String\tString\n"+
		"logexplorer\tevents\n"+
		"logexplorer\tminutes_log_index_summary\n")
	host, port := startAppHealthServer(t, fake)
	keeperAddr := startAppKeeper(t)
	spec := healthSpec(t, "logexplorer",
		"CREATE TABLE IF NOT EXISTS logexplorer.events (x Int) ENGINE=ReplicatedMergeTree;\n"+
			"CREATE TABLE logexplorer.minutes_log_index_summary (x Int) ENGINE=SummingMergeTree;\n")

	var out bytes.Buffer
	report, err := runHealthWith(&out, []string{
		"-cluster", "logexplorer",
		"-host", "0-0=" + host,
		"-port", strconv.Itoa(port),
		"-spec", spec,
		"-keeper", keeperAddr,
		"-self", "writer-0",
		"-timeout", "2s",
	})

	require.NoError(t, err)
	require.True(t, report.Healthy(), out.String())
	human := out.String()
	require.Contains(t, human, "✓ server: 1/1 hosts up\n")
	require.Contains(t, human, "✓ keeper: leader (")
	require.Contains(t, human, "✓ replicas: 1 replicated table(s), none readonly\n")
	require.Contains(t, human, "✓ tables: 2 expected table(s) present on 1 host(s)\n")
	require.Contains(t, human, `✓ self: replica "writer-0" registered and active`)
	require.Contains(t, human, "status: healthy (5 pass, 0 fail, 0 skip)\n")
}

func TestRunHealthReadonlyReplicaDegraded(t *testing.T) {
	// The runbook's is_readonly query as a gate: one readonly row
	// degrades the report and NAMES the table — exit 3 at the CLI.
	fake := &appHealthServer{}
	fake.respond("clusterAllReplicas", "host_name\tdatabase\ttable\tname\tis_readonly\treplica_is_active\n"+
		"String\tString\tString\tString\tUInt8\tUInt8\n"+
		"writer-0\tlogexplorer\tevents\twriter-0\t0\t1\n"+
		"reader-0\tlogexplorer\tevents\treader-0\t1\t1\n")
	host, port := startAppHealthServer(t, fake)

	var out bytes.Buffer
	report, err := runHealthWith(&out, []string{
		"-cluster", "logexplorer",
		"-host", "0-0=" + host,
		"-host", "0-1=" + host,
		"-port", strconv.Itoa(port),
		"-timeout", "2s",
	})

	require.NoError(t, err)
	require.False(t, report.Healthy())
	require.Equal(t, service.ReportStatusDegraded, report.Status)
	require.Contains(t, out.String(), "✗ replicas: 1 readonly table(s): logexplorer.events replica=reader-0 host=reader-0")
	require.Contains(t, out.String(), "status: degraded (1 pass, 1 fail, 3 skip)")
}

func TestRunHealthUnreachableHostDegraded(t *testing.T) {
	// A host that is down: per-host fail in the server check, the rest
	// of the checks still ran (the report is one pass, not an abort).
	// The down host uses TEST-NET-1 (guaranteed unroutable — the same
	// reserved range the keeper probe tests use) with a short timeout.
	fake := &appHealthServer{}
	fake.respond("clusterAllReplicas", healthyReplicasTSV())
	upHost, upPort := startAppHealthServer(t, fake)

	var out bytes.Buffer
	report, err := runHealthWith(&out, []string{
		"-cluster", "logexplorer",
		"-host", "0-0=" + upHost,
		"-host", "0-1=192.0.2.1",
		"-port", strconv.Itoa(upPort),
		"-timeout", "1s",
	})

	require.NoError(t, err)
	require.False(t, report.Healthy())
	human := out.String()
	require.Contains(t, human, "✗ server: 1/2 hosts up; down: 0-1:")
	// The reachable host still answered the replicas fan-out.
	require.Contains(t, human, "✓ replicas: 1 replicated table(s), none readonly\n")
}

func TestRunHealthJSONShape(t *testing.T) {
	// The -json wire form: parse it back and pin the shape — status,
	// five checks with name/status/detail in execution order.
	fake := &appHealthServer{}
	fake.respond("clusterAllReplicas", healthyReplicasTSV())
	host, port := startAppHealthServer(t, fake)

	var out bytes.Buffer
	report, err := runHealthWith(&out, []string{
		"-cluster", "logexplorer",
		"-host", "0-0=" + host,
		"-port", strconv.Itoa(port),
		"-json",
		"-timeout", "2s",
	})

	require.NoError(t, err)
	require.True(t, report.Healthy())
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(out.Bytes(), &decoded))
	require.Equal(t, "healthy", decoded["status"])
	checks := decoded["checks"].([]any)
	require.Len(t, checks, 5)
	names := []string{}
	for _, c := range checks {
		row := c.(map[string]any)
		require.Contains(t, row, "name")
		require.Contains(t, row, "status")
		require.Contains(t, row, "detail")
		names = append(names, row["name"].(string))
	}
	require.Equal(t, []string{"server", "keeper", "replicas", "tables", "self"}, names)
}

func TestRunHealthSpecMissingTableDetection(t *testing.T) {
	// The spec declares two tables; the cluster holds one — the missing
	// name is the actionable detail.
	fake := &appHealthServer{}
	fake.respond("clusterAllReplicas", healthyReplicasTSV())
	fake.respond("FROM system.tables", "database\tname\n"+
		"String\tString\n"+
		"logexplorer\tevents\n")
	host, port := startAppHealthServer(t, fake)
	spec := healthSpec(t, "logexplorer",
		"CREATE TABLE logexplorer.events (x Int) ENGINE=ReplicatedMergeTree;\n"+
			"CREATE TABLE logexplorer.minutes_log_index_summary (x Int) ENGINE=SummingMergeTree;\n")

	var out bytes.Buffer
	report, err := runHealthWith(&out, []string{
		"-cluster", "logexplorer",
		"-host", "0-0=" + host,
		"-port", strconv.Itoa(port),
		"-spec", spec,
		"-timeout", "2s",
	})

	require.NoError(t, err)
	require.False(t, report.Healthy())
	require.Contains(t, out.String(),
		"✗ tables: missing 1 of 2 expected table(s) on 1 host(s): logexplorer.minutes_log_index_summary")
}

func TestRunHealthClusterWhitelistedBySpec(t *testing.T) {
	// The fan-out SQL interpolates the cluster name; a name the CR does
	// not declare is refused before any SQL runs (usage error, exit 1
	// at main — same gate as replica gc).
	fake := &appHealthServer{}
	host, port := startAppHealthServer(t, fake)
	spec := healthSpec(t, "logexplorer", "CREATE TABLE logexplorer.events (x Int);")

	_, err := runHealthWith(io.Discard, []string{
		"-cluster", "evil'; DROP TABLE system.replicas; --",
		"-host", "0-0=" + host,
		"-port", strconv.Itoa(port),
		"-spec", spec,
	})

	require.ErrorContains(t, err, "not declared by the CR's spec")
}

func TestRunHealthUsageErrors(t *testing.T) {
	// The flag surface's usage errors: each names the missing/misused
	// flag (exit 1 at main).
	_, err := runHealthWith(io.Discard, []string{"-host", "0-0=writer-0.ip"})
	require.ErrorContains(t, err, "-cluster is required")
	_, err = runHealthWith(io.Discard, []string{"-cluster", "logexplorer"})
	require.ErrorContains(t, err, "-host <name=address> is required")
	_, err = runHealthWith(io.Discard, []string{"-cluster", "logexplorer", "-host", "writer-0"})
	require.ErrorContains(t, err, "must be name=address")
	_, err = runHealthWith(io.Discard, []string{"-cluster", "logexplorer", "-host", "0-0=writer-0.ip", "-timeout", "0s"})
	require.ErrorContains(t, err, "-timeout must be positive")
	_, err = runHealthWith(io.Discard, []string{"-cluster", "logexplorer", "-host", "0-0=writer-0.ip", "-timeout", "garbage"})
	require.ErrorContains(t, err, "parse -timeout")
	// Stray positional: rejected, not swallowed.
	_, err = runHealthWith(io.Discard, []string{"writer-0", "-cluster", "logexplorer", "-host", "0-0=writer-0.ip"})
	require.ErrorContains(t, err, "no positional argument")
	// -self works with the full multi-host inventory (the check reads
	// the replicas fan-out), so the usage-error list ends here.
}

func TestRunHealthSelfUnregisteredDegraded(t *testing.T) {
	// Fresh reader boot gate: tables created, registration absent —
	// degraded with the replica named.
	fake := &appHealthServer{}
	fake.respond("WHERE name = 'reader-0'", "database\ttable\tname\tis_readonly\treplica_is_active\n"+
		"String\tString\tString\tUInt8\tUInt8\n")
	fake.respond("clusterAllReplicas", "host_name\tdatabase\ttable\tname\tis_readonly\treplica_is_active\n"+
		"String\tString\tString\tString\tUInt8\tUInt8\n")
	host, port := startAppHealthServer(t, fake)

	var out bytes.Buffer
	report, err := runHealthWith(&out, []string{
		"-cluster", "logexplorer",
		"-host", "0-0=" + host,
		"-port", strconv.Itoa(port),
		"-self", "reader-0",
		"-timeout", "2s",
	})

	require.NoError(t, err)
	require.False(t, report.Healthy())
	require.Contains(t, out.String(), `✗ self: replica "reader-0" not registered in system.replicas`)
}

func TestUsageMentionsHealth(t *testing.T) {
	// The top-level help must document the health command surface
	// (docs update is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "chctl health -cluster <name>")
	require.Contains(t, out.String(), "[-spec chi.yaml] [-keeper <host:port>] [-self <replica>] [-json]")
}
