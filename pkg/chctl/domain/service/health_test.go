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

package service

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chctl/infra/keeper"

	"github.com/stretchr/testify/require"
)

// fakeHealthProbe is a hand-rolled ClusterHealthProbe stub — the same
// inline-fake style the gc tests use. Each method fails or answers
// per its fields so the orchestration tests can compose a scenario in
// one struct literal.
type fakeHealthProbe struct {
	// pingErrs maps host names to ping errors; others succeed.
	pingErrs map[string]error
	// replicas is the system.replicas fan-out result.
	replicas []port.HealthReplicaRow
	// replicasErr makes QueryReplicas fail.
	replicasErr error
	// tables is per-host existing tables.
	tables map[string][]domain.TableRef
	// tablesErrs maps host names to list errors.
	tablesErrs map[string]error
}

func (f *fakeHealthProbe) PingHost(ctx context.Context, request port.HealthPingRequest) (port.HealthHostResult, error) {
	if err, ok := f.pingErrs[request.Host.Name]; ok {
		return port.HealthHostResult{}, err
	}
	return port.HealthHostResult{Address: request.Host.Address}, nil
}

func (f *fakeHealthProbe) QueryReplicas(ctx context.Context, request port.HealthReplicasRequest) ([]port.HealthReplicaRow, error) {
	return f.replicas, f.replicasErr
}

func (f *fakeHealthProbe) QueryTables(ctx context.Context, request port.HealthTablesRequest) ([]domain.TableRef, error) {
	if err, ok := f.tablesErrs[request.Host.Name]; ok {
		return nil, err
	}
	return f.tables[request.Host.Name], nil
}

// fakeHealthKeeper is a KeeperProbe stub with a fixed result.
type fakeHealthKeeper struct {
	result port.MntrResult
	err    error
}

func (f *fakeHealthKeeper) Mntr(ctx context.Context, request port.MntrRequest) (port.MntrResult, error) {
	return f.result, f.err
}

// startMntrListener is a real TCP listener answering the mntr 4lw with
// a canned payload — the keeper probe tests' own wire-protocol
// approach; the health service exercises the real keeper.Probe against
// it, not a fake, because the keeper check must prove it consumes the
// real probe path.
func startMntrListener(t *testing.T, response string) string {
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
					_, _ = conn.Write([]byte(response))
				}
			}(conn)
		}
	}()
	return listener.Addr().String()
}

// healthTestRequest is the common request shape for the scenario tests.
func healthTestRequest(hosts ...domain.HostIdentity) HealthRequest {
	return HealthRequest{
		ClusterName: "logexplorer",
		Hosts:       hosts,
		Credentials: domain.Credentials{Username: "default"},
		HTTPPort:    8123,
	}
}

func healthHosts() []domain.HostIdentity {
	return []domain.HostIdentity{
		{Name: "0-0", Address: "writer-0.example.internal"},
		{Name: "0-1", Address: "reader-0.example.internal"},
	}
}

func healthService(cluster port.ClusterHealthProbe, keeper port.KeeperProbe) *HealthService {
	return NewHealthService(cluster, keeper, func() []domain.TableRef { return nil })
}

func healthCheck(t *testing.T, svc *HealthService, request HealthRequest) *HealthReport {
	t.Helper()
	report, err := svc.Check(context.Background(), request)
	require.NoError(t, err)
	return report
}

func TestHealthAllPassIsHealthy(t *testing.T) {
	svc := healthService(&fakeHealthProbe{
		replicas: []port.HealthReplicaRow{
			{Host: "writer-0", Database: "logexplorer", Table: "events", Replica: "writer-0", Active: true},
		},
	}, &fakeHealthKeeper{result: port.MntrResult{ServerState: "leader"}})
	request := healthTestRequest(healthHosts()...)
	request.KeeperAddress = "keeper-0:9181"

	report := healthCheck(t, svc, request)

	require.Equal(t, ReportStatusHealthy, report.Status)
	require.True(t, report.Healthy())
	// The five checks run in a fixed order with a stable shape.
	require.Equal(t, []string{"server", "keeper", "replicas", "tables", "self"},
		checkNames(report))
	for _, check := range report.Checks {
		switch check.Name {
		case "tables", "self":
			// Optional checks not requested: skip, never fail.
			require.Equal(t, CheckStatusSkip, check.Status, check.Detail)
		default:
			require.Equal(t, CheckStatusPass, check.Status, check.Detail)
		}
	}
	// Human output: pass line per check + the summary line.
	require.Contains(t, report.FormatHuman(), "✓ server: 2/2 hosts up\n")
	require.Contains(t, report.FormatHuman(), "✓ replicas: 1 replicated table(s), none readonly\n")
	require.Contains(t, report.FormatHuman(), "status: healthy (3 pass, 0 fail, 2 skip)\n")
}

func TestHealthServerFanOutReportsEveryDownHost(t *testing.T) {
	// BOTH hosts down: the check names each one — the fan-out must not
	// stop at the first failure (a cluster health command inherits a
	// load balancer's blind spot if it only pings one host).
	svc := healthService(&fakeHealthProbe{
		pingErrs: map[string]error{
			"0-0": errors.New("connection refused"),
			"0-1": errors.New("no route to host"),
		},
	}, &fakeHealthKeeper{})

	report := healthCheck(t, svc, healthTestRequest(healthHosts()...))

	require.Equal(t, ReportStatusDegraded, report.Status)
	server := findCheck(t, report, "server")
	require.Equal(t, CheckStatusFail, server.Status)
	require.Contains(t, server.Detail, "0/2 hosts up")
	require.Contains(t, server.Detail, "0-0: connection refused")
	require.Contains(t, server.Detail, "0-1: no route to host")
}

func TestHealthPartialOutageStillReportsOtherHosts(t *testing.T) {
	// One of two hosts down: the up-count reflects it, the rest of the
	// checks still ran.
	svc := healthService(&fakeHealthProbe{
		pingErrs: map[string]error{"0-1": errors.New("connection refused")},
	}, &fakeHealthKeeper{})

	report := healthCheck(t, svc, healthTestRequest(healthHosts()...))

	server := findCheck(t, report, "server")
	require.Equal(t, CheckStatusFail, server.Status)
	require.Contains(t, server.Detail, "1/2 hosts up")
	require.Equal(t, ReportStatusDegraded, report.Status)
}

func TestHealthReadonlyReplicaFailsAndNamesTheTable(t *testing.T) {
	// The runbook's ad-hoc `is_readonly` query, as a gate: the check
	// must name the table and replica so the operator knows where to
	// look without re-running SQL.
	svc := healthService(&fakeHealthProbe{
		replicas: []port.HealthReplicaRow{
			{Host: "reader-0", Database: "logexplorer", Table: "events", Replica: "reader-0", Active: true},
			{Host: "reader-0", Database: "logexplorer", Table: "minutes_log_index_summary", Replica: "reader-0", Readonly: true, Active: true},
		},
	}, &fakeHealthKeeper{})

	report := healthCheck(t, svc, healthTestRequest(healthHosts()...))

	replicas := findCheck(t, report, "replicas")
	require.Equal(t, CheckStatusFail, replicas.Status)
	require.Contains(t, replicas.Detail, "1 readonly table(s)")
	require.Contains(t, replicas.Detail, "logexplorer.minutes_log_index_summary")
	require.Contains(t, replicas.Detail, "replica=reader-0")
	require.Equal(t, ReportStatusDegraded, report.Status)
}

func TestHealthKeeperCheckReportsServerState(t *testing.T) {
	// mntr reachable with a state: pass, and the state is reported —
	// leader/follower/standalone is information, not a verdict.
	svc := healthService(&fakeHealthProbe{}, &fakeHealthKeeper{
		result: port.MntrResult{ServerState: "follower"},
	})
	request := healthTestRequest(healthHosts()...)
	request.KeeperAddress = "keeper-0:9181"

	report := healthCheck(t, svc, request)

	keeper := findCheck(t, report, "keeper")
	require.Equal(t, CheckStatusPass, keeper.Status)
	require.Contains(t, keeper.Detail, "follower")
}

func TestHealthKeeperEmptyStateIsFailure(t *testing.T) {
	// A keeper that answers mntr with nothing is not healthy — the
	// same contract `keeper mntr` carries (never an empty success).
	svc := healthService(&fakeHealthProbe{}, &fakeHealthKeeper{
		result: port.MntrResult{},
	})
	request := healthTestRequest(healthHosts()...)
	request.KeeperAddress = "keeper-0:9181"

	report := healthCheck(t, svc, request)

	keeper := findCheck(t, report, "keeper")
	require.Equal(t, CheckStatusFail, keeper.Status)
	require.Contains(t, keeper.Detail, "empty zk_server_state")
	require.Equal(t, ReportStatusDegraded, report.Status)
}

func TestHealthKeeperUnreachableIsFailure(t *testing.T) {
	// The real keeper.Probe against a listener that accepts and closes
	// without answering: the check fails with the address in the
	// detail (exit-3 semantics), never a panic and never a pass.
	keeperAddr := startMntrListener(t, "")
	svc := healthService(&fakeHealthProbe{}, keeper.NewProbe())
	request := healthTestRequest(healthHosts()...)
	request.KeeperAddress = keeperAddr

	report := healthCheck(t, svc, request)

	keeper := findCheck(t, report, "keeper")
	require.Equal(t, CheckStatusFail, keeper.Status)
	require.Contains(t, keeper.Detail, keeperAddr)
	require.Equal(t, ReportStatusDegraded, report.Status)
}

func TestHealthRealKeeperProbeLeaderPasses(t *testing.T) {
	// End-to-end keeper check over the real wire protocol: the real
	// keeper.Probe (from infra) injected through the port boundary —
	// the domain never imports infra itself, the test does (the same
	// boundary the CLI wires in production).
	keeperAddr := startMntrListener(t,
		"zk_version\t2.8.5\n"+
			"zk_server_state\tleader\n"+
			"zk_synced_followers\t2\n"+
			"zk_voting_followers\t3\n")
	svc := NewHealthService(&fakeHealthProbe{}, keeper.NewProbe(), func() []domain.TableRef { return nil })
	request := healthTestRequest(healthHosts()...)
	request.KeeperAddress = keeperAddr

	report := healthCheck(t, svc, request)

	keeper := findCheck(t, report, "keeper")
	require.Equal(t, CheckStatusPass, keeper.Status)
	require.Contains(t, keeper.Detail, "leader")
	require.Contains(t, keeper.Detail, keeperAddr)
}

func TestHealthTablesMissingSpecTablesFails(t *testing.T) {
	// The spec declares two tables; the cluster holds one — the
	// missing name is the detail the runbook acts on.
	expected := []domain.TableRef{
		{Database: "logexplorer", Name: "events"},
		{Database: "logexplorer", Name: "minutes_log_index_summary"},
	}
	svc := NewHealthService(&fakeHealthProbe{
		tables: map[string][]domain.TableRef{
			"0-0": {{Database: "logexplorer", Name: "events"}},
			"0-1": {{Database: "logexplorer", Name: "events"}},
		},
	}, &fakeHealthKeeper{}, func() []domain.TableRef { return expected })
	request := healthTestRequest(healthHosts()...)
	request.CheckTables = true

	report := healthCheck(t, svc, request)

	tables := findCheck(t, report, "tables")
	require.Equal(t, CheckStatusFail, tables.Status)
	require.Contains(t, tables.Detail, "missing 1 of 2 expected table(s)")
	require.Contains(t, tables.Detail, "logexplorer.minutes_log_index_summary")
	require.Equal(t, ReportStatusDegraded, report.Status)
}

func TestHealthTablesPresentOnAnyHostPasses(t *testing.T) {
	// A table existing on ONE host is present: schema sync materializes
	// per-host and a fresh reader may lag — the self check covers that
	// node, the tables check verifies the cluster holds the schema.
	expected := []domain.TableRef{
		{Database: "logexplorer", Name: "events"},
		{Database: "logexplorer", Name: "minutes_log_index_summary"},
	}
	svc := NewHealthService(&fakeHealthProbe{
		tables: map[string][]domain.TableRef{
			"0-0": {{Database: "logexplorer", Name: "events"}, {Database: "logexplorer", Name: "minutes_log_index_summary"}},
			"0-1": {{Database: "logexplorer", Name: "events"}},
		},
	}, &fakeHealthKeeper{}, func() []domain.TableRef { return expected })
	request := healthTestRequest(healthHosts()...)
	request.CheckTables = true

	report := healthCheck(t, svc, request)

	tables := findCheck(t, report, "tables")
	require.Equal(t, CheckStatusPass, tables.Status, tables.Detail)
	require.Contains(t, tables.Detail, "2 expected table(s) present on 2 host(s)")
}

func TestHealthTablesUnreachableHostsIsFailure(t *testing.T) {
	// No host could answer the table listing: the check cannot claim
	// the tables exist — failure, not skip (the check WAS requested).
	svc := NewHealthService(&fakeHealthProbe{
		tablesErrs: map[string]error{
			"0-0": errors.New("connection refused"),
			"0-1": errors.New("connection refused"),
		},
	}, &fakeHealthKeeper{}, func() []domain.TableRef {
		return []domain.TableRef{{Database: "logexplorer", Name: "events"}}
	})
	request := healthTestRequest(healthHosts()...)
	request.CheckTables = true

	report := healthCheck(t, svc, request)

	tables := findCheck(t, report, "tables")
	require.Equal(t, CheckStatusFail, tables.Status)
	require.Contains(t, tables.Detail, "no reachable host")
}

func TestHealthSelfRegisteredAndActivePasses(t *testing.T) {
	// The post-boot gate: this node's replica is registered AND active.
	// The check consumes the replicas fan-out rows, so both replicas'
	// rows are in the result and the -self name selects its own.
	svc := healthService(&fakeHealthProbe{
		replicas: []port.HealthReplicaRow{
			{Host: "writer-0", Database: "logexplorer", Table: "events", Replica: "writer-0", Active: true},
			{Host: "reader-0", Database: "logexplorer", Table: "events", Replica: "reader-0", Active: true},
			{Host: "reader-0", Database: "logexplorer", Table: "minutes_log_index_summary", Replica: "reader-0", Active: true},
		},
	}, &fakeHealthKeeper{})
	request := healthTestRequest(healthHosts()...)
	request.SelfName = "reader-0"

	report := healthCheck(t, svc, request)

	self := findCheck(t, report, "self")
	require.Equal(t, CheckStatusPass, self.Status)
	require.Contains(t, self.Detail, `replica "reader-0" registered and active`)
	require.Contains(t, self.Detail, "2 table(s)")
}

func TestHealthSelfUnregisteredFails(t *testing.T) {
	// Schema sync created tables but the registration never landed —
	// the node is not serving replication. The fan-out shows the OTHER
	// replica only; the -self name has no rows.
	svc := healthService(&fakeHealthProbe{
		replicas: []port.HealthReplicaRow{
			{Host: "writer-0", Database: "logexplorer", Table: "events", Replica: "writer-0", Active: true},
		},
	}, &fakeHealthKeeper{})
	request := healthTestRequest(healthHosts()...)
	request.SelfName = "reader-0"

	report := healthCheck(t, svc, request)

	self := findCheck(t, report, "self")
	require.Equal(t, CheckStatusFail, self.Status)
	require.Contains(t, self.Detail, `replica "reader-0" not registered in system.replicas`)
}

func TestHealthSelfInactiveFails(t *testing.T) {
	// Registered but replica_is_active=0 (restarted, mid-resync): the
	// runbook wants "participates", not "is known".
	svc := healthService(&fakeHealthProbe{
		replicas: []port.HealthReplicaRow{
			{Host: "reader-0", Database: "logexplorer", Table: "events", Replica: "reader-0", Active: false},
		},
	}, &fakeHealthKeeper{})
	request := healthTestRequest(healthHosts()...)
	request.SelfName = "reader-0"

	report := healthCheck(t, svc, request)

	self := findCheck(t, report, "self")
	require.Equal(t, CheckStatusFail, self.Status)
	require.Contains(t, self.Detail, "inactive")
}

func TestHealthJSONShapeIsStable(t *testing.T) {
	// The -json contract scripts pin on: one object, status, checks in
	// execution order with name/status/detail. Parse it back — the
	// shape check must not depend on marshal whitespace.
	svc := healthService(&fakeHealthProbe{
		pingErrs: map[string]error{"0-1": errors.New("connection refused")},
		replicas: []port.HealthReplicaRow{
			{Host: "writer-0", Database: "logexplorer", Table: "events", Replica: "writer-0", Active: true},
		},
	}, &fakeHealthKeeper{})
	request := healthTestRequest(healthHosts()...)

	report := healthCheck(t, svc, request)

	var decoded map[string]any
	out, err := json.Marshal(report.ToJSONShape())
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(out, &decoded))
	require.Equal(t, ReportStatusDegraded, decoded["status"])
	checks := decoded["checks"].([]any)
	require.Len(t, checks, 5)
	first := checks[0].(map[string]any)
	require.Equal(t, "server", first["name"])
	require.Equal(t, "fail", first["status"])
	require.Contains(t, first["detail"], "1/2 hosts up")
}

func TestHealthUsageErrors(t *testing.T) {
	svc := healthService(&fakeHealthProbe{}, &fakeHealthKeeper{})
	ctx := context.Background()

	// Cluster and inventory are required.
	_, err := svc.Check(ctx, HealthRequest{Hosts: healthHosts()})
	require.ErrorContains(t, err, "cluster name is empty")
	_, err = svc.Check(ctx, HealthRequest{ClusterName: "logexplorer"})
	require.ErrorContains(t, err, "host inventory is empty")
}

func TestHealthReplicasQueryFailureFailsAllHostsTried(t *testing.T) {
	// The fan-out walks every host; only after ALL of them fail is the
	// check a failure — a down first host must not preclude a healthy
	// second one answering.
	allDown := &fakeHealthProbe{replicasErr: errors.New("connection refused")}
	report := healthCheck(t, healthService(allDown, &fakeHealthKeeper{}), healthTestRequest(healthHosts()...))
	replicas := findCheck(t, report, "replicas")
	require.Equal(t, CheckStatusFail, replicas.Status)
	require.Contains(t, replicas.Detail, "query failed on all 2 inventory hosts")

	// A healthy result from the second host would have surfaced — the
	// loop tries each in order.
	firstDown := &fakeHealthProbe{replicas: []port.HealthReplicaRow{
		{Host: "writer-0", Database: "logexplorer", Table: "events", Replica: "writer-0", Active: true},
	}}
	report = healthCheck(t, healthService(firstDown, &fakeHealthKeeper{}), healthTestRequest(healthHosts()...))
	require.Equal(t, CheckStatusPass, findCheck(t, report, "replicas").Status)
}

// checkNames returns the checks' names in order.
func checkNames(report *HealthReport) []string {
	names := make([]string, 0, len(report.Checks))
	for _, check := range report.Checks {
		names = append(names, check.Name)
	}
	return names
}

// findCheck returns the named check.
func findCheck(t *testing.T, report *HealthReport, name string) HealthCheck {
	t.Helper()
	for _, check := range report.Checks {
		if check.Name == name {
			return check
		}
	}
	t.Fatalf("check %q not in report: %+v", name, report.Checks)
	return HealthCheck{}
}

func TestTableRefString(t *testing.T) {
	require.Equal(t, "logexplorer.events", domain.TableRef{Database: "logexplorer", Name: "events"}.String())
	require.Equal(t, "events", domain.TableRef{Name: "events"}.String())
}
