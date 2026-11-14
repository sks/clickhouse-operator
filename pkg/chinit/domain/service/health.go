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
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// Check statuses. "skip" is a first-class outcome: optional checks
// (-keeper, -spec, -self) that were not requested must appear in the
// report so scripts see a stable shape, while never counting against
// health.
const (
	CheckStatusPass = "pass"
	CheckStatusFail = "fail"
	CheckStatusSkip = "skip"
)

// Report statuses: healthy only when every executed check passed.
const (
	ReportStatusHealthy  = "healthy"
	ReportStatusDegraded = "degraded"
)

// HealthCheck is one check's outcome. Detail is human-oriented but
// stable enough for scripts to grep (table names, replica names, the
// error text).
type HealthCheck struct {
	// Name identifies the check: "server", "keeper", "replicas",
	// "tables", "self" (per-host server checks are aggregated under
	// "server" with the host in Detail; the JSON shape stays one entry
	// per check, not per host — scripts pin on the five names).
	Name string
	// Status is pass, fail or skip.
	Status string
	// Detail says what happened: pass — a short "what was verified";
	// fail — the offending host/table/replica plus the reason; skip —
	// why the check did not run (which flag enables it).
	Detail string
}

// HealthReport is the full outcome of one health run.
type HealthReport struct {
	// Status is "healthy" or "degraded".
	Status string
	// Checks is one entry per check, in execution order: server, keeper,
	// replicas, tables, self.
	Checks []HealthCheck
	// ExpectedTables carries the spec-derived expected table names for
	// the tables check (empty when -spec was not given).
	ExpectedTables []domain.TableRef
}

// Healthy reports whether every executed check passed (skips don't
// count against health).
func (r *HealthReport) Healthy() bool {
	return r.Status == ReportStatusHealthy
}

// FormatHuman renders the runbook-pasteable output: one line per check
// (✓/✗/SKIP) and a summary line.
func (r *HealthReport) FormatHuman() string {
	var b strings.Builder
	for _, check := range r.Checks {
		switch check.Status {
		case CheckStatusPass:
			b.WriteString("✓ ")
		case CheckStatusFail:
			b.WriteString("✗ ")
		default:
			b.WriteString("SKIP ")
		}
		b.WriteString(check.Name)
		if check.Detail != "" {
			b.WriteString(": " + check.Detail)
		}
		b.WriteString("\n")
	}
	summary := fmt.Sprintf("status: %s (%d pass, %d fail, %d skip)",
		r.Status, r.count(CheckStatusPass), r.count(CheckStatusFail), r.count(CheckStatusSkip))
	b.WriteString(summary + "\n")
	return b.String()
}

// JSONShape is the -json wire form: {"status":..., "checks":[{name,
// status, detail}...]}. A struct (not a map) so the key order is stable
// and scripts can pin on it.
type JSONShape struct {
	Status string         `json:"status"`
	Checks []JSONCheckRow `json:"checks"`
}

// JSONCheckRow is one check in the -json output.
type JSONCheckRow struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

// ToJSONShape projects the report onto the stable wire form.
func (r *HealthReport) ToJSONShape() JSONShape {
	rows := make([]JSONCheckRow, 0, len(r.Checks))
	for _, check := range r.Checks {
		rows = append(rows, JSONCheckRow{Name: check.Name, Status: check.Status, Detail: check.Detail})
	}
	return JSONShape{Status: r.Status, Checks: rows}
}

func (r *HealthReport) count(status string) int {
	n := 0
	for _, check := range r.Checks {
		if check.Status == status {
			n++
		}
	}
	return n
}

// DefaultHealthTimeout bounds one probe when the -timeout flag is not
// given. 5s matches the keeper mntr default: a probe that cannot answer
// in 5s is a down node, not a slow one.
const DefaultHealthTimeout = 5 * time.Second

// HealthService runs the stack's verification checks — the one command
// that gates boots, refreshes and runbooks, replacing the ad-hoc SQL +
// mntr snippets each of those carried. It composes two ports: the
// cluster health probe (server/replicas/tables/self SQL) and the keeper
// probe (mntr), both injected — the service itself carries no SQL and
// no driver types.
type HealthService struct {
	// cluster reads ClickHouse-side state.
	cluster port.ClusterHealthProbe
	// keeper reads keeper mntr state (nil never happens — the CLI
	// always injects the probe; the flag decides whether the check
	// runs).
	keeper port.KeeperProbe
	// expectedTables derives the tables the spec declares, so the
	// tables check can compare without the service knowing where in
	// the CR the DDL lives. Injected from the infra layer (the CR
	// files section is operator machinery).
	expectedTables func() []domain.TableRef
}

// NewHealthService builds the service over the probes and the
// expected-tables resolver.
func NewHealthService(cluster port.ClusterHealthProbe, keeper port.KeeperProbe, expectedTables func() []domain.TableRef) *HealthService {
	return &HealthService{cluster: cluster, keeper: keeper, expectedTables: expectedTables}
}

// HealthRequest is the input to HealthService.Check.
type HealthRequest struct {
	// ClusterName is the ClickHouse cluster name (clusterAllReplicas
	// fan-out; whitelisted against the CR by the adapter, same as
	// replica gc).
	ClusterName string
	// Hosts is the cluster inventory; the server check fans out over
	// every entry and reports each.
	Hosts []domain.HostIdentity
	// Credentials connect to the hosts.
	Credentials domain.Credentials
	// HTTPPort is the HTTP interface port (default 8123).
	HTTPPort int
	// Secure marks whether the cluster requires TLS.
	Secure bool
	// Timeout bounds one probe (default 5s).
	Timeout time.Duration
	// KeeperAddress, when non-empty, runs the keeper check against that
	// mntr endpoint. Empty skips the check.
	KeeperAddress string
	// CheckTables enables the tables-present check (the -spec flag).
	CheckTables bool
	// SelfName, when non-empty, runs the self-registration check for
	// that replica name. Empty skips the check. The check consumes the
	// replicas fan-out rows (cluster-wide, host_name tagged) — the
	// full inventory stays valid, no single-host requirement.
	SelfName string
}

// Check runs every requested check and reports pass/fail/skip per
// check. A failing probe is a failed check — never an abort: a cluster
// health command must report the whole picture in one pass (the
// runbooks paste one output, not five partial runs).
//
// The replicas rows are fetched once and shared by the replicas and
// self checks: both judge system.replicas state, and a second fan-out
// would double the query load for the same rows.
func (s *HealthService) Check(ctx context.Context, request HealthRequest) (*HealthReport, error) {
	timeout := request.Timeout
	if timeout == 0 {
		timeout = DefaultHealthTimeout
	}
	if request.ClusterName == "" {
		return nil, fmt.Errorf("cluster name is empty")
	}
	if len(request.Hosts) == 0 {
		return nil, fmt.Errorf("host inventory is empty")
	}

	report := &HealthReport{Status: ReportStatusHealthy}

	serverDetail, serverOK := s.checkServers(ctx, request, timeout)
	report.Checks = append(report.Checks, HealthCheck{Name: "server", Status: passFail(serverOK), Detail: serverDetail})

	keeperDetail, keeperOK, keeperSkipped := s.checkKeeper(ctx, request, timeout)
	switch {
	case keeperSkipped:
		report.Checks = append(report.Checks, HealthCheck{Name: "keeper", Status: CheckStatusSkip, Detail: keeperDetail})
	case keeperOK:
		report.Checks = append(report.Checks, HealthCheck{Name: "keeper", Status: CheckStatusPass, Detail: keeperDetail})
	default:
		report.Checks = append(report.Checks, HealthCheck{Name: "keeper", Status: CheckStatusFail, Detail: keeperDetail})
	}

	rows, replicasDetail, replicasOK := s.checkReplicas(ctx, request, timeout)
	report.Checks = append(report.Checks, HealthCheck{Name: "replicas", Status: passFail(replicasOK), Detail: replicasDetail})

	if !request.CheckTables {
		report.Checks = append(report.Checks, HealthCheck{Name: "tables", Status: CheckStatusSkip, Detail: "no -spec given"})
	} else {
		expected := s.expectedTables()
		report.ExpectedTables = expected
		tablesDetail, tablesOK := s.checkTables(ctx, request, timeout, expected)
		report.Checks = append(report.Checks, HealthCheck{Name: "tables", Status: passFail(tablesOK), Detail: tablesDetail})
	}

	if request.SelfName == "" {
		report.Checks = append(report.Checks, HealthCheck{Name: "self", Status: CheckStatusSkip, Detail: "no -self given"})
	} else {
		selfDetail, selfOK := checkSelf(request.SelfName, rows, replicasOK)
		report.Checks = append(report.Checks, HealthCheck{Name: "self", Status: passFail(selfOK), Detail: selfDetail})
	}

	for _, check := range report.Checks {
		if check.Status == CheckStatusFail {
			report.Status = ReportStatusDegraded
			break
		}
	}
	return report, nil
}

// passFail maps a bool to pass/fail.
func passFail(ok bool) string {
	if ok {
		return CheckStatusPass
	}
	return CheckStatusFail
}

// checkServers fans out over the full inventory: EVERY host is pinged
// and every result reported. A cluster health command that only pinged
// the first reachable host would echo the load balancer's blind spot —
// a down reader behind a healthy writer would pass. Unreachable hosts
// fail their line; the rest still run.
func (s *HealthService) checkServers(ctx context.Context, request HealthRequest, timeout time.Duration) (string, bool) {
	var failures []string
	okCount := 0
	for _, host := range request.Hosts {
		_, err := s.cluster.PingHost(ctx, port.HealthPingRequest{
			Host:        host,
			ClusterName: request.ClusterName,
			Credentials: request.Credentials,
			HTTPPort:    request.HTTPPort,
			Secure:      request.Secure,
			Timeout:     timeout,
		})
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", host.Name, err))
			continue
		}
		okCount++
	}
	if len(failures) > 0 {
		return fmt.Sprintf("%d/%d hosts up; down: %s", okCount, len(request.Hosts), strings.Join(failures, "; ")), false
	}
	return fmt.Sprintf("%d/%d hosts up", okCount, len(request.Hosts)), true
}

// checkKeeper probes mntr when an address was given: reachable AND a
// non-empty zk_server_state is a pass (leader/follower/standalone is
// reported, not judged — a healthy ensemble HAS followers; standalone is
// suspicious for a 3-node ensemble but is the normal state for one, so
// the check reports the state and lets the operator read it).
func (s *HealthService) checkKeeper(ctx context.Context, request HealthRequest, timeout time.Duration) (detail string, ok bool, skipped bool) {
	if request.KeeperAddress == "" {
		return "no -keeper given", false, true
	}
	mntr, err := s.keeper.Mntr(ctx, port.MntrRequest{Address: request.KeeperAddress})
	if err != nil {
		return fmt.Sprintf("mntr %s: %v", request.KeeperAddress, err), false, false
	}
	if mntr.ServerState == "" {
		return fmt.Sprintf("mntr %s: empty zk_server_state", request.KeeperAddress), false, false
	}
	return fmt.Sprintf("%s (%s)", mntr.ServerState, request.KeeperAddress), true, false
}

// checkReplicas reads system.replicas across the cluster (the fan-out
// runs on the first inventory host — clusterAllReplicas reaches every
// node from any one of them, and skip_unavailable_shards keeps a down
// peer from failing the sweep; down peers are already named by the
// server check). Any is_readonly row is a failure naming the table and
// replica.
func (s *HealthService) checkReplicas(ctx context.Context, request HealthRequest, timeout time.Duration) ([]port.HealthReplicaRow, string, bool) {
	var lastErr error
	var rows []port.HealthReplicaRow
	for _, host := range request.Hosts {
		rows, lastErr = s.cluster.QueryReplicas(ctx, port.HealthReplicasRequest{
			RunOn:       host,
			ClusterName: request.ClusterName,
			Credentials: request.Credentials,
			HTTPPort:    request.HTTPPort,
			Secure:      request.Secure,
			Timeout:     timeout,
		})
		if lastErr == nil {
			break
		}
	}
	if lastErr != nil {
		return nil, fmt.Sprintf("query failed on all %d inventory hosts: %v", len(request.Hosts), lastErr), false
	}

	var readonly []string
	for _, row := range rows {
		if row.Readonly {
			readonly = append(readonly, fmt.Sprintf("%s.%s replica=%s host=%s", row.Database, row.Table, row.Replica, row.Host))
		}
	}
	if len(readonly) > 0 {
		sort.Strings(readonly)
		return rows, fmt.Sprintf("%d readonly table(s): %s", len(readonly), strings.Join(readonly, "; ")), false
	}
	return rows, fmt.Sprintf("%d replicated table(s), none readonly", len(rows)), true
}

// checkTables compares the spec's expected tables against existing
// tables. The union across reachable hosts is the source of truth: a
// table existing on ANY host is "present" (schema sync materializes
// per-host; a fresh reader may still be catching up, which the self
// check covers). Missing on all hosts is the degraded state the
// tables check exists to catch.
func (s *HealthService) checkTables(ctx context.Context, request HealthRequest, timeout time.Duration, expected []domain.TableRef) (string, bool) {
	if len(expected) == 0 {
		return "no tables declared in the spec (nothing to check)", true
	}

	existing := make(map[string]bool)
	var reachable []string
	for _, host := range request.Hosts {
		tables, err := s.cluster.QueryTables(ctx, port.HealthTablesRequest{
			Host:        host,
			Credentials: request.Credentials,
			HTTPPort:    request.HTTPPort,
			Secure:      request.Secure,
			Timeout:     timeout,
		})
		if err != nil {
			continue
		}
		reachable = append(reachable, host.Name)
		for _, table := range tables {
			existing[table.String()] = true
		}
	}
	if len(reachable) == 0 {
		return "no reachable host to list tables from", false
	}

	var missing []string
	for _, table := range expected {
		if !existing[table.String()] {
			missing = append(missing, table.String())
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Sprintf("missing %d of %d expected table(s) on %d host(s): %s",
			len(missing), len(expected), len(reachable), strings.Join(missing, ", ")), false
	}
	return fmt.Sprintf("%d expected table(s) present on %d host(s)", len(expected), len(reachable)), true
}

// checkSelf verifies the -self replica is registered and active — the
// post-boot gate for a fresh reader: schema sync may have created
// tables, but until the replica's registration shows up in
// system.replicas the node is not serving replication. It consumes the
// replicas fan-out rows (cluster-wide, host_name tagged), so the full
// inventory stays valid. Inactive registrations (a restarted node
// mid-resync) fail too — the runbook wants "this node participates",
// not "this node is known". When the replicas fan-out itself failed,
// the check reports that instead of double-counting the same failure.
func checkSelf(selfName string, rows []port.HealthReplicaRow, replicasOK bool) (string, bool) {
	if !replicasOK {
		return fmt.Sprintf("cannot verify — replicas query failed (%d rows)", len(rows)), false
	}
	var mine []port.HealthReplicaRow
	for _, row := range rows {
		if row.Replica == selfName {
			mine = append(mine, row)
		}
	}
	if len(mine) == 0 {
		return fmt.Sprintf("replica %q not registered in system.replicas", selfName), false
	}
	var inactive []string
	for _, row := range mine {
		if !row.Active {
			inactive = append(inactive, fmt.Sprintf("%s.%s", row.Database, row.Table))
		}
	}
	if len(inactive) > 0 {
		sort.Strings(inactive)
		return fmt.Sprintf("replica %q registered but inactive for %d table(s): %s",
			selfName, len(inactive), strings.Join(inactive, ", ")), false
	}
	return fmt.Sprintf("replica %q registered and active (%d table(s))", selfName, len(mine)), true
}
