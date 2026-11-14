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

package operator

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// HealthProbe implements port.ClusterHealthProbe over the operator's
// clickhouse cluster client — the same connection machinery the schemer
// paths use (ClusterConnectionParams over the cloud inventory address,
// QueryAny against one host). All SQL lives here; the service layer
// only sees rows and errors.
type HealthProbe struct{}

// NewHealthProbe builds the operator-backed cluster health probe.
func NewHealthProbe() *HealthProbe {
	return &HealthProbe{}
}

// healthTimeout applies the per-probe default (5s).
func healthTimeout(timeout time.Duration) time.Duration {
	if timeout == 0 {
		return 5 * time.Second
	}
	return timeout
}

// healthConnection builds the connection params for one request's
// scheme/credentials/port — identical to the schemer's params so a
// health check exercises the same path the mutating commands do.
func healthConnection(secure bool, httpPort int, creds domain.Credentials) *clickhouse.ClusterConnectionParams {
	scheme := v1.ChSchemeHTTP
	if secure {
		scheme = v1.ChSchemeHTTPS
	}
	if httpPort == 0 {
		httpPort = 8123
	}
	return clickhouse.NewClusterConnectionParams(scheme, creds.Username, creds.Password, "", httpPort)
}

// queryHost runs one SQL statement against one inventory host and
// returns the query result. The cluster is built per call: the health
// command is a read-only fan-out, and a shared pool would tie probe
// failures across hosts (QueryAny's walk must see exactly one host).
func (p *HealthProbe) queryHost(ctx context.Context, address string, params *clickhouse.ClusterConnectionParams, timeout time.Duration, sql string) (*clickhouse.QueryResult, error) {
	cluster := clickhouse.NewCluster()
	cluster.ClusterConnectionParams = params
	cluster.SetHosts([]string{address})
	ctx, cancel := context.WithTimeout(ctx, healthTimeout(timeout))
	defer cancel()
	return cluster.QueryAny(ctx, sql)
}

// PingHost answers whether the host serves SQL (SELECT 1 over the
// driver's real connection path — the same probe the pool uses).
func (p *HealthProbe) PingHost(ctx context.Context, request port.HealthPingRequest) (port.HealthHostResult, error) {
	if request.Host.Address == "" {
		return port.HealthHostResult{}, fmt.Errorf("host %q has no address", request.Host.Name)
	}
	params := healthConnection(request.Secure, request.HTTPPort, request.Credentials)
	result, err := p.queryHost(ctx, request.Host.Address, params, request.Timeout, "SELECT 1")
	if err != nil {
		return port.HealthHostResult{}, err
	}
	if result == nil {
		return port.HealthHostResult{}, fmt.Errorf("no response")
	}
	defer result.Close()
	if _, err := result.Int(); err != nil {
		return port.HealthHostResult{}, err
	}
	return port.HealthHostResult{Address: request.Host.Address}, nil
}

// sqlReplicas builds the replication-state fan-out: every row of
// system.replicas across the cluster with the fields the health check
// judges (is_readonly, replica_is_active). skip_unavailable_shards
// tolerates down peers (the server check already names them) — the
// same tolerance the schemer's stale-replica detection carries.
//
// The only interpolated value is the cluster name, whitelisted against
// the loaded CR by the CLI before the check runs (mirroring replica
// gc's gate — ClickHouse has no parameters on table-function names).
func sqlReplicas(cluster string) string {
	return fmt.Sprintf(`
		SELECT host_name, database, table, name, is_readonly, replica_is_active
		FROM clusterAllReplicas('%s', system.replicas)
		SETTINGS skip_unavailable_shards = 1
	`, cluster)
}

// QueryReplicas reads replication state across the cluster from the
// run-on host.
func (p *HealthProbe) QueryReplicas(ctx context.Context, request port.HealthReplicasRequest) ([]port.HealthReplicaRow, error) {
	if request.RunOn.Address == "" {
		return nil, fmt.Errorf("host %q has no address", request.RunOn.Name)
	}
	params := healthConnection(request.Secure, request.HTTPPort, request.Credentials)
	result, err := p.queryHost(ctx, request.RunOn.Address, params, request.Timeout, sqlReplicas(request.ClusterName))
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	defer result.Close()

	var hosts, databases, tables, replicas, readonlys, actives []string
	if err := result.UnzipColumnsAsStrings(&hosts, &databases, &tables, &replicas, &readonlys, &actives); err != nil {
		return nil, err
	}
	rows := make([]port.HealthReplicaRow, 0, len(tables))
	for i := range tables {
		if i >= len(databases) {
			break
		}
		host, database, table := columnAt(hosts, i), columnAt(databases, i), columnAt(tables, i)
		rows = append(rows, port.HealthReplicaRow{
			Host:     host,
			Database: database,
			Table:    table,
			Replica:  columnAt(replicas, i),
			Readonly: columnAt(readonlys, i) == "1",
			Active:   columnAt(actives, i) == "1",
		})
	}
	return rows, nil
}

// sqlTables lists the user tables on the host (excluding system
// databases — the health check compares against spec-declared user
// tables only).
func sqlTables() string {
	return `
		SELECT database, name
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
	`
}

// QueryTables reads existing table names on one host.
func (p *HealthProbe) QueryTables(ctx context.Context, request port.HealthTablesRequest) ([]domain.TableRef, error) {
	if request.Host.Address == "" {
		return nil, fmt.Errorf("host %q has no address", request.Host.Name)
	}
	params := healthConnection(request.Secure, request.HTTPPort, request.Credentials)
	result, err := p.queryHost(ctx, request.Host.Address, params, request.Timeout, sqlTables())
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, nil
	}
	defer result.Close()

	var databases, names []string
	if err := result.UnzipColumnsAsStrings(&databases, &names); err != nil {
		return nil, err
	}
	tables := make([]domain.TableRef, 0, len(names))
	for i := range names {
		tables = append(tables, domain.TableRef{Database: columnAt(databases, i), Name: names[i]})
	}
	return tables, nil
}

// columnAt reads index i of a TSV column, tolerating a driver that
// returned fewer values than columns (empty string, never a panic).
func columnAt(column []string, i int) string {
	if i < 0 || i >= len(column) {
		return ""
	}
	return column[i]
}

// createObjectRe matches the object name a DDL statement creates. Two
// name forms, both optional-backtick: qualified (db.name) and bare
// (name). Alternation is anchored by the non-capturing group around
// the whole name so the two forms cannot mix.
var createObjectRe = regexp.MustCompile(`(?is)create\s+(?:or\s+replace\s+)?(?:materialized\s+view|live\s+view|window\s+view|table|view|dictionary)\s+(?:if\s+not\s+exists\s+)?` +
	"(?:" + backtickNameRe + `\s*\.\s*` + backtickNameRe + `|` + backtickNameRe + ")")

// backtickNameRe matches one identifier, backticked or bare: group 1/2
// hold the backticked and bare forms (used twice per alternation — the
// qualified form consumes the first two groups, the bare form the
// third; see createObjectName).
const backtickNameRe = "(?:`([^`]+)`|([a-zA-Z0-9_]+))"

// createDatabaseRe matches CREATE DATABASE [IF NOT EXISTS] name —
// used to SKIP database statements (databases are not tables to check
// presence of; a missing database surfaces as its tables missing).
var createDatabaseRe = regexp.MustCompile(`(?is)create\s+database\s+(?:if\s+not\s+exists\s+)?` + backtickNameRe)

// ExpectedTablesFromCR derives the tables the spec declares from the
// normalized CR's files section: every files entry ending in .sql is
// schema DDL (the operator's own initdb pattern — SQL files mounted
// into /docker-entrypoint-initdb.d), and every table/view/dictionary
// object created there is a table the cluster is expected to hold
// after schema sync.
//
// Why the files section: the CHI CR has no dedicated table/DDL
// declaration — specs that create schema carry .sql files (the same
// pattern docs/chi-examples/20-engines-01 uses), and the stack's
// chctl-based bootstrap relies on schema sync from peers; the check
// verifies the RESULT matches what the spec intends.
func ExpectedTablesFromCR(cr any) []domain.TableRef {
	chi, ok := cr.(*v1.ClickHouseInstallation)
	if !ok || chi == nil {
		return nil
	}
	files := chi.GetSpecT().GetConfiguration().GetFiles()
	if files == nil {
		return nil
	}
	tables := make(map[string]bool)
	files.WalkKeys(func(key string, setting *v1.Setting) {
		// File paths in the files section may carry a directory prefix
		// (initdb/init.sql) — only the .sql suffix and the content
		// matter here.
		if !strings.HasSuffix(key, ".sql") || !setting.IsScalar() {
			return
		}
		for _, name := range parseCreatedTableNames(setting.ScalarString()) {
			tables[name] = true
		}
	})
	return sortedTableRefs(tables)
}

// parseCreatedTableNames extracts every created table-ish object name
// from one SQL document. Names keep their qualifier: db.name when
// qualified, bare name when not (the default database — ClickHouse's
// own resolution). CREATE DATABASE statements are recognized and
// skipped: a database is not a table whose presence the check verifies.
//
// Statement splitting is ; -based with -- comment stripping. That is
// exact enough for the spec files the operator ecosystem carries; a
// CREATE inside a string or block comment would need to also parse as
// a statement to register, and a phantom name errs toward reporting a
// MISSING table — operator-visible, never silently green.
func parseCreatedTableNames(sql string) []string {
	names := make(map[string]bool)
	for _, statement := range splitSQLStatements(sql) {
		if createDatabaseRe.MatchString(statement) {
			continue
		}
		m := createObjectRe.FindStringSubmatch(statement)
		if m == nil {
			continue
		}
		// Groups: qualified form is db(1|2).name(3|4); bare form is
		// name(5|6) — the alternation consumed the first name twice
		// only in the qualified case.
		if database := firstGroup(m, 1, 2); database != "" {
			if name := firstGroup(m, 3, 4); name != "" {
				names[database+"."+name] = true
			}
			continue
		}
		if name := firstGroup(m, 5, 6); name != "" {
			names[name] = true
		}
	}
	result := make([]string, 0, len(names))
	for name := range names {
		result = append(result, name)
	}
	sort.Strings(result)
	return result
}

// sortedTableRefs builds the sorted TableRef list from a name set
// (qualified names split on the dot; bare names keep the empty
// database).
func sortedTableRefs(names map[string]bool) []domain.TableRef {
	result := make([]domain.TableRef, 0, len(names))
	for name := range names {
		database, table, hasDot := strings.Cut(name, ".")
		if !hasDot {
			result = append(result, domain.TableRef{Name: name})
			continue
		}
		result = append(result, domain.TableRef{Database: database, Name: table})
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Database != result[j].Database {
			return result[i].Database < result[j].Database
		}
		return result[i].Name < result[j].Name
	})
	return result
}

// splitSQLStatements splits a SQL document into ; -separated
// statements, stripping -- comments first (a trailing comment would
// otherwise glue onto the last statement).
func splitSQLStatements(sql string) []string {
	var statements []string
	for _, raw := range strings.Split(sql, ";") {
		var lines []string
		for _, line := range strings.Split(raw, "\n") {
			if idx := strings.Index(line, "--"); idx >= 0 {
				line = line[:idx]
			}
			lines = append(lines, line)
		}
		statement := strings.TrimSpace(strings.Join(lines, "\n"))
		if statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

// firstGroup returns the first non-empty capture group among indexes.
func firstGroup(m []string, indexes ...int) string {
	for _, idx := range indexes {
		if idx < len(m) && m[idx] != "" {
			return m[idx]
		}
	}
	return ""
}
