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
	"strings"
	"time"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/swversion"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/schemer"
	"github.com/altinity/clickhouse-operator/pkg/model/clickhouse"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
)

// SchemaRunner implements port.SchemaRunner over the operator's
// ClusterSchemer.
type SchemaRunner struct {
	// schemer is the operator client.
	schemer *schemer.ClusterSchemer
	// params are the cluster connection parameters (scheme, credentials,
	// port) for direct-SQL paths (DropReplica).
	params *clickhouse.ClusterConnectionParams
	// hosts maps cluster host names to operator Host values.
	hosts map[string]*v1.Host
}

// SchemaClientFactory implements port.SchemaClientFactory building
// operator-backed runners.
type SchemaClientFactory struct{}

// NewSchemaClientFactory builds the factory.
func NewSchemaClientFactory() *SchemaClientFactory {
	return &SchemaClientFactory{}
}

// defaultSchemaTimeout bounds one schema operation (the operator's
// HostSyncTables query timeout is 120s; creation is bounded the same way).
const defaultSchemaTimeout = 120 * time.Second

// NewClient builds a SchemaRunner bound to a cluster's inventory.
//
// Why the normalized CR: the operator's schemer names replicas by walking
// the CR's cluster/shard tree (GetShard dereferences the CR — hand-built
// Host values panic in its namer). The CR's normalized hosts carry the
// full runtime addressing; cloud addresses from the inventory override
// their FQDNs so connections go where the operator's pod-FQDNs cannot.
func (f *SchemaClientFactory) NewClient(ctx context.Context, request port.NewSchemaClientRequest) (port.SchemaRunner, error) {
	bootstrapChop()

	cr, ok := request.NormalizedCR.(*v1.ClickHouseInstallation)
	if !ok || cr == nil {
		return nil, fmt.Errorf("normalized CR is required (load the CHI spec first)")
	}
	if request.ClusterName == "" {
		return nil, fmt.Errorf("cluster name is empty")
	}
	if len(request.Hosts) == 0 {
		return nil, fmt.Errorf("host inventory is empty")
	}

	// Address overrides: cloud name=address pairs over the CR's FQDNs.
	addresses := make(map[string]string, len(request.Hosts))
	for _, h := range request.Hosts {
		if h.Name == "" || h.Address == "" {
			return nil, fmt.Errorf("host inventory entry requires Name and Address, got %+v", h)
		}
		addresses[h.Name] = h.Address
	}

	scheme := v1.ChSchemeHTTP
	if request.Secure {
		scheme = v1.ChSchemeHTTPS
	}
	httpPort := request.HTTPPort
	if httpPort == 0 {
		httpPort = 8123
	}

	params := clickhouse.NewClusterConnectionParams(
		scheme,
		request.Credentials.Username,
		request.Credentials.Password,
		"",
		httpPort,
	)

	// Cloud-address namer: every schemer ExecHost path resolves the exec
	// host through the namer's pod-FQDN pattern (chi-...svc.cluster.local,
	// unresolvable off-cluster); the wrapped namer translates FQDN output
	// to the cloud addresses from the inventory. Replica names and every
	// other form pass through operator-native (must match table paths).
	// BOTH embedded levels need the override: ExecHost resolves via the
	// embedded schemer.Cluster (whose own INameManager field is what
	// Names() promotes), so setting only the schemer's embedded field
	// leaves the Cluster's default namer active — the live "no such host"
	// finding that proved the deeper embedding.
	s := schemer.NewClusterSchemer(params, swversion.NewSoftWareVersion("24.3"))
	namer := newCloudNamer(addresses)
	s.INameManager = namer
	s.Cluster.INameManager = namer

	runner := &SchemaRunner{
		schemer: s,
		params:  params,
		hosts:   make(map[string]*v1.Host),
	}
	// Collect the CR's normalized hosts for the requested cluster and
	// re-point their FQDNs at the cloud addresses. Unmatched inventory
	// names are an error: a host the CR does not know cannot have
	// operator-consistent replica naming.
	errs := cr.WalkHosts(func(host *v1.Host) error {
		if host.Runtime.Address.ClusterName != request.ClusterName {
			return nil
		}
		address, found := addresses[host.Runtime.Address.HostName]
		if !found {
			return nil // inventory may be a subset (single-node ops)
		}
		host.Runtime.Address.FQDN = address
		runner.hosts[host.Runtime.Address.HostName] = host
		return nil
	})
	if err := firstErr(errs); err != nil {
		return nil, fmt.Errorf("walk hosts: %v", err)
	}
	if len(runner.hosts) == 0 {
		return nil, fmt.Errorf("no cluster %q hosts matched the inventory (CR has the cluster? names match?)",
			request.ClusterName)
	}
	return runner, nil
}

// SyncSchema creates all replicated and distributed objects on a host from
// cluster peers (HostCreateTables).
//
// Without it: a scaled-in reader serves queries with missing tables.
func (r *SchemaRunner) SyncSchema(ctx context.Context, request domain.SchemaSyncRequest) error {
	host, err := r.host(request.HostName)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, defaultSchemaTimeout)
	defer cancel()
	return r.schemer.HostCreateTables(ctx, host)
}

// DropReplica removes a dead replica's Keeper registration
// (HostDropReplica — SYSTEM DROP REPLICA).
//
// Why not schemer.HostDropReplica directly: it runs the SQL via ExecHost,
// which resolves the run-on host through the operator's pod-FQDN pattern
// (chi-...svc.cluster.local) — unresolvable off-cluster.
//
// Replica naming: SYSTEM DROP REPLICA takes the replica_path name that
// tables were CREATED with. On Kubernetes that is the operator's
// instance-hostname (chi-<chi>-<cluster>-<s>-<r>); on VM/EC2 stacks it is
// whatever the deployment's macros.xml declares — the host's cloud name
// (writer-0, reader-0) in the AWS stack. The inventory name IS the cloud
// name here, so it is used directly; no guessing between conventions.
// The DATABASE-replica shard index follows the operator's HostDropReplica
// (the run-on host's shard index).
func (r *SchemaRunner) DropReplica(ctx context.Context, request domain.ReplicaDropRequest) error {
	// hostToDrop validates inventory membership; the drop name is the
	// request's HostName (the cloud replica name — see the naming note
	// above).
	if _, err := r.host(request.HostName); err != nil {
		return err
	}
	hostToRunOn, err := r.host(request.RunOn)
	if err != nil {
		return err
	}

	replicaName := request.HostName
	shardIndex := hostToRunOn.Runtime.Address.ShardIndex

	dropSQLs := []string{
		fmt.Sprintf("SYSTEM DROP REPLICA '%s'", replicaName),
		fmt.Sprintf("SYSTEM DROP DATABASE REPLICA '%d|%s'", shardIndex, replicaName),
	}

	cluster := clickhouse.NewCluster()
	cluster.ClusterConnectionParams = r.params
	cluster.SetHosts([]string{hostToRunOn.Runtime.Address.FQDN})
	opts := clickhouse.NewQueryOptions().SetRetry(false).SetLogQueries(true)
	return cluster.ExecAll(ctx, dropSQLs, opts)
}

// SyncReplica re-queues replication on a restarted node
// (HostSyncTables — SYSTEM SYNC REPLICA).
//
// Without it: a restarted node can lag behind with empty replication queues.
func (r *SchemaRunner) SyncReplica(ctx context.Context, request domain.ReplicaSyncRequest) error {
	host, err := r.host(request.HostName)
	if err != nil {
		return err
	}
	return r.schemer.HostSyncTables(ctx, host)
}

// host resolves a host by CR name (e.g. "0-0") or by the cloud name the
// inventory carries ("writer-0") — the runner keys by CR name but the
// inventory names map onto them, so both forms address the same Host.
func (r *SchemaRunner) host(name string) (*v1.Host, error) {
	if host, ok := r.hosts[name]; ok {
		return host, nil
	}
	for _, host := range r.hosts {
		if host.Runtime.Address.FQDN == name || strings.Split(host.Runtime.Address.FQDN, ".")[0] == name {
			return host, nil
		}
	}
	return nil, fmt.Errorf("host %q not in inventory", name)
}
