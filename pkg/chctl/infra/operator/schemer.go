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
// Why operator Host values: the schemer names replicas exactly as
// ReplicatedMergeTree paths do; building api.Host values here keeps
// replica naming conventions identical to the operator's instead of
// re-deriving them by hand.
func (f *SchemaClientFactory) NewClient(ctx context.Context, request port.NewSchemaClientRequest) (port.SchemaRunner, error) {
	bootstrapChop()

	if request.ClusterName == "" {
		return nil, fmt.Errorf("cluster name is empty")
	}
	if len(request.Hosts) == 0 {
		return nil, fmt.Errorf("host inventory is empty")
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

	runner := &SchemaRunner{
		schemer: schemer.NewClusterSchemer(params, swversion.NewSoftWareVersion("24.3")),
		hosts:   make(map[string]*v1.Host, len(request.Hosts)),
	}
	for _, h := range request.Hosts {
		if h.Name == "" || h.Address == "" {
			return nil, fmt.Errorf("host inventory entry requires Name and Address, got %+v", h)
		}
		host := &v1.Host{Name: h.Name}
		host.Runtime.Address.HostName = h.Name
		host.Runtime.Address.ClusterName = request.ClusterName
		host.Runtime.Address.FQDN = h.Address
		runner.hosts[h.Name] = host
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
// Without it: dead-replica znode garbage accumulates until manual cleanup.
func (r *SchemaRunner) DropReplica(ctx context.Context, request domain.ReplicaDropRequest) error {
	hostToDrop, err := r.host(request.HostName)
	if err != nil {
		return err
	}
	hostToRunOn, err := r.host(request.RunOn)
	if err != nil {
		return err
	}
	return r.schemer.HostDropReplica(ctx, hostToRunOn, hostToDrop)
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

// host resolves a host by name or errors clearly.
func (r *SchemaRunner) host(name string) (*v1.Host, error) {
	host, ok := r.hosts[name]
	if !ok {
		return nil, fmt.Errorf("host %q not in inventory", name)
	}
	return host, nil
}
