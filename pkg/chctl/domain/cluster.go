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

// Package domain defines the core entities of the chctl cluster-control
// use case: the cluster spec reference, node identity and credentials.
//
// This is the innermost layer of the chctl package tree: it imports
// nothing from the operator's machinery, from clouds, or from any
// adapter. Outer layers (service, infra, presentation) depend on these
// types; nothing here depends on them.
package domain

// SpecReference names a cluster spec document and how to read it.
type SpecReference struct {
	// Name is the metadata.name of the CHI (or CHK).
	Name string
	// Namespace groups specs per environment (informational off-cluster;
	// it keeps parity with the operator's naming).
	Namespace string
	// Data is the raw spec document bytes.
	Data []byte
}

// Role enumerates a node's cluster role.
type Role string

const (
	// RoleWriter is an ingest-facing ClickHouse server.
	RoleWriter Role = "writer"
	// RoleReader is a query-facing ClickHouse server.
	RoleReader Role = "reader"
	// RoleKeeper is a ClickHouse Keeper (Raft) node.
	RoleKeeper Role = "keeper"
)

// HostIdentity identifies one cluster node, cloud-neutrally. Callers fill
// it from instance metadata or scaling-group listings.
type HostIdentity struct {
	// Cloud is the provider short name ("aws", "azure", "gcp").
	Cloud string
	// Region is the node's cloud region.
	Region string
	// ProviderID is the cloud-native instance/VM ID.
	ProviderID string
	// Name is the hostname the cluster knows the node by (ClickHouse
	// replica naming keys off this).
	Name string
	// Address is the resolvable address (IP or DNS) of the node.
	Address string
	// Role is the node's cluster role.
	Role Role
	// ScalingGroup names the ASG / VMSS / MIG the node belongs to.
	ScalingGroup string
}

// Credentials are ClickHouse connection credentials for control
// operations. Callers resolve them from the cloud secret store at use
// time; they never live in spec files.
type Credentials struct {
	// Username is the ClickHouse user to connect as.
	Username string
	// Password is the plaintext password.
	Password string
}

// SchemaSyncRequest asks the schema service to create all objects on a host.
type SchemaSyncRequest struct {
	// HostName is the node whose schema is (re)created from cluster peers
	// (the new-reader boot case).
	HostName string
}

// ReplicaDropRequest asks the schema service to remove a dead replica's
// registration, executed against a live peer.
type ReplicaDropRequest struct {
	// HostName is the replica being removed.
	HostName string
	// RunOn is the peer that executes SYSTEM DROP REPLICA.
	RunOn string
}

// ReplicaSyncRequest asks the schema service to re-queue replication on a
// restarted node.
type ReplicaSyncRequest struct {
	// HostName is the node that re-queues replication.
	HostName string
}
