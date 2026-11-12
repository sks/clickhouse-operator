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

// Package port declares the boundary interfaces (ports) that the chctl
// domain services depend on. Adapters in pkg/chctl/infra implement them.
//
// Every method follows the two-parameter pattern: context first, a request
// struct second. Interfaces here stay free of infrastructure types — no
// cloud SDKs — so the domain never bends to any adapter's shape.
package port

import (
	"context"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
)

// SpecLoader parses and normalizes a raw cluster spec into an
// operator-ready CR (the concrete type stays behind the port; services only
// need "a normalized spec" from it).
type SpecLoader interface {
	// LoadCHI parses, defaults and runtime-wires a CHI spec document.
	LoadCHI(ctx context.Context, request LoadCHIRequest) (any, error)
	// LoadCHK parses, defaults and runtime-wires a CHK spec document.
	LoadCHK(ctx context.Context, request LoadCHKRequest) (any, error)
}

// LoadCHIRequest is the input to SpecLoader.LoadCHI.
type LoadCHIRequest struct {
	// Spec names the document and carries its bytes.
	Spec domain.SpecReference
}

// LoadCHKRequest is the input to SpecLoader.LoadCHK.
type LoadCHKRequest struct {
	// Spec names the document and carries its bytes.
	Spec domain.SpecReference
}

// ConfigRenderer renders a normalized spec into per-node configuration
// files (the ConfigMap content, written to disk instead).
type ConfigRenderer interface {
	// RenderCHI writes the common, users and (when a host is selected)
	// host config files for a normalized CHI.
	RenderCHI(ctx context.Context, request RenderCHIRequest) ([]RenderedFile, error)
	// RenderCHK writes the common and host config files for a normalized
	// CHK.
	RenderCHK(ctx context.Context, request RenderCHKRequest) ([]RenderedFile, error)
}

// RenderCHIRequest is the input to ConfigRenderer.RenderCHI.
type RenderCHIRequest struct {
	// NormalizedCR is the SpecLoader's output (an operator CR behind
	// the port boundary; adapters type-assert it).
	NormalizedCR any
	// HostName selects the host whose host-scoped files are rendered;
	// empty renders only the common and users groups.
	HostName string
	// OutputDir is the directory files are written to.
	OutputDir string
	// UsersDir overrides the output directory for the users group when
	// non-empty (e.g. /etc/clickhouse-server/users.d).
	UsersDir string
	// DryRun skips writing; rendered files are still returned.
	DryRun bool
}

// RenderCHKRequest is the input to ConfigRenderer.RenderCHK.
type RenderCHKRequest struct {
	// NormalizedCR is the SpecLoader's keeper CR output.
	NormalizedCR any
	// HostName selects the keeper node whose server section is rendered.
	HostName string
	// OutputDir is the directory files are written to.
	OutputDir string
	// DryRun skips writing; rendered files are still returned.
	DryRun bool
}

// RenderedFile is one rendered configuration file.
type RenderedFile struct {
	// Group is which files group produced this file ("common", "users",
	// "host").
	Group string
	// Path is the file's destination path (relative name of the
	// generated section file).
	Path string
	// Content is the rendered file body (XML unless the spec named a
	// non-XML file).
	Content string
}

// SchemaRunner runs schema/replica lifecycle SQL against cluster hosts via
// the operator's schemer.
type SchemaRunner interface {
	// SyncSchema creates all replicated and distributed objects on a host
	// from cluster peers (HostCreateTables).
	SyncSchema(ctx context.Context, request domain.SchemaSyncRequest) error
	// DropReplica removes a dead replica's Keeper registration
	// (HostDropReplica — SYSTEM DROP REPLICA).
	DropReplica(ctx context.Context, request domain.ReplicaDropRequest) error
	// SyncReplica re-queues replication on a restarted node
	// (HostSyncTables — SYSTEM SYNC REPLICA).
	SyncReplica(ctx context.Context, request domain.ReplicaSyncRequest) error
}

// SchemaClientFactory builds SchemaRunner clients from credentials and the
// host inventory. It is itself a port: services must not construct the
// underlying client directly.
type SchemaClientFactory interface {
	// NewClient builds a SchemaRunner bound to a cluster's inventory.
	NewClient(ctx context.Context, request NewSchemaClientRequest) (SchemaRunner, error)
}

// NewSchemaClientRequest is the input to SchemaClientFactory.NewClient.
type NewSchemaClientRequest struct {
	// ClusterName is the ClickHouse cluster name inside the spec
	// (remote_servers naming).
	ClusterName string
	// Credentials connect to the target hosts.
	Credentials domain.Credentials
	// Hosts is the full cluster inventory; operations address hosts by
	// Name.
	Hosts []domain.HostIdentity
	// HTTPPort is the HTTP interface port used for all hosts (default
	// 8123).
	HTTPPort int
	// Secure marks whether the cluster requires TLS connections.
	Secure bool
	// NormalizedCR is a normalized CHI (from SpecLoader.LoadCHI). The
	// schemer walks the CR's cluster/shard structure for replica naming —
	// hand-built Host values panic in the operator's namer (GetShard
	// dereferences the CR). When nil, hosts are matched against the CR's
	// normalized host list.
	NormalizedCR any
}

// KeeperProbe reads live Raft state from a Keeper ensemble member.
type KeeperProbe interface {
	// Mntr queries the mntr endpoint and returns the parsed result.
	Mntr(ctx context.Context, request MntrRequest) (MntrResult, error)
}

// MntrRequest is the input to KeeperProbe.Mntr.
type MntrRequest struct {
	// Address is the keeper's tcp_interface address (host:port).
	Address string
}

// MntrResult is the parsed output of the mntr four-letter-word command.
type MntrResult struct {
	// ServerState is "leader", "follower" or "standalone".
	ServerState string
	// SyncedFollowers is the leader's count of synced followers; 0 on
	// followers and standalone.
	SyncedFollowers int
	// VotingMembers is the configured voting membership size.
	VotingMembers int
}

// SyncedCount returns the live Raft participants this node knows about.
// On the leader it is syncedFollowers + 1; on a follower or standalone
// node, 1 (a follower answers mntr only while synced with the leader).
func (m MntrResult) SyncedCount() int {
	switch {
	case m.ServerState == "leader":
		return m.SyncedFollowers + 1
	case m.ServerState == "standalone":
		return 1
	default:
		return 1
	}
}

// SecretSource resolves secret values by reference.
type SecretSource interface {
	// GetSecretValue returns one secret value by reference.
	GetSecretValue(ctx context.Context, request SecretRequest) (string, error)
}

// SecretRequest is the input to SecretSource.GetSecretValue.
type SecretRequest struct {
	// Name identifies the secret (cloud-store-specific id).
	Name string
	// Field selects a key inside a structured secret; ignored for
	// single-value secrets.
	Field string
}

// LifecycleAcker acknowledges cloud lifecycle actions once a node is safe
// to stop.
type LifecycleAcker interface {
	// Continue acknowledges the pending lifecycle action for this node.
	// ASG hooks, VMSS terminate notifications and MIG actions all have
	// different ack protocols; the safety service only knows "release
	// the node when quorum allows".
	Continue(ctx context.Context, request ContinueRequest) error
}

// ContinueRequest is the input to LifecycleAcker.Continue.
type ContinueRequest struct{}
