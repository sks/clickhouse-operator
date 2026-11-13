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

// Package service implements the chctl domain services: use-case
// orchestration and decision logic over the ports declared in
// pkg/chctl/domain/port. It imports no cloud SDKs.
package service

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
)

// ErrNotSafeToDisrupt is returned when disrupting the target keeper would
// drop the ensemble below Raft quorum. Callers (lifecycle hooks) must hold
// the node.
var ErrNotSafeToDisrupt = errors.New("disrupting this keeper would drop the ensemble below Raft quorum")

// FaultTolerantMinMembers mirrors raftFaultTolerantMinMembers from the
// CHK controller's raft-safety gate: below this ensemble size quorum
// gating is meaningless — a 1- or 2-member ensemble cannot survive any
// planned disrupt, so the gate must not pretend otherwise and must allow
// maintenance.
const FaultTolerantMinMembers = 3

// DefaultQuorumPollInterval and DefaultQuorumWaitTimeout mirror the
// controller's defaultQuorumDisruptPollInterval / WaitTimeout.
const (
	DefaultQuorumPollInterval = 5 * time.Second
	DefaultQuorumWaitTimeout  = 2 * time.Minute
)

// QuorumSize is Raft majority for an ensemble of n members (n/2 + 1).
// Extracted from the controller's raftQuorumSize.
func QuorumSize(members int) int {
	if members <= 0 {
		return 0
	}
	return members/2 + 1
}

// HasQuorumHeadroom mirrors the controller's ensembleHasQuorumHeadroom.
func HasQuorumHeadroom(members int) bool {
	return members >= FaultTolerantMinMembers
}

// EnsembleSnapshot captures live Raft state before a disrupt decision.
// Frozen on purpose (the controller does the same): never re-derive after
// the disrupt starts.
type EnsembleSnapshot struct {
	// Members is the configured ensemble size.
	Members int
	// Synced is the count of live Raft participants per Keeper mntr.
	Synced int
}

// SafeAfterDisrupt reports whether the remaining synced members would still
// meet quorum if this host were disrupted. Re-extracted from the
// controller's ensembleQuorumSafeAfterDisrupt, with mntr synced-followers
// replacing STS ReadyReplicas as the live-count source.
func (s EnsembleSnapshot) SafeAfterDisrupt(targetSynced bool) bool {
	if !HasQuorumHeadroom(s.Members) {
		return true
	}
	remaining := s.Synced
	if targetSynced {
		remaining--
	}
	return remaining >= QuorumSize(s.Members)
}

// KeeperSafetyService decides whether a keeper node may be stopped without
// dropping the ensemble below Raft quorum, using a KeeperProbe for live
// state. It is the off-cluster counterpart of the controller's
// ensureQuorumSafeToDisruptHost.
type KeeperSafetyService struct {
	// probe reads live ensemble state.
	probe port.KeeperProbe
}

// NewKeeperSafetyService builds the service over a probe.
func NewKeeperSafetyService(probe port.KeeperProbe) *KeeperSafetyService {
	return &KeeperSafetyService{probe: probe}
}

// SafeToStopRequest is the input to SafeToStop.
type SafeToStopRequest struct {
	// KeeperAddress is the mntr endpoint of the leader or any voting
	// member.
	KeeperAddress string
	// Members is the configured ensemble size (defaults to the mntr
	// voting membership when zero).
	Members int
	// TargetIsSynced marks whether the node being disrupted is currently a
	// synced participant (removing an unsynced node cannot break quorum).
	TargetIsSynced bool
	// PollInterval bounds the wait loop (default 5s).
	PollInterval time.Duration
	// WaitTimeout bounds the total wait (default 2m).
	WaitTimeout time.Duration
}

// SafeToStop polls the ensemble until disrupting the target is safe, the
// wait budget is spent, or the context ends — mirroring the controller's
// ensureQuorumSafeToDisruptHost flow, refusals included: a disrupt that
// would break quorum is never allowed, only deferred.
func (s *KeeperSafetyService) SafeToStop(ctx context.Context, request SafeToStopRequest) error {
	if request.KeeperAddress == "" {
		return fmt.Errorf("keeper address is empty")
	}
	pollInterval := request.PollInterval
	if pollInterval == 0 {
		pollInterval = DefaultQuorumPollInterval
	}
	waitTimeout := request.WaitTimeout
	if waitTimeout == 0 {
		waitTimeout = DefaultQuorumWaitTimeout
	}

	deadline := time.Now().Add(waitTimeout)
	for {
		mntr, err := s.probe.Mntr(ctx, port.MntrRequest{Address: request.KeeperAddress})
		if err != nil {
			return fmt.Errorf("mntr: %w", err)
		}
		members := request.Members
		if members == 0 {
			members = mntr.VotingMembers
		}
		snapshot := EnsembleSnapshot{Members: members, Synced: mntr.SyncedCount()}
		if !HasQuorumHeadroom(members) {
			// Small ensemble: the controller skips gating entirely.
			return nil
		}
		if !request.TargetIsSynced || snapshot.SafeAfterDisrupt(request.TargetIsSynced) {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w (synced=%d quorum=%d members=%d)",
				ErrNotSafeToDisrupt, snapshot.Synced, QuorumSize(members), members)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
			// Re-check: a flapping follower may have re-synced.
		}
	}
}

// SchemaService orchestrates schema/replica operations via a SchemaRunner
// factory.
type SchemaService struct {
	// factory builds runners bound to a cluster inventory.
	factory port.SchemaClientFactory
}

// NewSchemaService builds the service over a runner factory.
func NewSchemaService(factory port.SchemaClientFactory) *SchemaService {
	return &SchemaService{factory: factory}
}

// SchemaRequest binds a schema operation to one cluster context.
type SchemaRequest struct {
	// ClusterName is the ClickHouse cluster name (remote_servers naming).
	ClusterName string
	// Credentials connect to the target hosts.
	Credentials domain.Credentials
	// Hosts is the cluster inventory (operations address hosts by Name).
	Hosts []domain.HostIdentity
	// HTTPPort is the HTTP interface port (default 8123).
	HTTPPort int
	// Secure marks whether the cluster requires TLS connections.
	Secure bool
	// NormalizedCR is the SpecLoader's CHI output. Required: the operator
	// schemer names replicas by walking the CR's cluster/shard tree, and
	// hand-built Host values panic in its namer.
	NormalizedCR any
}

// runner builds a client for one operation.
func (s *SchemaService) runner(ctx context.Context, request SchemaRequest) (port.SchemaRunner, error) {
	return s.factory.NewClient(ctx, port.NewSchemaClientRequest{
		ClusterName:  request.ClusterName,
		Credentials:  request.Credentials,
		Hosts:        request.Hosts,
		HTTPPort:     request.HTTPPort,
		Secure:       request.Secure,
		NormalizedCR: request.NormalizedCR,
	})
}

// DefaultSchemaSyncRetries is the default -retries for schema sync: two
// full create passes. One pass re-runs the operator's per-SQL retry, but a
// materialized view whose TO-target table lands later in the same batch
// stays uncreated until a fresh pass re-orders the batch against the now
// existing targets (the live finding).
const DefaultSchemaSyncRetries = 2

// SyncSchema creates all schema objects on a target host from cluster
// peers — the new-reader boot case (HostCreateTables).
//
// Retries: up to sync.Retries full create passes (0 = DefaultSchemaSyncRetries),
// stopping early on the first clean pass. The operator's ExecHost retry
// re-runs failed SQLs, but ordering across a fresh pass is not guaranteed —
// the retry loop makes the second pass deterministic instead of relying on
// a manual re-run.
//
// PruneStaleSelf: when a pass fails with Code 253 (the target's replica
// registration already exists in Keeper — an ASG/VMSS replacement node
// re-claiming an ordinal whose dead predecessor left a PERSISTENT znode),
// the service runs the DropReplica pair for the failed registration and
// continues the loop. The drop name comes from the error the pass
// reported; the runner's DropReplica refuses any name not in the
// inventory, and the flag is opt-in (default false). Unflagged, the 253
// error returns immediately with the flag named — every retry would hit
// the same znode, so a retry would only burn a pass.
//
// Non-253 failures retry through every pass: the MV whose TO-target
// lands later in the batch is what the NEXT pass fixes.
//
// Without the retry loop: a single-pass failure leaves the node schemaless
// until a human re-runs the command (the live MV finding). Without
// PruneStaleSelf: a recycled ordinal cannot recover its own registration
// at all — CREATE and the auto-ATTACH fallback both fail on a fresh disk,
// and only a manual `replica drop` on a peer unblocks it.
func (s *SchemaService) SyncSchema(ctx context.Context, request SchemaRequest, sync domain.SchemaSyncRequest) error {
	runner, err := s.runner(ctx, request)
	if err != nil {
		return err
	}
	retries := sync.Retries
	if retries <= 0 {
		retries = DefaultSchemaSyncRetries
	}
	runOn := sync.RunOn
	if runOn == "" {
		runOn = sync.HostName
	}

	var lastErr error
	for pass := 0; pass < retries; pass++ {
		lastErr = runner.SyncSchema(ctx, sync)
		if lastErr == nil {
			return nil
		}
		staleName, stale := staleSelfRegistration(lastErr)
		if !stale {
			// A non-253 failure (e.g. an MV whose TO-target table lands
			// later in the batch) is what the NEXT pass fixes — the loop
			// continues; only the last pass's error is returned.
			continue
		}
		if !sync.PruneStaleSelf {
			// The registration already exists and the caller did not
			// opt into reclaiming it: retrying without the drop is
			// pointless (every pass hits the same znode), so the error
			// surfaces immediately with the recovery flag named.
			return fmt.Errorf("%w (pass %d/%d — reclaim it with -prune-stale-self)", lastErr, pass+1, retries)
		}
		// The failed pass named the registration the target could not
		// claim. Drop exactly that one — the runner's DropReplica
		// validates it against the inventory before any SQL runs.
		if err := runner.DropReplica(ctx, domain.ReplicaDropRequest{HostName: staleName, RunOn: runOn}); err != nil {
			return fmt.Errorf("prune stale self: %w (after %w)", err, lastErr)
		}
	}
	return lastErr
}

// staleSelfRegistration extracts the replica name a failed create pass
// reported as already registered (Code 253) — the live error shape is
// Replica /clickhouse/tables/.../replicas/<name> already exists (the
// znode path form), or the quoted Replica '<name>' already exists form
// some drivers wrap it in.
func staleSelfRegistration(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := err.Error()
	if !strings.Contains(msg, "Code: 253") && !strings.Contains(msg, "already exists") {
		return "", false
	}
	for _, marker := range []string{"/replicas/", "'"} {
		if !strings.Contains(msg, marker) {
			continue
		}
		if name := extractReplicaName(msg, marker); name != "" {
			return name, true
		}
	}
	return "", false
}

// extractReplicaName returns the replica name named by the marker's
// segment in msg: for "/replicas/" it is the token after the LAST
// occurrence (the znode path's final component — the path prefix can
// itself contain the marker on deeper trees, while the replica segment
// is always last); for "'" the quoted name between the LAST-but-one
// and the LAST quote (the closing quote of Replica '<name>').
func extractReplicaName(msg, marker string) string {
	if marker == "'" {
		// Quoted form: the name sits between the closing pair of quotes.
		second := strings.LastIndexByte(msg, '\'')
		if second <= 0 {
			return ""
		}
		first := strings.LastIndexByte(msg[:second], '\'')
		if first < 0 {
			return ""
		}
		return msg[first+1 : second]
	}
	idx := strings.LastIndex(msg, marker)
	if idx < 0 {
		return ""
	}
	rest := msg[idx+len(marker):]
	// The replica name ends at the first character that cannot appear
	// in a znode name (the path ends at whitespace or the next path
	// boundary).
	end := strings.IndexAny(rest, " \t\r\n:\"<>")
	if end < 0 {
		end = len(rest)
	}
	name := rest[:end]
	if name == "" || strings.Contains(name, "/") {
		return ""
	}
	return name
}

// DropReplica removes a dead replica's Keeper registration via a live peer
// (HostDropReplica — SYSTEM DROP REPLICA).
func (s *SchemaService) DropReplica(ctx context.Context, request SchemaRequest, drop domain.ReplicaDropRequest) error {
	runner, err := s.runner(ctx, request)
	if err != nil {
		return err
	}
	return runner.DropReplica(ctx, drop)
}

// SyncReplica re-queues replication on a restarted node
// (HostSyncTables — SYSTEM SYNC REPLICA).
func (s *SchemaService) SyncReplica(ctx context.Context, request SchemaRequest, sync domain.ReplicaSyncRequest) error {
	runner, err := s.runner(ctx, request)
	if err != nil {
		return err
	}
	return runner.SyncReplica(ctx, sync)
}

// ReplicaGcResult reports the outcome of a GC sweep so the CLI can print
// the TSV list and the human summary from one pass.
type ReplicaGcResult struct {
	// Stale is every stale host name found (before pattern filtering).
	Stale []string
	// Dropped is every host whose registration was removed successfully.
	Dropped []string
	// Failed is every host whose drop errored (already removed from
	// Dropped).
	Failed []string
}

// DefaultReplicaGcPattern is the eligibility glob for a GC sweep. The
// AWS stack's dead-replica-gc.sh gated on reader-%: only readers are
// disposable; a stale writer or keeper registration must survive a GC
// pass untouched, so the default keeps that gate.
const DefaultReplicaGcPattern = "reader-*"

// GcReplica detects stale replica registrations on a peer and drops them
// in one pass — the dead-replica GC that replaced the weekly bash timer
// on the AWS stack.
//
// The pattern gate is applied here, not in SQL: detection stays a simple
// query (stays easy to reason about), and the eligibility rule lives in
// one place the CLI documents. A host that does not match is reported as
// stale but never dropped.
//
// Drop failures are logged per-host and do not abort the sweep — one
// flaky replica must not shield the rest from cleanup. The caller is
// expected to exit non-zero when Failed is non-empty.
func (s *SchemaService) GcReplica(ctx context.Context, request SchemaRequest, gc domain.ReplicaGcRequest) (*ReplicaGcResult, error) {
	if gc.RunOn == "" {
		return nil, fmt.Errorf("gc: -on peer is empty")
	}
	pattern := gc.NamePattern
	if pattern == "" {
		pattern = DefaultReplicaGcPattern
	}
	// Validate the glob BEFORE any SQL: an invalid pattern (e.g. "[a-")
	// must fail fast as a usage error, not surface as a silent no-op.
	if _, err := path.Match(pattern, ""); err != nil {
		return nil, fmt.Errorf("gc: invalid name pattern %q: %w", pattern, err)
	}
	gc.NamePattern = pattern

	runner, err := s.runner(ctx, request)
	if err != nil {
		return nil, err
	}
	stale, err := runner.FindStaleReplicas(ctx, gc)
	if err != nil {
		return nil, err
	}

	result := &ReplicaGcResult{Stale: stale}
	for _, host := range stale {
		matched, err := path.Match(pattern, host)
		// The pattern was validated above; a per-host match error cannot
		// happen. Being defensive costs one line and never reaps on
		// ambiguity.
		if err != nil || !matched {
			continue
		}
		if err := runner.DropReplica(ctx, domain.ReplicaDropRequest{HostName: host, RunOn: gc.RunOn}); err != nil {
			result.Failed = append(result.Failed, host)
			continue
		}
		result.Dropped = append(result.Dropped, host)
	}
	return result, nil
}
