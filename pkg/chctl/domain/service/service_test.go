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
	"errors"
	"testing"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"

	"github.com/stretchr/testify/require"
)

// fakeProbe is a hand-rolled KeeperProbe stub (the port's surface is two
// methods, so an inline stub keeps these tests readable).
type fakeProbe struct {
	result port.MntrResult
	err    error
}

func (f *fakeProbe) Mntr(ctx context.Context, request port.MntrRequest) (port.MntrResult, error) {
	return f.result, f.err
}

// fakeSchemaRunner is a hand-rolled SchemaRunner stub recording the GC
// requests it served — the same inline-stub style as fakeProbe.
type fakeSchemaRunner struct {
	// stale is the list FindStaleReplicas returns.
	stale []string
	// detectErr makes FindStaleReplicas fail.
	detectErr error
	// dropErrs maps host names to drop errors; unlisted hosts drop fine.
	dropErrs map[string]error
	// drops records every drop attempt, in order.
	drops []domain.ReplicaDropRequest
	// syncErrs maps pass numbers (0-based) to the error that pass of
	// SyncSchema returns; passes without an entry succeed.
	syncErrs map[int]error
	// syncs records the schema sync requests served, in order.
	syncs []domain.SchemaSyncRequest
}

func (f *fakeSchemaRunner) SyncSchema(ctx context.Context, request domain.SchemaSyncRequest) error {
	f.syncs = append(f.syncs, request)
	if err, ok := f.syncErrs[len(f.syncs)-1]; ok {
		return err
	}
	return nil
}

func (f *fakeSchemaRunner) DropReplica(ctx context.Context, request domain.ReplicaDropRequest) error {
	f.drops = append(f.drops, request)
	if err, ok := f.dropErrs[request.HostName]; ok {
		return err
	}
	return nil
}

func (f *fakeSchemaRunner) SyncReplica(ctx context.Context, request domain.ReplicaSyncRequest) error {
	return nil
}

func (f *fakeSchemaRunner) FindStaleReplicas(ctx context.Context, request domain.ReplicaGcRequest) ([]string, error) {
	return f.stale, f.detectErr
}

// fakeSchemaFactory returns its one runner from every NewClient call.
type fakeSchemaFactory struct {
	runner *fakeSchemaRunner
}

func (f *fakeSchemaFactory) NewClient(ctx context.Context, request port.NewSchemaClientRequest) (port.SchemaRunner, error) {
	return f.runner, nil
}

// gcTestService wires a service over a fake runner.
func gcTestService(runner *fakeSchemaRunner) (*SchemaService, SchemaRequest) {
	return NewSchemaService(&fakeSchemaFactory{runner: runner}), SchemaRequest{
		ClusterName: "logexplorer",
		NormalizedCR: map[string]any{ // opaque through the port boundary
			"cluster": "logexplorer",
		},
	}
}

func TestQuorumSize(t *testing.T) {
	require.Equal(t, 0, QuorumSize(0))
	require.Equal(t, 1, QuorumSize(1))
	require.Equal(t, 2, QuorumSize(3))
	require.Equal(t, 3, QuorumSize(4))
	require.Equal(t, 3, QuorumSize(5))
}

func TestHasQuorumHeadroom(t *testing.T) {
	require.False(t, HasQuorumHeadroom(2))
	require.True(t, HasQuorumHeadroom(3))
	require.True(t, HasQuorumHeadroom(5))
}

func TestEnsembleSnapshotSafeAfterDisrupt(t *testing.T) {
	cases := []struct {
		name         string
		members      int
		synced       int
		targetSynced bool
		expected     bool
	}{
		// 5-member, all synced: dropping one leaves 4 >= 3.
		{"5 members all synced, drop one", 5, 5, true, true},
		// 3-member, 3 synced: dropping one leaves 2 >= 2 — safe.
		{"3 members all synced, drop one", 3, 3, true, true},
		// 3-member, 2 synced: dropping a synced node leaves 1 < 2.
		{"3 members, 2 synced, drop synced node", 3, 2, true, false},
		// An unsynced target contributes nothing; 2 remaining >= 2.
		{"3 members, 2 synced, drop unsynced node", 3, 2, false, true},
		// Small ensembles skip gating entirely (controller behavior).
		{"2-member ensemble skips gating", 2, 2, true, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			snapshot := EnsembleSnapshot{Members: c.members, Synced: c.synced}
			require.Equal(t, c.expected, snapshot.SafeAfterDisrupt(c.targetSynced))
		})
	}
}

func TestSafeToStopAllowsWithHeadroom(t *testing.T) {
	svc := NewKeeperSafetyService(&fakeProbe{result: port.MntrResult{
		ServerState:     "leader",
		SyncedFollowers: 4,
		VotingMembers:   5,
	}})
	err := svc.SafeToStop(context.Background(), SafeToStopRequest{
		KeeperAddress:  "keeper:9181",
		TargetIsSynced: true,
	})
	require.NoError(t, err)
}

func TestSafeToStopRefusesWhenQuorumWouldBreak(t *testing.T) {
	svc := NewKeeperSafetyService(&fakeProbe{result: port.MntrResult{
		ServerState:     "leader",
		SyncedFollowers: 1,
		VotingMembers:   3,
	}})
	err := svc.SafeToStop(context.Background(), SafeToStopRequest{
		KeeperAddress:  "keeper:9181",
		TargetIsSynced: true,
		WaitTimeout:    time.Nanosecond,
	})
	require.ErrorIs(t, err, ErrNotSafeToDisrupt)
}

func TestSafeToStopSkipsGatingForSmallEnsembles(t *testing.T) {
	svc := NewKeeperSafetyService(&fakeProbe{result: port.MntrResult{
		ServerState:     "leader",
		SyncedFollowers: 0,
		VotingMembers:   2,
	}})
	err := svc.SafeToStop(context.Background(), SafeToStopRequest{
		KeeperAddress:  "keeper:9181",
		TargetIsSynced: true,
	})
	require.NoError(t, err)
}

func TestSafeToStopPropagatesProbeErrors(t *testing.T) {
	svc := NewKeeperSafetyService(&fakeProbe{err: errors.New("dial refused")})
	err := svc.SafeToStop(context.Background(), SafeToStopRequest{
		KeeperAddress: "keeper:9181",
	})
	require.ErrorContains(t, err, "mntr")
}

func TestSafeToStopRejectsEmptyAddress(t *testing.T) {
	svc := NewKeeperSafetyService(&fakeProbe{})
	err := svc.SafeToStop(context.Background(), SafeToStopRequest{})
	require.ErrorContains(t, err, "keeper address is empty")
}

func TestMntrResultSyncedCount(t *testing.T) {
	cases := []struct {
		state     string
		followers int
		expected  int
	}{
		{"leader", 4, 5},
		{"leader", 1, 2},
		{"follower", 3, 1},
		{"standalone", 0, 1},
	}
	for _, c := range cases {
		result := port.MntrResult{ServerState: c.state, SyncedFollowers: c.followers}
		require.Equal(t, c.expected, result.SyncedCount(), c.state)
	}
}

func TestGcReplicaDropsStaleReadersAndSparesWriters(t *testing.T) {
	// The pattern gate: only reader-* hosts are dropped; a stale writer
	// (and, in the field, a keeper) survives the sweep untouched.
	runner := &fakeSchemaRunner{stale: []string{"reader-3", "writer-0"}}
	svc, request := gcTestService(runner)

	result, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{
		RunOn: "writer-0",
	})
	require.NoError(t, err)

	require.Equal(t, []string{"reader-3", "writer-0"}, result.Stale)
	require.Equal(t, []string{"reader-3"}, result.Dropped)
	require.Empty(t, result.Failed)
	// Only the reader was ever asked to drop — the writer must not reach
	// DropReplica.
	require.Len(t, runner.drops, 1)
	require.Equal(t, "reader-3", runner.drops[0].HostName)
	require.Equal(t, "writer-0", runner.drops[0].RunOn)
}

func TestGcReplicaCustomPatternGatesEligibility(t *testing.T) {
	runner := &fakeSchemaRunner{stale: []string{"reader-1", "reader-hot-1", "keeper-2"}}
	svc, request := gcTestService(runner)

	result, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{
		RunOn:       "writer-0",
		NamePattern: "reader-[0-9]",
	})
	require.NoError(t, err)
	require.Equal(t, []string{"reader-1"}, result.Dropped)
	require.Empty(t, result.Failed)
}

func TestGcReplicaExcludeSparesLiveHostsButReportsThem(t *testing.T) {
	// The liveness guard for chinit's writer pass: a host holding a
	// live ordinal lease is excluded, so even when the pattern names it
	// and detection calls it stale it must NOT be dropped — but it
	// stays in Stale so the sweep remains visible in the log.
	runner := &fakeSchemaRunner{stale: []string{"writer-0", "writer-1", "writer-2"}}
	svc, request := gcTestService(runner)

	result, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{
		RunOn:       "writer-0",
		NamePattern: "writer-*",
		ExcludeHosts: []string{
			"writer-0", // this node (the run-on peer is always live)
			"writer-1", // mid-boot re-join, lease renewed 5 minutes ago
		},
	})
	require.NoError(t, err)

	// Detection still reports all three — exclusions never hide
	// staleness from the operator reading the timer log.
	require.Equal(t, []string{"writer-0", "writer-1", "writer-2"}, result.Stale)
	// Only the non-excluded writer drops; the excluded ones never reach
	// DropReplica.
	require.Equal(t, []string{"writer-2"}, result.Dropped)
	require.Empty(t, result.Failed)
	require.Len(t, runner.drops, 1)
	require.Equal(t, "writer-2", runner.drops[0].HostName)
}

func TestGcReplicaExcludeOnlyGatesMatchingHosts(t *testing.T) {
	// An exclude entry for a host the pattern does not name changes
	// nothing: the two gates compose as pattern-then-exclude, and the
	// reader pass with writer excludes keeps its reader-only behavior.
	runner := &fakeSchemaRunner{stale: []string{"reader-3", "reader-4"}}
	svc, request := gcTestService(runner)

	result, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{
		RunOn:        "writer-0",
		NamePattern:  "reader-*",
		ExcludeHosts: []string{"reader-4"},
	})
	require.NoError(t, err)
	require.Equal(t, []string{"reader-3"}, result.Dropped)
	require.Len(t, runner.drops, 1)
}

func TestGcReplicaContinuesOnDropFailure(t *testing.T) {
	// One failing host must not shield the rest from cleanup: the sweep
	// continues and reports both outcomes.
	runner := &fakeSchemaRunner{
		stale:    []string{"reader-1", "reader-2", "reader-3"},
		dropErrs: map[string]error{"reader-2": errors.New("connection refused")},
	}
	svc, request := gcTestService(runner)

	result, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{
		RunOn: "writer-0",
	})
	require.NoError(t, err, "GC must not fail the sweep on per-host drop errors")
	require.Len(t, runner.drops, 3, "every stale reader must be attempted")
	require.Equal(t, []string{"reader-1", "reader-3"}, result.Dropped)
	require.Equal(t, []string{"reader-2"}, result.Failed)
}

func TestGcReplicaFreshClusterIsANoOp(t *testing.T) {
	// Zero detected rows → zero drops, no error: a fresh cluster's GC
	// run must be indistinguishable from success.
	runner := &fakeSchemaRunner{}
	svc, request := gcTestService(runner)

	result, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{
		RunOn: "writer-0",
	})
	require.NoError(t, err)
	require.Empty(t, result.Stale)
	require.Empty(t, result.Dropped)
	require.Empty(t, result.Failed)
	require.Empty(t, runner.drops)
}

func TestGcReplicaRejectsInvalidPattern(t *testing.T) {
	// An unterminated glob class must fail before any SQL runs.
	runner := &fakeSchemaRunner{stale: []string{"reader-1"}}
	svc, request := gcTestService(runner)

	_, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{
		RunOn:       "writer-0",
		NamePattern: "reader-[a-",
	})
	require.ErrorContains(t, err, "invalid name pattern")
	require.Empty(t, runner.drops)
}

func TestGcReplicaRejectsEmptyRunOn(t *testing.T) {
	svc, request := gcTestService(&fakeSchemaRunner{})
	_, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{})
	require.ErrorContains(t, err, "-on peer is empty")
}

func TestGcReplicaPropagatesDetectionError(t *testing.T) {
	runner := &fakeSchemaRunner{detectErr: errors.New("no such host")}
	svc, request := gcTestService(runner)

	_, err := svc.GcReplica(context.Background(), request, domain.ReplicaGcRequest{RunOn: "writer-0"})
	require.ErrorContains(t, err, "no such host")
	require.Empty(t, runner.drops)
}

func TestSyncSchemaOneCleanPass(t *testing.T) {
	// The default behavior: a single clean create pass ends the loop —
	// no second pass, no drop attempts.
	runner := &fakeSchemaRunner{}
	svc, request := gcTestService(runner)

	err := svc.SyncSchema(context.Background(), request, domain.SchemaSyncRequest{
		HostName: "reader-0",
		Retries:  3,
	})
	require.NoError(t, err)
	require.Len(t, runner.syncs, 1, "early stop: one pass must not be followed by more")
	require.Empty(t, runner.drops)
}

func TestSyncSchemaRetriesUntilCleanPass(t *testing.T) {
	// The MV finding: pass 1 fails (the TO-target table lands later in
	// the same batch), pass 2 sees it existing and succeeds. Without the
	// retry loop a manual second run was the only fix.
	runner := &fakeSchemaRunner{syncErrs: map[int]error{
		0: errors.New("Code: 60. Table db.mv does not exist"),
	}}
	svc, request := gcTestService(runner)

	err := svc.SyncSchema(context.Background(), request, domain.SchemaSyncRequest{
		HostName: "reader-0",
		Retries:  3,
	})
	require.NoError(t, err)
	require.Len(t, runner.syncs, 2, "the second pass must run and succeed")
	require.Empty(t, runner.drops, "a non-253 failure never prunes")
}

func TestSyncSchemaLastPassErrorIsReturned(t *testing.T) {
	// Every pass failing returns the LAST pass's error after exactly
	// Retries passes (0 = the default, 2).
	runner := &fakeSchemaRunner{syncErrs: map[int]error{
		0: errors.New("pass one: connection refused"),
		1: errors.New("pass two: connection refused"),
		2: errors.New("pass three must never run"),
	}}
	svc, request := gcTestService(runner)

	err := svc.SyncSchema(context.Background(), request, domain.SchemaSyncRequest{HostName: "reader-0"})
	require.ErrorContains(t, err, "pass two")
	require.Len(t, runner.syncs, 2, "Retries 0 must normalize to the default 2")
}

func TestSyncSchemaDefaultRetries(t *testing.T) {
	// The default loop count is a named constant the flag default binds
	// to — the app flag and the service must not drift apart.
	require.Equal(t, 2, DefaultSchemaSyncRetries)
}

func TestSyncSchemaPruneStaleSelfDropsAndRetries(t *testing.T) {
	// The ASG-replacement finding: the create pass fails with Code 253
	// naming the target's own registration. With the flag set, the stale
	// registration is dropped (the target's own name, run on the target)
	// and the create pass retries — the manual `replica drop` recovery
	// automated.
	runner := &fakeSchemaRunner{syncErrs: map[int]error{
		0: errors.New("Code: 253. Replica /clickhouse/tables/1/logexplorer/replicas/reader-0 already exists"),
	}}
	svc, request := gcTestService(runner)

	err := svc.SyncSchema(context.Background(), request, domain.SchemaSyncRequest{
		HostName:       "reader-0",
		Retries:        2,
		PruneStaleSelf: true,
	})
	require.NoError(t, err, "the retry pass must succeed once the stale znode is reclaimed")
	require.Len(t, runner.syncs, 2)
	require.Len(t, runner.drops, 1)
	require.Equal(t, "reader-0", runner.drops[0].HostName,
		"only the target's own replica name is ever dropped")
	require.Equal(t, "reader-0", runner.drops[0].RunOn,
		"the drop runs on the target itself — HostCreateTables executes there")
}

func TestSyncSchemaPruneStaleSelfUnflaggedNeverDrops(t *testing.T) {
	// The flag is opt-in (destructive-adjacent): the identical 253 error
	// without the flag surfaces immediately, naming the recovery flag —
	// and no drop attempt may ever run.
	runner := &fakeSchemaRunner{syncErrs: map[int]error{
		0: errors.New("Code: 253. Replica /clickhouse/tables/1/logexplorer/replicas/reader-0 already exists"),
	}}
	svc, request := gcTestService(runner)

	err := svc.SyncSchema(context.Background(), request, domain.SchemaSyncRequest{
		HostName: "reader-0",
		Retries:  2,
	})
	require.Error(t, err, "unflagged: the 253 error is returned, never worked around")
	require.ErrorContains(t, err, "-prune-stale-self")
	require.Empty(t, runner.drops, "unflagged: no DropReplica may ever run")
	require.Len(t, runner.syncs, 1, "retrying the same znode is pointless — the error surfaces after the first pass")
}

func TestSyncSchemaPruneStaleSelfSkipsNonRegistrationErrors(t *testing.T) {
	// A failure that is not an already-exists registration (a connection
	// refusal, a missing target table) must never prune, even flagged.
	runner := &fakeSchemaRunner{syncErrs: map[int]error{
		0: errors.New("Code: 60. Table db.mv does not exist"),
		1: errors.New("Code: 60. Table db.mv does not exist"),
	}}
	svc, request := gcTestService(runner)

	err := svc.SyncSchema(context.Background(), request, domain.SchemaSyncRequest{
		HostName:       "reader-0",
		Retries:        2,
		PruneStaleSelf: true,
	})
	require.ErrorContains(t, err, "db.mv")
	require.Empty(t, runner.drops, "a non-253 error must never prune")
}

func TestSyncSchemaPruneStaleSelfDropFailureSurfacesBoth(t *testing.T) {
	// When the prune drop itself fails (keeper unreachable), the error
	// must name the prune AND keep the create failure it was responding
	// to — an operator reading one line has both halves.
	runner := &fakeSchemaRunner{
		syncErrs: map[int]error{
			0: errors.New("Code: 253. Replica /clickhouse/tables/1/logexplorer/replicas/reader-0 already exists"),
		},
		dropErrs: map[string]error{"reader-0": errors.New("keeper unreachable")},
	}
	svc, request := gcTestService(runner)

	err := svc.SyncSchema(context.Background(), request, domain.SchemaSyncRequest{
		HostName:       "reader-0",
		Retries:        2,
		PruneStaleSelf: true,
	})
	require.ErrorContains(t, err, "prune stale self")
	require.ErrorContains(t, err, "keeper unreachable")
	require.ErrorContains(t, err, "already exists")
}

func TestSyncSchemaPruneStaleSelfRespectsCustomRunOn(t *testing.T) {
	// RunOn steers the prune drop to a peer when the caller needs the SQL
	// executed elsewhere (empty RunOn = the target itself).
	runner := &fakeSchemaRunner{syncErrs: map[int]error{
		0: errors.New("Code: 253. Replica /clickhouse/tables/1/logexplorer/replicas/reader-0 already exists"),
	}}
	svc, request := gcTestService(runner)

	err := svc.SyncSchema(context.Background(), request, domain.SchemaSyncRequest{
		HostName:       "reader-0",
		RunOn:          "writer-0",
		Retries:        2,
		PruneStaleSelf: true,
	})
	require.NoError(t, err)
	require.Len(t, runner.drops, 1)
	require.Equal(t, "reader-0", runner.drops[0].HostName)
	require.Equal(t, "writer-0", runner.drops[0].RunOn)
}

func TestStaleSelfRegistration(t *testing.T) {
	// The error-shape extraction: the replica name is the final path
	// segment after /replicas/ in the Code 253 message the driver
	// surfaces, or the quoted replica argument.
	cases := []struct {
		name     string
		err      error
		expected string
		found    bool
	}{
		{
			name:     "the live path shape",
			err:      errors.New("Code: 253. Replica /clickhouse/tables/1/logexplorer/replicas/reader-0 already exists"),
			expected: "reader-0",
			found:    true,
		},
		{
			name:     "quoted replica form",
			err:      errors.New("Code: 253: Replica 'reader-0' already exists"),
			expected: "reader-0",
			found:    true,
		},
		{
			name:     "not a 253",
			err:      errors.New("Code: 60. Table db.mv does not exist"),
			expected: "",
			found:    false,
		},
		{
			name:     "nil error",
			err:      nil,
			expected: "",
			found:    false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			name, found := staleSelfRegistration(c.err)
			require.Equal(t, c.found, found)
			require.Equal(t, c.expected, name)
		})
	}
}
