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
