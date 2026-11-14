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

package keeper

import (
	"context"
	"net"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"

	"github.com/stretchr/testify/require"
)

// startMntrServer is a minimal TCP server answering 4lw commands with
// canned mntr output, exercising the real wire protocol.
func startMntrServer(t *testing.T, response string) string {
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

func TestProbeMntrParsesLeader(t *testing.T) {
	addr := startMntrServer(t,
		"zk_version\t2.8.5\n"+
			"zk_server_state\tleader\n"+
			"zk_synced_followers\t4\n"+
			"zk_voting_followers\t5\n"+
			"zk_outstanding_requests\t0\n"+
			"zk_znode_count\t42\n")

	probe := NewProbe()
	result, err := probe.Mntr(context.Background(), port.MntrRequest{Address: addr})
	require.NoError(t, err)
	require.Equal(t, "leader", result.ServerState)
	require.Equal(t, 4, result.SyncedFollowers)
	require.Equal(t, 5, result.VotingMembers)
	require.Equal(t, 5, result.SyncedCount())
}

func TestProbeMntrUnreachable(t *testing.T) {
	probe := NewProbe()
	// Reserved TEST-NET address: guaranteed unroutable.
	_, err := probe.Mntr(context.Background(), port.MntrRequest{Address: "192.0.2.1:9181"})
	require.ErrorContains(t, err, "dial")
}

func TestProbeMntrMalformedOutput(t *testing.T) {
	addr := startMntrServer(t, "garbage without tabs\n")

	probe := NewProbe()
	result, err := probe.Mntr(context.Background(), port.MntrRequest{Address: addr})
	require.NoError(t, err)
	require.Equal(t, 1, result.SyncedCount(), "malformed mntr must not panic; zero counts fail loudly")
}

func TestProbeMntrKeepsFullPayload(t *testing.T) {
	// The payload is deliberately not alphabetical and includes keys
	// beyond the three quorum fields: `keeper mntr` prints the full
	// verbatim payload, replacing `echo mntr | nc | awk` parsing.
	addr := startMntrServer(t,
		"zk_server_state\tleader\n"+
			"zk_version\t2.8.5\n"+
			"zk_synced_followers\t4\n"+
			"zk_voting_followers\t5\n"+
			"zk_outstanding_requests\t0\n"+
			"zk_znode_count\t42\n"+
			"garbage without tabs\n")

	probe := NewProbe()
	result, err := probe.Mntr(context.Background(), port.MntrRequest{Address: addr})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"zk_server_state":         "leader",
		"zk_version":              "2.8.5",
		"zk_synced_followers":     "4",
		"zk_voting_followers":     "5",
		"zk_outstanding_requests": "0",
		"zk_znode_count":          "42",
	}, result.Fields)
	require.Equal(t, "leader", result.ServerState, "typed quorum fields stay populated for the safety gate")
	require.Equal(t, 5, result.SyncedCount())
}

func TestProbeMntrEmptyResponse(t *testing.T) {
	addr := startMntrServer(t, "")

	probe := NewProbe()
	result, err := probe.Mntr(context.Background(), port.MntrRequest{Address: addr})
	require.NoError(t, err)
	require.Empty(t, result.Fields, "empty wire response yields an empty Fields map; the service layer turns that into exit 3")
}
