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

// Package keeper adapts ClickHouse Keeper's four-letter-word interface to
// the chctl KeeperProbe port.
package keeper

import (
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
)

// Probe implements port.KeeperProbe over the mntr 4lw TCP endpoint.
type Probe struct {
	// Timeout bounds each query (default 5s).
	Timeout time.Duration
}

// NewProbe builds the keeper probe.
func NewProbe() *Probe {
	return &Probe{}
}

// Mntr queries a keeper's mntr endpoint and returns the parsed result.
//
// Why mntr: it exposes synced_followers — the real Raft health signal —
// which the controller approximates with pod readiness; reading it directly
// makes the off-cluster gate at least as accurate.
//
// Without it: the safety gate would guess from TCP reachability only.
func (p *Probe) Mntr(ctx context.Context, request port.MntrRequest) (port.MntrResult, error) {
	timeout := p.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", request.Address)
	if err != nil {
		return port.MntrResult{}, fmt.Errorf("dial keeper %s: %w", request.Address, err)
	}
	defer func() { _ = conn.Close() }()

	deadline, _ := ctx.Deadline()
	_ = conn.SetDeadline(deadline)
	if _, err := conn.Write([]byte("mntr")); err != nil {
		return port.MntrResult{}, fmt.Errorf("send mntr: %w", err)
	}

	raw, err := io.ReadAll(conn)
	if err != nil {
		return port.MntrResult{}, fmt.Errorf("read mntr: %w", err)
	}
	return parseMntr(string(raw)), nil
}

// parseMntr parses mntr key=value lines.
func parseMntr(out string) port.MntrResult {
	result := port.MntrResult{}
	for _, line := range strings.Split(out, "\n") {
		key, value, ok := splitKV(strings.TrimSpace(line))
		if !ok {
			continue
		}
		switch key {
		case "zk_server_state":
			result.ServerState = value
		case "zk_synced_followers":
			result.SyncedFollowers = parseInt(value)
		case "zk_voting_followers":
			result.VotingMembers = parseInt(value)
		}
	}
	return result
}

// splitKV splits one mntr line on the first tab.
func splitKV(line string) (string, string, bool) {
	idx := strings.IndexByte(line, '\t')
	if idx < 0 {
		return "", "", false
	}
	return line[:idx], line[idx+1:], true
}

// parseInt parses an mntr integer field; unparsable values yield 0 so a
// malformed field fails the gate loudly via zero counts rather than a
// panic.
func parseInt(value string) int {
	n, _ := strconv.Atoi(value)
	return n
}
