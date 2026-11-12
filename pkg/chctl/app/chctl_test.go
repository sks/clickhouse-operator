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

package app

import (
	"bytes"
	"strings"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/service"

	"github.com/stretchr/testify/require"
)

func TestPrintGcResultTSVShape(t *testing.T) {
	// The bash timer piped stdout line-by-line: dropped hosts must be
	// one TSV row each with no extra decoration, in detection order.
	var stdout, stderr bytes.Buffer
	printGcResult(&stdout, &stderr, &service.ReplicaGcResult{
		Stale:   []string{"reader-1", "reader-2", "writer-0"},
		Dropped: []string{"reader-1", "reader-2"},
		Failed:  []string{"reader-3"},
	})

	require.Equal(t, "reader-1\nreader-2\n", stdout.String())
	require.True(t, strings.HasPrefix(stderr.String(), `gc: drop failed for "reader-3"`+"\n"))
	require.Contains(t, stderr.String(), "gc: 3 stale replica(s) detected, 2 dropped, 1 failed\n")
}

func TestPrintGcResultEmptyIsSilentOnStdout(t *testing.T) {
	// A fresh cluster: no drops, no stdout output for the timer to log.
	var stdout, stderr bytes.Buffer
	printGcResult(&stdout, &stderr, &service.ReplicaGcResult{})

	require.Empty(t, stdout.String())
	require.Equal(t, "gc: 0 stale replica(s) detected, 0 dropped, 0 failed\n", stderr.String())
}

func TestUsageMentionsReplicaGc(t *testing.T) {
	// The top-level help must document the new subcommand (docs update
	// is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "chctl replica gc -on <peer>")
}

func TestUsageMentionsRenderInventoryFlags(t *testing.T) {
	// The render command's cloud inventory must appear in the help
	// (docs update is a separate todo).
	var out bytes.Buffer
	usageWith(&out)
	require.Contains(t, out.String(), "[-host name=address]...")
	require.Contains(t, out.String(), "[-replica name=cloudname]")
}

func TestParseRenderHosts(t *testing.T) {
	// Bare form: the live harness's host selector, unchanged.
	name, hosts, err := parseRenderHosts([]string{"0-0"})
	require.NoError(t, err)
	require.Equal(t, "0-0", name)
	require.Empty(t, hosts)

	// Inventory form (repeatable): CR name → cloud address.
	name, hosts, err = parseRenderHosts([]string{"0-0=writer-0.ip", "0-1=reader-0.ip"})
	require.NoError(t, err)
	require.Empty(t, name)
	require.Equal(t, []domain.HostIdentity{
		{Name: "0-0", Address: "writer-0.ip"},
		{Name: "0-1", Address: "reader-0.ip"},
	}, hosts)

	// Mixed: selector plus inventory — the render-everything case.
	name, hosts, err = parseRenderHosts([]string{"0-0", "0-1=reader-0.ip"})
	require.NoError(t, err)
	require.Equal(t, "0-0", name)
	require.Equal(t, []domain.HostIdentity{{Name: "0-1", Address: "reader-0.ip"}}, hosts)

	// Malformed and duplicate-selector inputs are rejected.
	_, _, err = parseRenderHosts([]string{"0-0="})
	require.ErrorContains(t, err, "name=address or a bare host name")
	_, _, err = parseRenderHosts([]string{"=writer-0.ip"})
	require.ErrorContains(t, err, "name=address or a bare host name")
	_, _, err = parseRenderHosts([]string{"0-0", "0-1"})
	require.ErrorContains(t, err, "only one bare -host selector")
}
