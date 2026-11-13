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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// TestEngine_IdentityPersister_SavesImmediately: the persister merges the
// settled identity into the state file at claim time — a later critical
// failure must not lose it, and the job subcommands read it there.
func TestEngine_IdentityPersister_SavesImmediately(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir() + "/state.json")
	require.NoError(t, err)
	engine := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1")

	persist := engine.IdentityPersister("aws")
	require.NotNil(t, persist, "a store-backed engine must expose the persister")

	// Identity settles mid-run (the settle step's contract).
	persist(model.NodeIdentity{
		Role:        model.RoleReader,
		Hostname:    "reader-3",
		InstanceID:  "i-abc",
		PrivateIP:   "10.0.1.13",
		Region:      "us-east-1",
		ClusterName: "logexplorer",
		DNSZone:     "ch.internal",
		CRName:      "01-3",
		CROrdinal:   3,
		LeaseTable:  "reader-ordinals",
	})

	loaded, err := store.Load()
	require.NoError(t, err)
	require.NotNil(t, loaded.Identity)
	require.Equal(t, "reader-3", loaded.Identity.Hostname)
	require.Equal(t, "01-3", loaded.Identity.CRName)
	require.Equal(t, "aws", loaded.Identity.Cloud)
	require.Equal(t, "hash-1", loaded.SpecHash)
}

// TestEngine_IdentityPersister_NilWithoutStore: no store, no persister
// (unit tests and dry runs never write identity).
func TestEngine_IdentityPersister_NilWithoutStore(t *testing.T) {
	engine := NewEngine(nil, nil)
	require.Nil(t, engine.IdentityPersister("aws"))
}

// TestEngine_SpecHashInvalidatesResume: steps recorded done under an
// old bundle hash re-run when the hash changes; the same hash resumes.
func TestEngine_SpecHashInvalidatesResume(t *testing.T) {
	store, err := NewFileStateStore(t.TempDir() + "/state.json")
	require.NoError(t, err)

	// First run completes one step under hash-1.
	first := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1")
	var ran int
	res := first.Run(context.Background(), []Step{
		StepFunc("install_static_configs", Critical, onceFatal, func(context.Context) error {
			ran++
			return nil
		}),
	})
	require.NoError(t, res.Fatality)
	require.Equal(t, 1, ran)

	// Same hash: skipped (resume).
	second := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-1")
	res = second.Run(context.Background(), []Step{
		StepFunc("install_static_configs", Critical, onceFatal, func(context.Context) error {
			ran++
			return nil
		}),
	})
	require.True(t, res.Steps[0].Skipped, "same bundle hash: the done step is skipped")
	require.Equal(t, 1, ran)

	// New hash: the prior record is stale — the step re-runs.
	third := NewEngine(nil, store).WithIdentity(nil, "aws", "hash-2")
	res = third.Run(context.Background(), []Step{
		StepFunc("install_static_configs", Critical, onceFatal, func(context.Context) error {
			ran++
			return nil
		}),
	})
	require.False(t, res.Steps[0].Skipped, "a new bundle hash must invalidate the done record")
	require.Equal(t, 2, ran)
}

// wallClockSleepFree keeps the fake-clock import honest for future
// timing-sensitive tests in this file.
var _ = time.Second
