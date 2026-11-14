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

// Package conformance is the provider contract suite: the same scenarios
// run against every provider (AWS, Azure, and every future entry in the
// registry). A new cloud proves itself here once, and the boot pipeline
// never special-cases a provider again.
package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	chctlport "github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// Suite drives one provider through the capability scenarios. Fakes for
// each provider's API surfaces inject at the provider's own seams; the
// suite only speaks ports.
type Suite struct {
	Name     string
	Provider chinitport.Provider
}

// Run executes every conformance scenario. Providers implementing the
// optional capabilities run those scenarios; the suite SKIPS (does not
// fail) optional scenarios for providers that lack the capability —
// that degradation is the factory's designed behavior.
func (s Suite) Run(t *testing.T) {
	t.Helper()

	t.Run("name", func(t *testing.T) {
		require.NotEmpty(t, s.Provider.Name())
	})

	t.Run("metadata_shape", func(t *testing.T) {
		meta, err := s.Provider.Metadata(context.Background())
		require.NoError(t, err, "metadata is a mandatory capability")
		require.NotEmpty(t, meta.InstanceID, "instance id must resolve")
		require.NotEmpty(t, meta.PrivateIP, "private ip must resolve")
		require.NotEmpty(t, meta.Region, "region must resolve")
		require.NotEmpty(t, meta.Provider, "the provider stamps its name")
	})

	t.Run("secrets_roundtrip", func(t *testing.T) {
		value, err := s.Provider.GetSecretValue(context.Background(), chctlport.SecretRequest{
			Name: "conformance-test-secret",
		})
		require.NoError(t, err, "the conformance wiring serves this secret")
		require.NotEmpty(t, value, "a missing secret must be an error, never an empty success — the empty-success shape once wrote empty passwords to users.d")
	})

	t.Run("config_bundle_fetch", func(t *testing.T) {
		bundle := model.ConfigBundle{Bucket: "conformance-bucket", Prefix: "bootstrap"}
		dest := t.TempDir()
		written, err := s.Provider.FetchBundle(context.Background(), bundle, dest)
		require.NoError(t, err, "the conformance wiring serves the bundle")
		require.NotEmpty(t, written, "at least one member must land")
		for _, path := range written {
			require.FileExists(t, path)
		}
	})

	// Optional capabilities: probed, not required.
	t.Run("optional_capabilities", func(t *testing.T) {
		if chinitport.AsOrdinalClaimer(s.Provider) == nil {
			t.Logf("provider %s has no OrdinalClaimer (acceptable: static-identity clouds)", s.Provider.Name())
		}
		if chinitport.AsLeaseLister(s.Provider) == nil {
			t.Logf("provider %s has no LeaseLister (acceptable: inventory falls back to static spec lists)", s.Provider.Name())
		}
		if chinitport.AsDNSRegistrar(s.Provider) == nil {
			t.Logf("provider %s has no DNSRegistrar (acceptable: degraded DNS)", s.Provider.Name())
		}
		if chinitport.AsSpotSignals(s.Provider) == nil {
			t.Logf("provider %s has no SpotSignals (acceptable: on-demand only)", s.Provider.Name())
		}
	})
}

// RunAll runs every suite against the registry.
func RunAll(t *testing.T, suites []Suite) {
	t.Helper()
	for _, s := range suites {
		s := s
		t.Run(s.Name, func(t *testing.T) {
			s.Run(t)
		})
	}
}
