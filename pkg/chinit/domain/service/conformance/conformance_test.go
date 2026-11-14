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

// Package conformance_test wires the real provider constructors through
// the conformance suite with each provider's injectable fakes. This is
// the proof the factory contract holds for every registered cloud.
package conformance_test

import (
	"net/http"
	"testing"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/service/conformance"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/aws"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/azure"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/fakes"
)

// TestProviderConformance runs the contract suite against every
// registered provider with fake IMDS endpoints, canned secrets and a
// canned bundle — no real API calls, deterministic CI. Adding GCP/Civo
// later means adding a suite entry here; the scenarios never change.
func TestProviderConformance(t *testing.T) {
	spec := &model.BootSpec{
		Role:         model.RoleReader,
		IdentityMode: model.IdentityLease,
		ClusterName:  "conformance",
	}

	secrets := &fakes.SecretSource{Values: map[string]string{
		"conformance-test-secret": "conformance-secret-value",
	}}
	bundle := &fakes.BundleSource{Files: map[string]string{
		"network.xml": "<clickhouse><listen_host>0.0.0.0</listen_host></clickhouse>",
		"storage.xml": "<clickhouse><storage_configuration/></clickhouse>",
	}}

	awsProvider := aws.New(port.Config{Region: "us-east-1", Spec: spec})
	if p, ok := awsProvider.(interface {
		WithIMDSClient(c interface {
			Do(req *http.Request) (*http.Response, error)
		}) port.Provider
	}); ok {
		awsProvider = p.WithIMDSClient(fakes.AWSIMDS().Client())
	}
	if p, ok := awsProvider.(interface {
		WithSecrets(port.SecretSource) port.Provider
	}); ok {
		awsProvider = p.WithSecrets(secrets)
	}
	if p, ok := awsProvider.(interface {
		WithBundle(port.ConfigSource) port.Provider
	}); ok {
		awsProvider = p.WithBundle(bundle)
	}

	azureProvider := azure.New(port.Config{Region: "eastus", Spec: spec})
	if p, ok := azureProvider.(interface {
		WithIMDSClient(c interface {
			Do(req *http.Request) (*http.Response, error)
		}) port.Provider
	}); ok {
		azureProvider = p.WithIMDSClient(fakes.AzureIMDS().Client())
	}
	if p, ok := azureProvider.(interface {
		WithSecrets(port.SecretSource) port.Provider
	}); ok {
		azureProvider = p.WithSecrets(secrets)
	}
	if p, ok := azureProvider.(interface {
		WithBundle(port.ConfigSource) port.Provider
	}); ok {
		azureProvider = p.WithBundle(bundle)
	}

	suites := []conformance.Suite{
		{Name: "aws", Provider: awsProvider},
		{Name: "azure", Provider: azureProvider},
	}
	conformance.RunAll(t, suites)
}
