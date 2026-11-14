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

// Package aws implements chinit's AWS provider: IMDS metadata, Secrets
// Manager, S3 config-bundle fetch, DynamoDB ordinal leases, Route53
// private-zone DNS and the EC2 spot interruption watcher.
//
// Every AWS API call lives behind this package's small client structs so
// the conformance suite can drive the provider with fake API surfaces
// while production uses real SDK clients (constructor-injected).
package aws

import (
	"context"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// Provider is the AWS entry in chinit's provider registry. It composes
// the mandatory capabilities and implements the optional ones (ordinal
// claims, DNS, spot) — the registry constructor wires the real clients.
type Provider struct {
	region string
	spec   *model.BootSpec

	meta            *IMDSMetadata
	secrets         *SecretsManagerSource
	secretsOverride port.SecretSource
	bundle          *S3BundleSource
	bundleOverride  port.ConfigSource
	claims          *DynamoDBClaimer
	lister          *DynamoDBClaimLister
	dns             *Route53Registrar
	spot            *SpotWatcher
}

// Compile-time capability checks: the factory relies on these.
var (
	_ port.Provider       = (*Provider)(nil)
	_ port.OrdinalClaimer = (*Provider)(nil)
	_ port.LeaseLister    = (*Provider)(nil)
	_ port.DNSRegistrar   = (*Provider)(nil)
	_ port.SpotSignals    = (*Provider)(nil)
)

// New builds the AWS provider with real SDK clients. The metadata
// client is injectable for the conformance suite (fake IMDS).
func New(cfg port.Config) port.Provider {
	region := cfg.Region
	if region == "" && cfg.Spec != nil {
		region = cfg.Spec.Region
	}
	p := &Provider{
		region: region,
		spec:   cfg.Spec,
	}
	p.meta = NewIMDSMetadata()
	p.secrets = NewSecretsManagerSource(region)
	p.bundle = NewS3BundleSource(region)
	p.claims = NewDynamoDBClaimer(region)
	p.lister = NewDynamoDBClaimLister(region)
	p.dns = NewRoute53Registrar(region)
	p.spot = NewSpotWatcher()
	return p
}

// WithIMDSClient injects a metadata HTTP client (conformance tests).
func (p *Provider) WithIMDSClient(c interface {
	Do(req *http.Request) (*http.Response, error)
}) port.Provider {
	p.meta = p.meta.WithClient(c)
	return p
}

// WithSecrets overrides the secret source (conformance tests inject a
// fake; production uses the instance-role Secrets Manager source).
func (p *Provider) WithSecrets(s port.SecretSource) port.Provider {
	p.secretsOverride = s
	return p
}

// WithBundle overrides the config-bundle fetcher (conformance tests).
func (p *Provider) WithBundle(c port.ConfigSource) port.Provider {
	p.bundleOverride = c
	return p
}

// Name is the registry key.
func (p *Provider) Name() string { return "aws" }

// Metadata probes EC2 IMDSv2.
func (p *Provider) Metadata(ctx context.Context) (port.InstanceMetadata, error) {
	m, err := p.meta.Get(ctx)
	if err != nil {
		return port.InstanceMetadata{}, err
	}
	m.Provider = p.Name()
	return m, nil
}

// GetSecretValue resolves a Secrets Manager entry (the adapter), or
// the test-injected override when present.
func (p *Provider) GetSecretValue(ctx context.Context, request port.SecretRequest) (string, error) {
	if p.secretsOverride != nil {
		return p.secretsOverride.GetSecretValue(ctx, request)
	}
	return p.secrets.GetSecretValue(ctx, request)
}

// FetchBundle downloads the config bundle from S3 with the SHA gate.
func (p *Provider) FetchBundle(ctx context.Context, bundle model.ConfigBundle, destDir string) ([]string, error) {
	if p.bundleOverride != nil {
		return p.bundleOverride.FetchBundle(ctx, bundle, destDir)
	}
	return p.bundle.FetchBundle(ctx, bundle, destDir)
}

// ClaimOrdinal claims a reader ordinal via DynamoDB.
func (p *Provider) ClaimOrdinal(ctx context.Context, req port.ClaimRequest) (int, error) {
	return p.claims.Claim(ctx, req)
}

// RenewLease extends the node's lease.
func (p *Provider) RenewLease(ctx context.Context, req port.RenewRequest) error {
	return p.claims.Renew(ctx, req)
}

// ReleaseLease drops the node's claim.
func (p *Provider) ReleaseLease(ctx context.Context, req port.ReleaseRequest) error {
	return p.claims.Release(ctx, req)
}

// ListActiveClaims enumerates the cluster's live ordinal leases (the
// inventory source for remote_servers and the -host flags).
func (p *Provider) ListActiveClaims(ctx context.Context, req port.ListClaimsRequest) ([]port.Claim, error) {
	return p.lister.ListActiveClaims(ctx, req)
}

// ResolveZoneID finds the Route53 private zone by exact DNS name.
func (p *Provider) ResolveZoneID(ctx context.Context, zoneName string) (string, error) {
	return p.dns.ResolveZoneID(ctx, zoneName)
}

// UpsertARecord creates or replaces the node's private A record.
func (p *Provider) UpsertARecord(ctx context.Context, req port.ARecordRequest) error {
	return p.dns.UpsertARecord(ctx, req)
}

// DeleteARecord removes the node's A record.
func (p *Provider) DeleteARecord(ctx context.Context, req port.ARecordRequest) error {
	return p.dns.DeleteARecord(ctx, req)
}

// Watch polls the IMDS spot interruption endpoint until the notice
// arrives, then delivers it once.
func (p *Provider) Watch(ctx context.Context) <-chan port.InterruptionNotice {
	return p.spot.Watch(ctx)
}

// awsConfig loads the SDK config with the provider's region pin. Every
// client constructor calls this — one place for the credential chain
// (instance role) and retry-mode settings.
func awsConfig(ctx context.Context, region string) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx, func(opts *config.LoadOptions) error {
		if region != "" {
			opts.Region = region
		}
		return nil
	})
	if err != nil {
		return aws.Config{}, fmt.Errorf("load AWS config: %w", err)
	}
	return cfg, nil
}
