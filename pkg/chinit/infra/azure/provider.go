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

// Package azure implements chinit's Azure provider: IMDS metadata (with
// managed identity), Key Vault secrets, Blob config-bundle fetch, Table
// Storage ordinal leases, Private DNS record management and scheduled-
// events interruption watching.
//
// The Azure bash port predates the ordinal-lease mechanism (its readers
// had static names); this provider implements the same lease contract as
// AWS over Table Storage so both clouds converge on one identity model —
// the conformance suite runs the same scenarios against both.
package azure

import (
	"context"
	"fmt"
	"net/http"

	chctlport "github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// Provider is the Azure registry entry.
type Provider struct {
	region string
	spec   *model.BootSpec

	meta            *IMDSMetadata
	secrets         *KeyVaultSource
	secretsOverride chctlport.SecretSource
	bundle          *BlobBundleSource
	bundleOverride  chinitport.ConfigSource
	claims          *TableClaimer
	dns             *PrivateDNS
	spot    *ScheduledEventsWatcher
}

// Compile-time capability checks.
var (
	_ chinitport.Provider       = (*Provider)(nil)
	_ chinitport.OrdinalClaimer = (*Provider)(nil)
	_ chinitport.DNSRegistrar   = (*Provider)(nil)
	_ chinitport.SpotSignals     = (*Provider)(nil)
)

// New builds the Azure provider.
func New(cfg chinitport.Config) chinitport.Provider {
	region := cfg.Region
	if region == "" && cfg.Spec != nil {
		region = cfg.Spec.Region
	}
	p := &Provider{region: region, spec: cfg.Spec}
	p.meta = NewIMDSMetadata()
	p.secrets = NewKeyVaultSource(cfg.Spec)
	p.bundle = NewBlobBundleSource(cfg.Spec)
	p.claims = NewTableClaimer(cfg.Spec)
	p.dns = NewPrivateDNS(cfg.Spec)
	p.spot = NewScheduledEventsWatcher()
	return p
}

// Name is the registry key.
func (p *Provider) Name() string { return "azure" }

// WithIMDSClient injects a metadata HTTP client (conformance tests).
func (p *Provider) WithIMDSClient(c interface {
	Do(req *http.Request) (*http.Response, error)
}) chinitport.Provider {
	p.meta = p.meta.WithClient(c)
	return p
}

// Metadata probes the Azure IMDS.
func (p *Provider) Metadata(ctx context.Context) (chinitport.InstanceMetadata, error) {
	return p.meta.Get(ctx)
}

// GetSecretValue resolves a Key Vault secret (managed identity), or
// the test-injected override when present.
func (p *Provider) GetSecretValue(ctx context.Context, request chctlport.SecretRequest) (string, error) {
	if p.secretsOverride != nil {
		return p.secretsOverride.GetSecretValue(ctx, request)
	}
	return p.secrets.GetSecretValueCtx(ctx, request.Name, request.Field)
}

// WithSecrets overrides the secret source (conformance tests).
func (p *Provider) WithSecrets(s chctlport.SecretSource) chinitport.Provider {
	p.secretsOverride = s
	return p
}

// WithBundle overrides the config-bundle fetcher (conformance tests).
func (p *Provider) WithBundle(c chinitport.ConfigSource) chinitport.Provider {
	p.bundleOverride = c
	return p
}

// FetchBundle downloads the config bundle from Blob Storage.
func (p *Provider) FetchBundle(ctx context.Context, bundle model.ConfigBundle, destDir string) ([]string, error) {
	if p.bundleOverride != nil {
		return p.bundleOverride.FetchBundle(ctx, bundle, destDir)
	}
	return p.bundle.FetchBundle(ctx, bundle, destDir)
}

// ClaimOrdinal claims a reader ordinal via Table Storage.
func (p *Provider) ClaimOrdinal(ctx context.Context, req chinitport.ClaimRequest) (int, error) {
	return p.claims.Claim(ctx, req)
}

// RenewLease extends the node's lease.
func (p *Provider) RenewLease(ctx context.Context, req chinitport.RenewRequest) error {
	return p.claims.Renew(ctx, req)
}

// ReleaseLease drops the node's claim.
func (p *Provider) ReleaseLease(ctx context.Context, req chinitport.ReleaseRequest) error {
	return p.claims.Release(ctx, req)
}

// ResolveZoneID finds the private DNS zone by name (ARM query).
func (p *Provider) ResolveZoneID(ctx context.Context, zoneName string) (string, error) {
	return p.dns.ResolveZoneID(ctx, zoneName)
}

// UpsertARecord creates or replaces the node's A record.
func (p *Provider) UpsertARecord(ctx context.Context, req chinitport.ARecordRequest) error {
	return p.dns.UpsertARecord(ctx, req)
}

// DeleteARecord removes the node's A record.
func (p *Provider) DeleteARecord(ctx context.Context, req chinitport.ARecordRequest) error {
	return p.dns.DeleteARecord(ctx, req)
}

// Watch polls scheduled events for the Preempt document.
func (p *Provider) Watch(ctx context.Context) <-chan chinitport.InterruptionNotice {
	return p.spot.Watch(ctx)
}

// azureConfigError names missing spec fields a constructor needed (the
// app layer renders it before any client is built).
type azureConfigError struct{ Missing string }

func (e *azureConfigError) Error() string {
	return "azure provider: missing " + e.Missing
}

// fmtGuard keeps fmt imported for the smaller files in this package.
var _ = fmt.Sprintf
