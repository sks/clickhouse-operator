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

// Package port defines chinit's provider capability interfaces — the
// factory seam the bash script never had.
//
// Why capability ports instead of one mega-interface: clouds differ in
// what they offer (Civo has no IMDS; AWS spot notices ≠ Azure scheduled
// events; Azure's bash port never had ordinal leases). A provider
// implements the small interface for each capability it supports, and the
// boot step checks for it once: `if c, ok := p.(port.OrdinalClaimer); ok`.
// Adding a cloud means implementing the capabilities it has — nothing
// else in the pipeline changes. This mirrors chctl's domain/port shape
// (SecretSource) so the two CLIs compose without adapters.
package port

import (
	"context"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// InstanceMetadata is what the boot pipeline needs from the platform the
// node is running on. Everything identity-related derives from it.
type InstanceMetadata struct {
	Provider   string // "aws", "azure", …
	Region     string
	Zone       string // availability zone, when the platform has one
	InstanceID string
	PrivateIP  string
	// HostnameHint is the platform-assigned hostname (EC2's ip-* or the
	// cloud-init hostname); the identity step overrides it with the
	// settled cluster identity.
	HostnameHint string
}

// MetadataProvider reads instance metadata. Mandatory: every provider
// implements this (AWS IMDSv2, Azure IMDS; a cloud without an IMDS
// endpoint implements it from /etc/chinit/metadata baked by user-data).
type MetadataProvider interface {
	// Metadata returns the instance metadata, probing the platform.
	// Callers retry transient failures at the policy layer.
	Metadata(ctx context.Context) (InstanceMetadata, error)
}

// ConfigSource fetches the node's config bundle (chinit.yaml, static
// XMLs, chi.yaml/keeper.yaml) from provider object storage.
//
// Why a port: bootstrap.sh shelled aws s3 cp with hand-rolled retries;
// Azure needs Blob, and the SHA gate must sit in one place.
type ConfigSource interface {
	// FetchBundle downloads the bundle directory into destDir and
	// returns the files written. The SHA256 gate (when the spec pins
	// one) is the provider's contract: a mismatch is a Fatal error,
	// never a retry.
	FetchBundle(ctx context.Context, bundle model.ConfigBundle, destDir string) ([]string, error)
}

// OrdinalClaim lets an ASG member claim a stable reader-N identity.
//
// Why a lease and not static naming: readers are a spot/on-demand mix
// behind an ASG; identity must be stable across replacements while the
// instance itself is ephemeral. First writer-registered claim wins; the
// TTL lets a dead node's ordinal expire (AWS: DynamoDB conditional put;
// Azure: Table Storage entity with lease_until).
type OrdinalClaimer interface {
	// ClaimOrdinal acquires the first free ordinal in [0, count) and
	// returns it. It retries the sweep internally up to the deadline
	// (one lease window) — an instance-refresh replacement boots while
	// the old node still holds its lease.
	ClaimOrdinal(ctx context.Context, req ClaimRequest) (int, error)
	// RenewLease extends the holder's lease (the renewal unit calls it
	// every 5 minutes against a 15-minute TTL).
	RenewLease(ctx context.Context, req RenewRequest) error
	// ReleaseLease deletes the node's claim (spot drain / termination).
	ReleaseLease(ctx context.Context, req ReleaseRequest) error
}

// ClaimRequest is the input to ClaimOrdinal.
type ClaimRequest struct {
	Table        string // provider lease table (DynamoDB table / Storage table)
	Prefix       string // hostname prefix ("reader-")
	Count        int    // ordinals 0..Count-1
	InstanceID   string
	PrivateIP    string
	LeaseSeconds int
	// Deadline bounds the whole claim sweep (16 min > 15-min lease).
	DeadlineSeconds int
}

// RenewRequest is the input to RenewLease.
type RenewRequest struct {
	Table        string
	Hostname     string
	InstanceID   string
	LeaseSeconds int
}

// ReleaseRequest is the input to ReleaseLease.
type ReleaseRequest struct {
	Table    string
	Hostname string
}

// DNSRegistrar manages the node's private-zone A record. Readers
// self-register at claim time and the spot drain deregisters; writers
// register statically when the zone id is known.
type DNSRegistrar interface {
	// ResolveZoneID finds the private zone by exact DNS name (the
	// Terraform graph cannot pass the zone id without a cycle, so it
	// is resolved at boot and cached).
	ResolveZoneID(ctx context.Context, zoneName string) (string, error)
	// UpsertARecord creates or replaces the node's A record.
	UpsertARecord(ctx context.Context, req ARecordRequest) error
	// DeleteARecord removes it (drain/termination).
	DeleteARecord(ctx context.Context, req ARecordRequest) error
}

// ARecordRequest names one record operation.
type ARecordRequest struct {
	ZoneID     string
	ZoneName   string
	RecordName string // short hostname; FQDN = RecordName + "." + ZoneName
	IP         string
	TTL        int
}

// InterruptionNotice is the platform's "you are being reclaimed" signal.
type InterruptionNotice struct {
	// DeadlineSeconds is how long the node has (EC2 spot: 120; Azure
	// scheduled events: notice window from the event payload).
	DeadlineSeconds int
	// Raw is the platform payload for the drain log.
	Raw string
}

// SpotSignals watches the platform's interruption endpoint and delivers
// the notice once. EC2: IMDS spot/instance-action; Azure: Scheduled
// Events Preempt document. Long-running — the spot-watch subcommand
// owns the loop; the channel closes after the first notice.
type SpotSignals interface {
	Watch(ctx context.Context) <-chan InterruptionNotice
}

// Provider is the factory product: the mandatory capabilities every
// cloud implements, plus the chctl SecretSource (passwords/TLS — the
// same port chctl already uses, so one adapter serves both binaries).
type Provider interface {
	// Name is the provider registry key ("aws", "azure", …).
	Name() string
	// Metadata probes instance metadata (mandatory capability).
	MetadataProvider
	// Secrets resolves secret-store entries by name (mandatory).
	SecretSource
	// ConfigSource fetches the config bundle (mandatory).
	ConfigSource
}

// Capability probes — the factory's optional-capability seam. The boot
// steps call these once and degrade loudly (WARNING + feature disabled)
// when a provider lacks a capability, so a Civo-shaped provider (no
// IMDS, no managed DNS) still boots instead of panic-failing.

// AsOrdinalClaimer returns the claim capability, or nil.
func AsOrdinalClaimer(p Provider) OrdinalClaimer {
	if c, ok := p.(OrdinalClaimer); ok {
		return c
	}
	return nil
}

// AsDNSRegistrar returns the DNS capability, or nil.
func AsDNSRegistrar(p Provider) DNSRegistrar {
	if c, ok := p.(DNSRegistrar); ok {
		return c
	}
	return nil
}

// AsSpotSignals returns the interruption-watch capability, or nil.
func AsSpotSignals(p Provider) SpotSignals {
	if c, ok := p.(SpotSignals); ok {
		return c
	}
	return nil
}

// Claim is one live ordinal-lease row (see LeaseLister): the identity a
// cluster member currently holds — hostname reader-N/writer-N with the
// instance that owns it.
type Claim struct {
	Hostname   string
	InstanceID string
	IP         string
}

// LeaseLister enumerates the cluster's ACTIVE ordinal leases.
//
// Why an optional capability: cluster inventory (remote_servers, the
// schema-sync -host flags, chctl health fan-out) must enumerate only
// ACTIVE lease holders — a reserved-but-vacant slot's CR host would put
// an unreachable peer in remote_servers/clusterAllReplicas and fail
// every DDL read that fans out (the vacant-slot finding). The ASG owns
// slot RESERVATIONS (max_size); the lease table owns LIVENESS, so the
// lease table is the only source that knows which reserved slots have a
// machine behind them. Providers without a listable lease store keep
// the static spec inventory (loud warning at the call site).
type LeaseLister interface {
	// ListActiveClaims returns every lease row with lease_until > now
	// from the table (one DynamoDB Scan / Azure Table query — the
	// caller treats the result as the cluster's live member set).
	ListActiveClaims(ctx context.Context, req ListClaimsRequest) ([]Claim, error)
}

// ListClaimsRequest is the input to ListActiveClaims.
type ListClaimsRequest struct {
	// Table is the provider lease table to scan (DynamoDB table /
	// Storage table) — the same table ClaimOrdinal writes.
	Table string
}

// AsLeaseLister returns the lease-listing capability, or nil.
func AsLeaseLister(p Provider) LeaseLister {
	if c, ok := p.(LeaseLister); ok {
		return c
	}
	return nil
}

// Registry is the provider factory: name → constructor. Detection fills
// in the constructor's config from metadata; a new cloud is one entry
// here plus one infra package.
type Registry map[string]func(cfg Config) Provider

// Config carries what a constructor needs before metadata exists: the
// region hint and the resolved spec (for provider-specific knobs like
// the Route53 zone id override).
type Config struct {
	Region string
	Spec   *model.BootSpec
}

// Register adds a provider constructor. Called from each infra package's
// init() — app imports the infra packages for their side effect and the
// registry is complete.
func (r Registry) Register(name string, ctor func(Config) Provider) {
	r[name] = ctor
}

// Build instantiates the named provider.
func (r Registry) Build(name string, cfg Config) (Provider, error) {
	ctor, ok := r[name]
	if !ok {
		return nil, &UnknownProviderError{Name: name}
	}
	return ctor(cfg), nil
}

// UnknownProviderError names a provider the registry does not know.
type UnknownProviderError struct{ Name string }

func (e *UnknownProviderError) Error() string {
	return "unknown cloud provider: " + e.Name + " (known: register it in pkg/chinit/infra)"
}
