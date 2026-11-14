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

package port

import (
	"context"
)

// UsersRenderer renders users.d XML from a normalized CHI, resolving
// password references through the SecretSource.
type UsersRenderer interface {
	// RenderUsers writes the users.d file(s) for a normalized CHI.
	// Network restrictions come from the request (cloud CIDRs are cloud
	// data, never spec content).
	RenderUsers(ctx context.Context, request RenderUsersRequest) ([]RenderedFile, error)
}

// RenderUsersRequest is the input to UsersRenderer.RenderUsers.
type RenderUsersRequest struct {
	// NormalizedCR is the SpecLoader's CHI output.
	NormalizedCR any
	// OutputDir is the users.d directory (e.g. /etc/clickhouse-server/users.d).
	OutputDir string
	// Networks are the per-user IP allowlists, keyed by user name;
	// localhost is always appended. Cloud CIDRs (subnet, NLB) live here —
	// NOT in the spec — so the spec stays cloud-agnostic.
	Networks map[string][]string
	// DryRun skips writing; rendered files are still returned.
	DryRun bool
}

// SecretsResolver resolves password references for user rendering.
type SecretsResolver interface {
	// ResolveSecret fetches one secret value by name (cloud store).
	ResolveSecret(ctx context.Context, request SecretRequest) (string, error)
}

// UserSpec is one user in the cluster's user configuration.
type UserSpec struct {
	// Name is the ClickHouse user name.
	Name string
	// Profile is the ClickHouse profile.
	Profile string
	// Quota is the ClickHouse quota.
	Quota string
	// AccessManagement enables SQL access management for this user.
	AccessManagement bool
	// Networks are the IP allowlist entries (cloud CIDRs + operator extras).
	Networks []string
	// PasswordSHA256Hex is the resolved password hash (never plaintext).
	PasswordSHA256Hex string
}
