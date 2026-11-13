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
	"os"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
)

// NodeRenderer renders and installs ONE node's complete configuration set
// in a single invocation: the host sections, the inventory sections
// (remote_servers), the users.d files — or, for a keeper node, the merged
// single config file.
//
// Why a dedicated port: the stack's node bootstrap previously assembled
// several render calls plus per-site prune lists and chmod/chown lines,
// and one site forgetting the chown/chmod left root-owned 0640 files the
// ClickHouse service user could not read — a whole-node crash class born
// from orchestration spread across callers. RenderNode owns the entire
// discipline: render → validate → stage → own/permission → install
// atomically, with a check mode that renders and diffs without writing.
type NodeRenderer interface {
	// RenderNode renders the node's full config set and installs it
	// (unless DryRun/Check), applying ownership and permission discipline.
	RenderNode(ctx context.Context, request RenderNodeRequest) (RenderNodeResult, error)
}

// RenderNodeRequest is the input to NodeRenderer.RenderNode.
type RenderNodeRequest struct {
	// NormalizedCR is the SpecLoader's output (CHI or CHK per Keeper).
	NormalizedCR any
	// Keeper selects the keeper flow: a merged single-file render
	// (SingleFilePath) with no users render — keepers carry no
	// ClickHouse users and run with an explicit --config file.
	Keeper bool
	// NodeType is the informational role flag (writer|reader|backup|
	// keeper): it documents intent in the summary and must agree with
	// Keeper (the app layer validates; it rides here for the summary).
	NodeType string

	// CHI flow inputs (mirrors RenderCHIRequest / RenderUsersRequest).
	HostName string
	Hosts    []domain.HostIdentity
	Replicas map[string]string
	// UserPasswords injects runtime password hashes into the users
	// group (name → sha256 hex; hashed in the app layer, never bytes).
	UserPasswords map[string]string
	// RemoteUser/RemotePassword credential every remote_servers
	// <replica> for distributed queries.
	RemoteUser     string
	RemotePassword string
	// Networks are the per-user IP allowlists for the users.d render
	// (cloud CIDRs arrive by flag, never spec content).
	Networks map[string][]string

	// Keeper flow inputs (mirrors RenderCHKRequest).
	KeeperAddresses map[string]string
	ServerID        int
	RaftIDs         map[string]int
	SingleFilePath  string

	// Install inputs.
	// OutputDir is the config.d directory (required).
	OutputDir string
	// UsersDir is the users.d directory (required; the app layer
	// defaults it to the sibling "users.d" of OutputDir).
	UsersDir string
	// OwnerUser names the account every installed file is chown'd to
	// ("clickhouse" by default, or numeric "uid[:gid]" for distros whose
	// name resolution is odd). Empty keeps the default.
	OwnerUser string
	// DryRun skips every write: the result carries the full plan
	// (destinations, modes, content) and the would-be prunes.
	DryRun bool
	// Check renders and diffs the installed files WITHOUT writing
	// anything: Drift carries the delta and the caller exits 4 on it.
	Check bool
}

// RenderNodeFile is one file of the rendered node plan, with its
// destination and the exact permission the install applies.
type RenderNodeFile struct {
	// Group is the producing group ("common", "host", "users", "single").
	Group string
	// Path is the FULL destination path the file installs to.
	Path string
	// Mode is the permission the install applies (after write, so umask
	// can never soften the discipline).
	Mode os.FileMode
	// Content is the rendered file body.
	Content string
}

// RenderNodeResult is the outcome of a render-node run.
type RenderNodeResult struct {
	// Files is the rendered plan: every file with its destination, mode
	// and content — present for install, dry-run and check alike.
	Files []RenderNodeFile
	// Pruned lists chctl-owned installed files the current render no
	// longer produces: removed on install, reported (not removed) on
	// dry-run, and carried into Drift on check.
	Pruned []string
	// Drift is the check-mode delta ("<path>: missing|changed|stale");
	// empty when the installed config matches the render. The caller
	// turns a non-empty Drift into exit code 4.
	Drift []string
	// DryRun/Check/NodeType echo the request so printing layers label
	// the outcome without carrying the request along.
	DryRun   bool
	Check    bool
	NodeType string
}
