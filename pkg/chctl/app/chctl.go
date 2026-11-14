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

// Package app implements the chctl CLI: flag parsing, subcommand
// dispatch, and wiring of the domain services with their infra adapters.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/service"
	awsinfra "github.com/altinity/clickhouse-operator/pkg/chctl/infra/aws"
	"github.com/altinity/clickhouse-operator/pkg/chctl/infra/keeper"
	"github.com/altinity/clickhouse-operator/pkg/chctl/infra/operator"
)

// Version is the chctl build version. The release workflow overrides it
// at build time via -ldflags "-X .../app.Version=<tag>"; locally it stays
// "dev".
//
// Why a build-time var: node bootstrap needs to compare the installed
// binary's version against the targeted release instead of
// unconditionally overwriting (and re-rendering configs) on every boot.
var Version = "dev"

// Exit codes for scripts and lifecycle hooks. Keeping the failure modes
// apart stops a broken probe from being misread as "safe" or as a gate
// hold, and keeps the drift answer (0) apart from re-render-needed (4).
const (
	exitOK          = 0
	exitUsageError  = 1
	exitQuorumHeld  = 2
	exitOpError     = 3
	exitConfigDrift = 4
)

// Run parses arguments and dispatches to a subcommand.
func Run(args []string) error {
	if len(args) == 0 {
		usage()
		return fmt.Errorf("no command given")
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "config":
		return runConfig(rest)
	case "users":
		return runUsers(rest)
	case "schema":
		return runSchema(rest)
	case "replica":
		return runReplica(rest)
	case "keeper":
		return runKeeper(rest)
	case "health":
		return runHealth(rest)
	case "version":
		printVersion(os.Stdout)
		return nil
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// printVersion writes the binary's version line — machine-comparable for
// bootstrap scripts (`chctl version` alone), with the Go toolchain build
// info when available.
func printVersion(w io.Writer) {
	if info, ok := debug.ReadBuildInfo(); ok && info.GoVersion != "" {
		fmt.Fprintf(w, "chctl %s (%s)\n", Version, info.GoVersion)
		return
	}
	fmt.Fprintf(w, "chctl %s\n", Version)
}

// usage prints the top-level help.
func usage() {
	usageWith(os.Stdout)
}

// usageWith writes the top-level help to a writer (testability).
func usageWith(w io.Writer) {
	fmt.Fprintf(w, `chctl %s — off-cluster ClickHouse control, powered by the operator's own code

Usage:
  chctl config render -spec chi.yaml [-host 0-0] [-host name=address]... [-replica name=cloudname]...
                        [-user-password name=path]... [-out config.d] [-users-dir users.d] [-dry-run] [-check] [-keeper]
                        [-remote-user <name> -remote-password-env <VAR> | -remote-password-file <path>]
                        keeper mode: [-keeper] -host <host> [-server-id <n>] [-raft-id name=id]... [-single-file <path>]
  chctl config render-node -spec chi.yaml -host <host> [-host name=address]... [-replica name=cloudname]...
                        -out config.d [-users-dir users.d] [-user clickhouse] [-node-type writer|reader|backup|keeper]
                        [-user-password name=path]... [-remote-user <name> -remote-password-env <VAR> | -remote-password-file <path>]
                        [-network user=ip]... [-secrets aws|none] [-region r] [-dry-run] [-check]
                        keeper mode: -keeper -node-type keeper -single-file <path> [-server-id <n>] [-raft-id name=id]... [-keeper-address name=addr]...
  chctl schema sync <host> -cluster <name> -host <name=addr>... [-user u] [-password p] [-port n] [-secure]
                        [-retries 2] [-prune-stale-self]
  chctl replica drop <host> -on <peer> -cluster <name> -host <name=addr>...
  chctl replica sync <host> -cluster <name> -host <name=addr>...
  chctl replica gc -on <peer> -cluster <name> -host <name=addr>... [-name-pattern reader-*] [-exclude <host>]...
  chctl keeper safe-to-stop -keeper <host:port> [-members n] [-wait-timeout 2m]
  chctl keeper mntr -keeper <host:port> [-timeout 5s] [-json]
  chctl health -cluster <name> -host <name=addr>... [-user u] [-password p] [-port n] [-secure]
               [-spec chi.yaml] [-keeper <host:port>] [-self <replica>] [-json] [-timeout 5s]
  chctl version

The <host> positional and the flags may appear in ANY order.

Commands:
  config    render config.d/users.d XML for a cluster or host from a CHI/CHK spec
            -user-password injects runtime passwords: each file is read, sha256-hashed
            and emitted as password_sha256_hex for that user (the spec declares the
            user, the secret store supplies the password — never the spec file)
            -remote-user with -remote-password-env/-remote-password-file injects
            <user>/<password> into every remote_servers <replica> so
            distributed queries authenticate (never argv passwords)
            -check renders to memory and byte-compares each would-be-installed
            file against -out (users group against -users-dir): prints
            IN SYNC / DRIFTED <file> / MISSING <file> per file and exits
            0 in sync, 4 on any drift — nothing is written (the config
            refresher and health runbook's probe; a wiped or hand-edited
            config.d is drift, never "in sync")
            keeper mode: -single-file merges common+host sections into ONE complete
            <clickhouse>-rooted config for nodes running clickhouse-keeper with an
            explicit --config (which disables config.d auto-include); -server-id
            overrides the CR's zero-based replica index with the myid file's
            authoritative identity; -raft-id overrides the raft_configuration
            <server><id> values for existing ensembles with ONE-based ids persisted
            in raft logs (unmapped hosts keep CR-derived ids; -server-id and this
            node's -raft-id must agree)
            render-node renders EVERYTHING for ONE node in a single invocation and
            installs it: the host-selected sections (macros/zookeeper/settings/
            profiles/files) AND the inventory sections (remote_servers with
            credentials) AND the users.d files — or, for a keeper node (-keeper
            -node-type keeper -single-file), the merged single config file.
            Install discipline is built in: staged writes renamed into place
            atomically, every file chown'd to the service account (-user, default
            clickhouse), 0640 for credential-bearing files (remote_servers with
            a password, users files with password hashes) and 0644 for the rest
            — a render or XML-validation failure installs NOTHING and names the
            offending section; -check diffs the render against the installed
            tree and exits 0 in sync, 4 with the per-file delta (nothing written);
            -dry-run prints the plan; files a previous render-node installed but
            this one no longer produces are pruned (manifest-tracked —
            hand-maintained files are never touched)
  schema    create all schema objects on a host from cluster peers (new-reader boot)
            -retries bounds the number of full create passes (a materialized view
            whose TO-target table lands later in the same batch needs the second);
            -prune-stale-self reclaims this host's OWN stale Keeper registration
            when the create fails with Code 253 — an ASG/VMSS replacement node
            re-claiming an ordinal whose dead predecessor left a znode
  replica   drop a dead replica's registration / re-sync a restarted one / gc stale ones
  keeper    Raft-quorum safety gate for planned keeper stop/replacement,
            plus mntr payload inspection for health scripts
  health    verify the stack in one pass: every inventory host serves SQL,
            replicas are writable (is_readonly=0), keeper mntr answers
            (-keeper), the spec's tables exist (-spec), and this node's
            replica is registered (-self). Exit 0 all-pass, 3 any-degraded,
            1 usage. -json emits {"status","checks":[{name,status,detail}]}
  version   print the binary version (build-time injected; for bootstrap
            scripts comparing against a target release)

Docs: docs/chctl.md
`, Version)
}

// runConfig implements `chctl config render` and `chctl config render-node`.
func runConfig(args []string) error {
	if len(args) == 0 || (args[0] != "render" && args[0] != "render-node") {
		return fmt.Errorf("usage: chctl config render -spec chi.yaml ... | chctl config render-node -spec chi.yaml ...")
	}
	if args[0] == "render-node" {
		return runRenderNode(args[1:])
	}
	fs := flag.NewFlagSet("config render", flag.ContinueOnError)
	var (
		specFile   string
		outDir     string
		usersDir   string
		dryRun     bool
		check      bool
		isKeeper   bool
		serverID   int
		singleFile string
	)
	var (
		hostFlags          hostList
		replicaMap         replicaList
		keeperAddrs        keeperAddrList
		raftIDList         raftIDFlagList
		userPasswords      userPasswordList
		remoteUser         string
		remotePasswordEnv  string
		remotePasswordFile string
	)
	fs.StringVar(&specFile, "spec", "", "CHI/CHK spec file (YAML), required")
	fs.Var(&hostFlags, "host", "render host-scoped files for this host; with name=address syntax (repeatable) also maps the CR host to its cloud DNS name/IP")
	fs.StringVar(&outDir, "out", "out", "output directory for rendered files")
	fs.StringVar(&usersDir, "users-dir", "", "output directory for users-group files (defaults to -out)")
	fs.BoolVar(&dryRun, "dry-run", false, "print rendered files without writing")
	fs.BoolVar(&check, "check", false, "compare the would-be-rendered files against -out (and -users-dir) without writing anything: exit 0 in sync, 4 if any file drifted or is missing on disk")
	fs.BoolVar(&isKeeper, "keeper", false, "spec is a ClickHouseKeeperInstallation (CHK)")
	fs.Var(&replicaMap, "replica", "CR host name to cloud short hostname mapping name=cloudname (repeatable; the rendered macros <replica> carries the cloud name)")
	fs.Var(&keeperAddrs, "keeper-address", "CHK host cloud DNS mapping crName=address (repeatable; raft server entries use the cloud names)")
	fs.IntVar(&serverID, "server-id", 0, "keeper mode: overrides the rendered <server_id> for the selected host (e.g. the myid file's 1-based value; 0 keeps the CR-derived replica index)")
	fs.Var(&raftIDList, "raft-id", "keeper mode: CR host name to raft_configuration <server><id> mapping name=id (repeatable, e.g. 0-0=1; unmapped hosts keep CR-derived zero-based ids)")
	fs.StringVar(&singleFile, "single-file", "", "keeper mode: write ONE complete <clickhouse>-rooted config file (common+host sections merged) to this path instead of per-group files in -out; requires -host")
	fs.Var(&userPasswords, "user-password", "user name=password-file path (repeatable); the file is read, hashed sha256 and injected as password_sha256_hex for that user — never the spec")
	fs.StringVar(&remoteUser, "remote-user", "", "ClickHouse user distributed queries authenticate as (with -remote-password-env/-remote-password-file)")
	fs.StringVar(&remotePasswordEnv, "remote-password-env", "", "environment variable holding the distributed-query password (mutually exclusive with -remote-password-file)")
	fs.StringVar(&remotePasswordFile, "remote-password-file", "", "file holding the distributed-query password (mutually exclusive with -remote-password-env)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if specFile == "" {
		return fmt.Errorf("-spec is required")
	}
	// -check is a stricter -dry-run: it shares the no-writes guarantee,
	// and running both is the caller asking twice for nothing.
	if check && dryRun {
		return fmt.Errorf("-check and -dry-run are mutually exclusive: -check already writes nothing")
	}

	// Distributed-query credentials: resolve the password once, from
	// exactly one source, before anything renders. The rendered
	// remote_servers.xml carries the pair per <replica> so peers
	// authenticate instead of rejecting passwordless "default"
	// (PASSWORD_REQUIRED). The password itself never appears in argv,
	// logs, or the spec.
	remotePassword, err := readRemotePassword(remoteUser, remotePasswordEnv, remotePasswordFile)
	if err != nil {
		return err
	}

	// -host is dual-form: a bare name (e.g. "0-0") selects the host whose
	// files render; a name=address pair adds the host to the cloud
	// inventory instead. The live harness passes the bare form, so the
	// selector must survive it.
	hostName, hosts, err := parseRenderHosts(hostFlags.values)
	if err != nil {
		return err
	}
	replicas := make(map[string]string, len(replicaMap.values))
	for _, entry := range replicaMap.values {
		name, cloudName, ok := strings.Cut(entry, "=")
		if !ok || name == "" || cloudName == "" {
			return fmt.Errorf("-replica %q must be name=cloudname", entry)
		}
		replicas[name] = cloudName
	}

	// Runtime user passwords: read each secret file and hash it here in
	// the CLI layer — only the hex crosses into the request. A spec file
	// with a checked-in password would be the exact leak this prevents.
	userPasswordHashes, err := readUserPasswordHashes(userPasswords.values)
	if err != nil {
		return err
	}

	specData, err := os.ReadFile(specFile)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}
	specRef := domain.SpecReference{Data: specData}
	loader := operator.NewSpecLoader()
	renderer := operator.NewConfigRenderer()
	ctx := context.Background()

	// Keeper-mode flags are meaningless against a CHI spec: refuse
	// instead of silently ignoring (a CHI render has no <server_id>
	// and no keeper single-file shape).
	if !isKeeper {
		if singleFile != "" {
			return fmt.Errorf("-single-file applies to keeper mode only: add -keeper")
		}
		if serverID != 0 {
			return fmt.Errorf("-server-id applies to keeper mode only: add -keeper")
		}
		if len(raftIDList.values) > 0 {
			return fmt.Errorf("-raft-id applies to keeper mode only: add -keeper")
		}
	}
	if serverID < 0 {
		return fmt.Errorf("-server-id must be positive, got %d", serverID)
	}

	if isKeeper {
		if len(userPasswordHashes) > 0 {
			return fmt.Errorf("-user-password is not applicable to a CHK spec (keeper installations carry no ClickHouse users)")
		}
		// A single-file render is host-complete — one keeper node's
		// entire config — so a host must be selected.
		if singleFile != "" && hostName == "" {
			return fmt.Errorf("-single-file requires a keeper host: pass -host (name or name=address)")
		}
		// -check compares the per-group files the config render command
		// installs to -out; a single-file render never writes them, so
		// the comparison would report every file MISSING by definition.
		// Single-file drift is the render-node surface.
		if singleFile != "" && check {
			return fmt.Errorf("-check with -single-file is not supported here: the single-file drift check is the render-node -check surface")
		}
		// The server-id override targets the selected host's
		// <server_id> section; without a host there is no section to
		// shape.
		if serverID != 0 && hostName == "" {
			return fmt.Errorf("-server-id requires a keeper host: pass -host (name or name=address)")
		}
		raftIDs, err := parseRaftIDFlags(raftIDList.values)
		if err != nil {
			return err
		}
		for name, id := range raftIDs {
			if id <= 0 {
				return fmt.Errorf("-raft-id %s=%d: raft ids must be positive", name, id)
			}
		}
		cr, err := loader.LoadCHK(ctx, port.LoadCHKRequest{Spec: specRef})
		if err != nil {
			return err
		}
		keeperMap := map[string]string{}
		for _, entry := range keeperAddrs.values {
			name, address, ok := strings.Cut(entry, "=")
			if !ok || name == "" || address == "" {
				return fmt.Errorf("-keeper-address %q must be crName=address", entry)
			}
			keeperMap[name] = address
		}
		rendered, err := renderer.RenderCHK(ctx, port.RenderCHKRequest{
			NormalizedCR:    cr,
			HostName:        hostName,
			Hosts:           hosts,
			OutputDir:       outDir,
			DryRun:          dryRun || check,
			KeeperAddresses: keeperMap,
			ServerID:        serverID,
			RaftIDs:         raftIDs,
			SingleFilePath:  singleFile,
		})
		if err != nil {
			return err
		}
		if check {
			// -single-file is the render-node surface (a host-complete
			// file outside -out); here the per-group comparison against
			// -out is what -check sees. The single-file entry the render
			// appends would compare a destination the config render
			// command never installs, so it is excluded.
			return runCheck(filterSingleFile(rendered), outDir, "", os.Stdout)
		}
		printRendered(rendered)
		return nil
	}

	cr, err := loader.LoadCHI(ctx, port.LoadCHIRequest{Spec: specRef})
	if err != nil {
		return err
	}
	rendered, err := renderer.RenderCHI(ctx, port.RenderCHIRequest{
		NormalizedCR:   cr,
		HostName:       hostName,
		Hosts:          hosts,
		Replicas:       replicas,
		UserPasswords:  userPasswordHashes,
		RemoteUser:     remoteUser,
		RemotePassword: remotePassword,
		OutputDir:      outDir,
		UsersDir:       usersDir,
		DryRun:         dryRun || check,
	})
	if err != nil {
		return err
	}
	if check {
		return runCheck(rendered, outDir, usersDir, os.Stdout)
	}
	printRendered(rendered)
	return nil
}

// filterSingleFile drops the merged single-file entry a keeper render
// appends (Group "single") so the -check comparison covers only the
// per-group files the config render command installs to -out.
func filterSingleFile(rendered []port.RenderedFile) []port.RenderedFile {
	filtered := make([]port.RenderedFile, 0, len(rendered))
	for _, file := range rendered {
		if file.Group != "single" {
			filtered = append(filtered, file)
		}
	}
	return filtered
}

// renderNodeFlagSurface is the `config render-node` flag set: the full
// surface the node bootstrap passes — spec, destinations, host
// selection/inventory, credentials, users, keeper identity, install
// discipline and the check/dry-run modes.
type renderNodeFlagSurface struct {
	specFile           string
	outDir             string
	usersDir           string
	owner              string
	nodeType           string
	dryRun             bool
	check              bool
	keeper             bool
	serverID           int
	singleFile         string
	remoteUser         string
	remotePasswordEnv  string
	remotePasswordFile string
	region             string
	secrets            string
	hostFlags          hostList
	replicaMap         replicaList
	keeperAddrs        keeperAddrList
	raftIDList         raftIDFlagList
	userPasswords      userPasswordList
	networks           portNetworkList
}

// newRenderNodeFlagSet builds the flag set over the surface struct.
func newRenderNodeFlagSet() (*flag.FlagSet, *renderNodeFlagSurface) {
	fs := flag.NewFlagSet("config render-node", flag.ContinueOnError)
	s := &renderNodeFlagSurface{}
	fs.StringVar(&s.specFile, "spec", "", "CHI/CHK spec file (YAML), required")
	fs.StringVar(&s.outDir, "out", "", "config.d directory the rendered files install to, required")
	fs.StringVar(&s.usersDir, "users-dir", "", "users.d directory (defaults to the 'users.d' sibling of -out)")
	fs.StringVar(&s.owner, "user", "clickhouse",
		"account every installed file is chown'd to (name, uid, or uid:gid — numeric forms escape distros with odd name resolution)")
	fs.StringVar(&s.nodeType, "node-type", "writer",
		"informational node role: writer|reader|backup|keeper (gates the keeper single-file flow and documents intent in the summary)")
	fs.BoolVar(&s.dryRun, "dry-run", false, "print the render plan (destinations and modes) without writing")
	fs.BoolVar(&s.check, "check", false, "render to staging and diff the installed files without writing anything: exit 0 in sync, 4 with the delta on drift")
	fs.BoolVar(&s.keeper, "keeper", false, "spec is a ClickHouseKeeperInstallation (CHK): renders the merged single file, no users render")
	fs.IntVar(&s.serverID, "server-id", 0, "keeper mode: overrides the rendered <server_id> (e.g. the myid file's 1-based value; 0 keeps the CR-derived replica index)")
	fs.StringVar(&s.singleFile, "single-file", "", "keeper mode: destination of the ONE complete <clickhouse>-rooted keeper config (required in keeper mode)")
	fs.Var(&s.hostFlags, "host", "host this node renders (bare name); with name=address syntax (repeatable) adds the cloud inventory entry")
	fs.Var(&s.replicaMap, "replica", "CR host name to cloud short hostname mapping name=cloudname (repeatable; the rendered macros <replica> carries the cloud name)")
	fs.Var(&s.keeperAddrs, "keeper-address", "CHK host cloud DNS mapping crName=address (repeatable; raft server entries use the cloud names)")
	fs.Var(&s.raftIDList, "raft-id", "keeper mode: CR host name to raft_configuration <server><id> mapping name=id (repeatable)")
	fs.Var(&s.userPasswords, "user-password", "user name=password-file path (repeatable); read, sha256-hashed and injected as password_sha256_hex for that user")
	fs.StringVar(&s.remoteUser, "remote-user", "", "ClickHouse user distributed queries authenticate as (with -remote-password-env/-remote-password-file)")
	fs.StringVar(&s.remotePasswordEnv, "remote-password-env", "", "environment variable holding the distributed-query password")
	fs.StringVar(&s.remotePasswordFile, "remote-password-file", "", "file holding the distributed-query password")
	fs.Var(&s.networks, "network", "user=ip users.d allowlist entry (repeatable; localhost is always appended)")
	fs.StringVar(&s.secrets, "secrets", "aws", "secret source for passwordFrom refs: aws (Secrets Manager) | none (fail on refs)")
	fs.StringVar(&s.region, "region", "", "cloud region (empty = SDK default chain)")
	return fs, s
}

// runRenderNode implements `chctl config render-node`: ONE invocation
// renders EVERYTHING for a node and installs it with correct ownership
// and permissions — the host-selected sections AND the inventory
// sections AND the users.d files (or, for a keeper node, the merged
// single config file) — replacing the several render calls plus
// per-site prune lists and chmod/chown lines the calling stack
// previously assembled (one site forgetting the chmod being the live
// root-owned-0640 crash class).
//
// Everything renders into memory first; the install writes staged files
// and renames them into place, then applies the exact mode and the
// service-account ownership. A render or validation failure installs
// NOTHING and names the offending section. -check computes the same
// install plan and diffs it against the installed tree without writing;
// a non-empty drift exits 4 with the per-file delta.
func runRenderNode(args []string) error {
	fs, s := newRenderNodeFlagSet()
	// The pre-scan keeps flags and positionals order-independent for
	// this subcommand too (a stray positional is a usage error below).
	flagArgs, positionals := separatePositionals(fs, args)
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	if len(positionals) != 0 {
		return fmt.Errorf("usage: chctl config render-node -spec chi.yaml -out config.d ... (no positional argument; got %v)", positionals)
	}
	if s.specFile == "" {
		return fmt.Errorf("-spec is required")
	}
	if s.outDir == "" {
		return fmt.Errorf("-out is required (the config.d directory)")
	}
	if s.check && s.dryRun {
		return fmt.Errorf("-check and -dry-run are mutually exclusive: -check already writes nothing")
	}
	if s.nodeType == "" {
		return fmt.Errorf("-node-type is required: writer|reader|backup|keeper")
	}
	switch s.nodeType {
	case "writer", "reader", "backup", "keeper":
	default:
		return fmt.Errorf("-node-type must be writer|reader|backup|keeper, got %q", s.nodeType)
	}
	if s.keeper && s.nodeType != "keeper" {
		return fmt.Errorf("-keeper with -node-type %q: a CHK spec renders keeper nodes only", s.nodeType)
	}
	if !s.keeper && s.nodeType == "keeper" {
		return fmt.Errorf("-node-type keeper requires -keeper (the spec is a ClickHouseKeeperInstallation)")
	}
	// -users-dir defaults to the users.d sibling of -out — the stack's
	// standard layout (/etc/clickhouse-server/{config.d,users.d}).
	if s.usersDir == "" {
		s.usersDir = filepath.Join(filepath.Dir(s.outDir), "users.d")
	}
	if s.keeper && s.singleFile == "" {
		return fmt.Errorf("keeper mode requires -single-file (the keeper's --config destination)")
	}
	if !s.keeper && s.singleFile != "" {
		return fmt.Errorf("-single-file applies to keeper mode only: add -keeper")
	}
	if s.serverID < 0 {
		return fmt.Errorf("-server-id must be positive, got %d", s.serverID)
	}
	if !s.keeper {
		if s.serverID != 0 || len(s.raftIDList.values) > 0 || len(s.keeperAddrs.values) > 0 {
			return fmt.Errorf("-server-id/-raft-id/-keeper-address apply to keeper mode only: add -keeper")
		}
	}

	// Distributed-query credentials: resolved once, from exactly one
	// source, before anything renders (see readRemotePassword).
	remotePassword, err := readRemotePassword(s.remoteUser, s.remotePasswordEnv, s.remotePasswordFile)
	if err != nil {
		return err
	}

	hostName, hosts, err := parseRenderHosts(s.hostFlags.values)
	if err != nil {
		return err
	}
	if hostName == "" {
		return fmt.Errorf("render-node requires a host: pass -host (bare name or name=address)")
	}
	replicas := make(map[string]string, len(s.replicaMap.values))
	for _, entry := range s.replicaMap.values {
		name, cloudName, ok := strings.Cut(entry, "=")
		if !ok || name == "" || cloudName == "" {
			return fmt.Errorf("-replica %q must be name=cloudname", entry)
		}
		replicas[name] = cloudName
	}
	userPasswordHashes, err := readUserPasswordHashes(s.userPasswords.values)
	if err != nil {
		return err
	}
	if s.keeper && len(userPasswordHashes) > 0 {
		return fmt.Errorf("-user-password is not applicable to a CHK spec (keeper installations carry no ClickHouse users)")
	}
	nets := make(map[string][]string, len(s.networks.values))
	for _, entry := range s.networks.values {
		user, ip, ok := strings.Cut(entry, "=")
		if !ok || user == "" || ip == "" {
			return fmt.Errorf("-network %q must be user=ip", entry)
		}
		nets[user] = append(nets[user], ip)
	}

	// Secrets resolver for passwordFrom refs: AWS Secrets Manager by
	// default; "none" renders only specs without references (the
	// offline mode the validate harness uses).
	var resolver port.SecretsResolver
	switch s.secrets {
	case "aws":
		resolver = secretSourceAdapter{awsinfra.NewSecretsManagerSource(s.region)}
	case "none":
		resolver = refusingResolver{}
	default:
		return fmt.Errorf("unknown -secrets backend %q (aws|none)", s.secrets)
	}

	specData, err := os.ReadFile(s.specFile)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}
	specRef := domain.SpecReference{Data: specData}
	loader := operator.NewSpecLoader()
	ctx := context.Background()

	if s.keeper {
		raftIDs, err := parseRaftIDFlags(s.raftIDList.values)
		if err != nil {
			return err
		}
		for name, id := range raftIDs {
			if id <= 0 {
				return fmt.Errorf("-raft-id %s=%d: raft ids must be positive", name, id)
			}
		}
		cr, err := loader.LoadCHK(ctx, port.LoadCHKRequest{Spec: specRef})
		if err != nil {
			return err
		}
		keeperMap := map[string]string{}
		for _, entry := range s.keeperAddrs.values {
			name, address, ok := strings.Cut(entry, "=")
			if !ok || name == "" || address == "" {
				return fmt.Errorf("-keeper-address %q must be crName=address", entry)
			}
			keeperMap[name] = address
		}
		result, err := operator.NewNodeRenderer(operator.NewConfigRenderer(), nil).RenderNode(ctx, port.RenderNodeRequest{
			NormalizedCR:    cr,
			Keeper:          true,
			NodeType:        s.nodeType,
			HostName:        hostName,
			Hosts:           hosts,
			KeeperAddresses: keeperMap,
			ServerID:        s.serverID,
			RaftIDs:         raftIDs,
			SingleFilePath:  s.singleFile,
			OutputDir:       s.outDir,
			OwnerUser:       s.owner,
			DryRun:          s.dryRun,
			Check:           s.check,
		})
		if err != nil {
			return err
		}
		printRenderNodeResult(os.Stdout, result)
		return renderNodeDrift(result)
	}

	cr, err := loader.LoadCHI(ctx, port.LoadCHIRequest{Spec: specRef})
	if err != nil {
		return err
	}
	result, err := operator.NewNodeRenderer(operator.NewConfigRenderer(), operator.NewUsersRenderer(resolver)).RenderNode(ctx, port.RenderNodeRequest{
		NormalizedCR:   cr,
		NodeType:       s.nodeType,
		HostName:       hostName,
		Hosts:          hosts,
		Replicas:       replicas,
		UserPasswords:  userPasswordHashes,
		RemoteUser:     s.remoteUser,
		RemotePassword: remotePassword,
		Networks:       nets,
		OutputDir:      s.outDir,
		UsersDir:       s.usersDir,
		OwnerUser:      s.owner,
		DryRun:         s.dryRun,
		Check:          s.check,
	})
	if err != nil {
		return err
	}
	printRenderNodeResult(os.Stdout, result)
	return renderNodeDrift(result)
}

// renderNodeDrift converts a check-mode drift into the exit-4 error
// (scripts key on the code, and the delta lines are already on stdout).
func renderNodeDrift(result port.RenderNodeResult) error {
	if !result.Check || len(result.Drift) == 0 {
		return nil
	}
	return &driftError{drifted: len(result.Drift), total: len(result.Files)}
}

// printRenderNodeResult prints the render-node summary — the shape of
// printRendered, extended with the install mode each file receives and
// the plan's prune/check annotations. Machine-greppable: one line per
// file ("group: path mode"), summary lines at the end.
func printRenderNodeResult(w io.Writer, result port.RenderNodeResult) {
	for _, file := range result.Files {
		fmt.Fprintf(w, "%s: %s %04o\n", file.Group, file.Path, file.Mode.Perm())
	}
	for _, path := range result.Pruned {
		fmt.Fprintf(w, "prune: %s\n", path)
	}
	for _, line := range result.Drift {
		fmt.Fprintf(w, "drift: %s\n", line)
	}
	switch {
	case result.Check:
		fmt.Fprintf(w, "check: %d file(s), %d drift\n", len(result.Files), len(result.Drift))
	case result.DryRun:
		fmt.Fprintf(w, "dry-run: %d file(s), nothing written\n", len(result.Files))
	default:
		fmt.Fprintf(w, "render-node(%s): %d file(s) installed\n", result.NodeType, len(result.Files))
	}
}

// splitSubcommand validates and strips the sub-word (e.g. "sync" from
// `chctl schema sync -flags...`). Go's flag package stops parsing at the
// first positional argument, so leaving the sub-word in would silently
// swallow every flag after it — the bug the live Phase 0 harness caught
// (`-cluster is required` despite the flag being passed).
func splitSubcommand(command string, args []string) (string, []string, error) {
	if len(args) == 0 {
		return "", nil, fmt.Errorf("usage: chctl %s <subcommand> ...", command)
	}
	return args[0], args[1:], nil
}

// boolFlagger is the flag package's own marker for flags that take no
// separate value (the flag package's IsBoolFlag). Everything else —
// string flags, int flags, and repeatable fs.Var flags like -host —
// consumes the next token as its value.
type boolFlagger interface {
	IsBoolFlag() bool
}

// separatePositionals pre-scans raw args against a constructed FlagSet and
// returns (flagsOnly, positionals): the flag tokens plus their consumed
// values in order, and every token that is neither a flag nor a consumed
// value.
//
// Why the pre-scan: Go's flag package stops parsing at the first
// non-flag token, so `chctl replica drop reader-0 -cluster x` fails with
// "-cluster is required" even though the flag was passed (the live
// finding). The CLI accepts flags and positionals in any order by
// stripping the positionals BEFORE fs.Parse. The FlagSet is never
// executed against here — parsing (and its errors) stay with fs.Parse.
//
// The scan is conservative and shape-driven:
//   - "--" ends flag scanning; everything after it is positional.
//   - "--flag" and "-flag" forms are recognized; a token that is not a
//     known flag name, not a consumed value, and not "-" or "--" is
//     positional.
//   - "--flag=value" and "-flag=value" self-contain their value.
//   - bool flags take no value; everything else consumes the next token
//     (flag.Value's IsBoolFlag is the marker — repeatable fs.Var flags
//     like -host consume a value too).
//   - an unknown flag-looking token (e.g. "-cluster" with a typo) is
//     treated as a flag and left for fs.Parse to reject — errors keep
//     their existing messages and surface before any positional
//     validation runs.
//   - a consumed value that looks like a name (e.g. `-user admin`) is
//     skipped as a value, never mistaken for a positional.
//
// Without it: every positional-after-flags command line that worked in
// practice breaks the moment a script puts the host first — with no
// remedy other than re-ordering the caller.
func separatePositionals(fs *flag.FlagSet, args []string) (flagsOnly []string, positionals []string) {
	// flag.Value kinds: bool flags take no value; everything else
	// (string, int, duration, fs.Var repeatables) consumes the next
	// token.
	consumesValue := make(map[string]bool)
	fs.VisitAll(func(f *flag.Flag) {
		if bf, ok := f.Value.(boolFlagger); ok && bf.IsBoolFlag() {
			consumesValue[f.Name] = false
			return
		}
		consumesValue[f.Name] = true
	})

	flagsOnly = make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		token := args[i]
		if token == "--" {
			// Conventional terminator: everything after is positional.
			flagsOnly = append(flagsOnly, args[i:]...)
			positionals = append(positionals, args[i+1:]...)
			return flagsOnly, positionals
		}
		if !strings.HasPrefix(token, "-") || token == "-" {
			positionals = append(positionals, token)
			continue
		}
		name := strings.TrimLeft(token, "-")
		if eq := strings.IndexByte(name, '='); eq >= 0 {
			// Self-contained value: the whole token stays a flag token.
			flagsOnly = append(flagsOnly, token)
			continue
		}
		flagsOnly = append(flagsOnly, token)
		if consumesValue[name] {
			// The value may be a "-"-prefixed negative number or a name
			// that looks like a flag: consumed unconditionally, matching
			// the flag package's own behavior.
			if i+1 < len(args) {
				i++
				flagsOnly = append(flagsOnly, args[i])
			}
		}
	}
	return flagsOnly, positionals
}

// runUsers implements `chctl users render`.
//
// Renders users.d XML from the CHI spec, resolving passwordFrom secret
// references via the cloud secret store (AWS Secrets Manager by default;
// -secrets flag selects none for offline render of hash-only users).
// Cloud network CIDRs arrive as -network user=ip flags — never spec
// content.
func runUsers(args []string) error {
	if len(args) == 0 || args[0] != "render" {
		return fmt.Errorf("usage: chctl users render -spec chi.yaml -out users.d [-network user=ip]... [-secrets aws|none] [-region r]")
	}
	fs := flag.NewFlagSet("users render", flag.ContinueOnError)
	var (
		specFile string
		outDir   string
		region   string
		secrets  string
		networks portNetworkList
	)
	fs.StringVar(&specFile, "spec", "", "CHI spec file (YAML), required")
	fs.StringVar(&outDir, "out", "/etc/clickhouse-server/users.d", "users.d output directory")
	fs.StringVar(&region, "region", "", "cloud region (empty = SDK default chain)")
	fs.StringVar(&secrets, "secrets", "aws", "secret source: aws (Secrets Manager via instance role) | none (fail on passwordFrom refs)")
	fs.Var(&networks, "network", "user=ip allowlist entry (repeatable; cloud CIDRs — localhost always appended)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if specFile == "" {
		return fmt.Errorf("-spec is required")
	}

	specData, err := os.ReadFile(specFile)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}
	crAny, err := operator.NewSpecLoader().LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: specData},
	})
	if err != nil {
		return err
	}

	// Secrets resolver: AWS Secrets Manager by default; "none" renders
	// only specs without passwordFrom references (offline mode).
	var resolver port.SecretsResolver
	switch secrets {
	case "aws":
		awsSource := awsinfra.NewSecretsManagerSource(region)
		resolver = secretSourceAdapter{awsSource}
	case "none":
		resolver = refusingResolver{}
	default:
		return fmt.Errorf("unknown -secrets backend %q (aws|none)", secrets)
	}

	nets := map[string][]string{}
	for _, entry := range networks.values {
		user, ip, ok := strings.Cut(entry, "=")
		if !ok || user == "" || ip == "" {
			return fmt.Errorf("-network %q must be user=ip", entry)
		}
		nets[user] = append(nets[user], ip)
	}

	rendered, err := operator.NewUsersRenderer(resolver).RenderUsers(context.Background(), port.RenderUsersRequest{
		NormalizedCR: crAny,
		OutputDir:    outDir,
		Networks:     nets,
	})
	if err != nil {
		return err
	}
	for _, file := range rendered {
		fmt.Printf("%s: %s\n", file.Group, file.Path)
	}
	return nil
}

// keeperAddrList collects repeated -keeper-address crName=address values.
type keeperAddrList struct {
	values []string
}

// String implements flag.Value.
func (h *keeperAddrList) String() string { return strings.Join(h.values, ", ") }

// Set implements flag.Value.
func (h *keeperAddrList) Set(value string) error {
	h.values = append(h.values, value)
	return nil
}

// raftIDFlagList collects repeated -raft-id name=id values.
type raftIDFlagList struct {
	values []string
}

// String implements flag.Value.
func (r *raftIDFlagList) String() string { return strings.Join(r.values, ", ") }

// Set implements flag.Value.
func (r *raftIDFlagList) Set(value string) error {
	r.values = append(r.values, value)
	return nil
}

// parseRaftIDFlags converts the raw name=id flag values into the
// RaftIDs map. Each value must be name=id with a positive integer id;
// malformed entries are refused (a silent skip would renumber an
// ensemble's identity invisibly).
func parseRaftIDFlags(values []string) (map[string]int, error) {
	if len(values) == 0 {
		return nil, nil
	}
	raftIDs := make(map[string]int, len(values))
	for _, value := range values {
		name, idStr, ok := strings.Cut(value, "=")
		if !ok || name == "" || idStr == "" {
			return nil, fmt.Errorf("-raft-id %q must be name=id", value)
		}
		id, err := strconv.Atoi(idStr)
		if err != nil {
			return nil, fmt.Errorf("-raft-id %q must be name=id with an integer id", value)
		}
		raftIDs[name] = id
	}
	return raftIDs, nil
}

// userPasswordList collects repeated -user-password name=path values.
type userPasswordList struct {
	values []string
}

// String implements flag.Value. Never includes file contents — only the
// flag text the user already typed.
func (u *userPasswordList) String() string { return strings.Join(u.values, ", ") }

// Set implements flag.Value.
func (u *userPasswordList) Set(value string) error {
	u.values = append(u.values, value)
	return nil
}

// readUserPasswordHashes reads each -user-password secret file and hashes
// it to SHA-256 hex: name=path flag values in, name=hex out.
//
// Why hash in the app layer: password bytes must never cross the request
// boundary into the domain — only the hex does. Trailing newline(s) are
// trimmed first: cloud-init's `printf '%s\n'` writes one, and an
// untrimmed hash would silently set a different password than the one
// the stack stored.
//
// Without it: the password would have to live in the spec file as a
// password_sha256_hex literal — a runtime secret in version control,
// precisely what the cloud secret-store + per-role 600 files exist to
// prevent. Error messages carry the file path (already known to the
// operator), never the password bytes.
func readUserPasswordHashes(entries []string) (map[string]string, error) {
	if len(entries) == 0 {
		return nil, nil
	}
	hashes := make(map[string]string, len(entries))
	for _, entry := range entries {
		name, path, ok := strings.Cut(entry, "=")
		if !ok || name == "" || path == "" {
			return nil, fmt.Errorf("-user-password %q must be name=path", entry)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("-user-password %s: read %s: %w", name, path, err)
		}
		password := strings.TrimRight(string(data), "\r\n")
		if password == "" {
			return nil, fmt.Errorf("-user-password %s: file %s is empty", name, path)
		}
		hash := sha256.Sum256([]byte(password))
		hashes[name] = hex.EncodeToString(hash[:])
	}
	return hashes, nil
}

// replicaList collects repeated -replica name=cloudname values.
type replicaList struct {
	values []string
}

// String implements flag.Value.
func (r *replicaList) String() string { return strings.Join(r.values, ", ") }

// Set implements flag.Value.
func (r *replicaList) Set(value string) error {
	r.values = append(r.values, value)
	return nil
}

// parseRenderHosts splits the config-render -host flag values into the
// host selector and the cloud inventory. Each value is dual-form:
//
//	0-0                → selects host "0-0" (the pre-inventory behavior —
//	                     the live harness passes this form)
//	0-0=writer-0.ip    → inventory entry: CR host "0-0" resolves to
//	                     cloud address "writer-0.ip" (repeatable)
//
// Only one bare selector is allowed; name=address entries may repeat.
func parseRenderHosts(values []string) (hostName string, hosts []domain.HostIdentity, err error) {
	hosts = make([]domain.HostIdentity, 0, len(values))
	for _, value := range values {
		name, address, ok := strings.Cut(value, "=")
		if ok {
			if name == "" || address == "" {
				return "", nil, fmt.Errorf("-host %q must be name=address or a bare host name", value)
			}
			hosts = append(hosts, domain.HostIdentity{Name: name, Address: address})
			continue
		}
		if value == "" {
			return "", nil, fmt.Errorf("-host %q must be name=address or a bare host name", value)
		}
		if hostName != "" {
			return "", nil, fmt.Errorf("only one bare -host selector allowed, got %q and %q", hostName, value)
		}
		hostName = value
	}
	return hostName, hosts, nil
}

// readRemotePassword resolves the distributed-query password for the
// config render from exactly one of -remote-password-env or
// -remote-password-file.
//
// Why: the AWS stack's remote_servers entries need <user>/<password> for
// peers to authenticate (the cluster-level <secret> is rejected by
// ClickHouse 26.8), but the password may never sit in argv, the spec, or
// a log line. Environment variable and file are the two secret-friendly
// sources cloud-init and systemd both offer.
//
// Without it: every passwordless render leaves distributed queries
// failing as "default" (PASSWORD_REQUIRED), or the password leaks into
// shell history via argv. Error messages name the flag, never the value.
func readRemotePassword(user, envVar, file string) (string, error) {
	switch {
	case user == "" && envVar == "" && file == "":
		// No credentials requested — the backwards-compatible render.
		return "", nil
	case user == "":
		return "", fmt.Errorf("-remote-password-env/-remote-password-file requires -remote-user")
	case envVar != "" && file != "":
		return "", fmt.Errorf("-remote-password-env and -remote-password-file are mutually exclusive")
	case envVar != "":
		password, ok := os.LookupEnv(envVar)
		if !ok {
			return "", fmt.Errorf("-remote-password-env: environment variable not set")
		}
		return password, nil
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			// No path echo: the flag value must not surface in errors —
			// "cannot read" plus the flag name is enough to act on.
			return "", fmt.Errorf("-remote-password-file: cannot read (missing or unreadable)")
		}
		return strings.TrimRight(string(data), "\r\n"), nil
	default:
		// -remote-user given with neither password source: without the
		// password the pair would half-authenticate.
		return "", fmt.Errorf("-remote-user requires -remote-password-env or -remote-password-file")
	}
}

// portNetworkList collects repeated -network user=ip flag values.
type portNetworkList struct {
	values []string
}

// String implements flag.Value.
func (h *portNetworkList) String() string { return strings.Join(h.values, ", ") }

// Set implements flag.Value.
func (h *portNetworkList) Set(value string) error {
	h.values = append(h.values, value)
	return nil
}

// secretSourceAdapter adapts port.SecretSource to port.SecretsResolver.
type secretSourceAdapter struct {
	inner port.SecretSource
}

// ResolveSecret implements port.SecretsResolver.
func (a secretSourceAdapter) ResolveSecret(ctx context.Context, request port.SecretRequest) (string, error) {
	return a.inner.GetSecretValue(ctx, request)
}

// refusingResolver fails every reference — the -secrets none backend.
type refusingResolver struct{}

// ResolveSecret implements port.SecretsResolver.
func (refusingResolver) ResolveSecret(ctx context.Context, request port.SecretRequest) (string, error) {
	return "", fmt.Errorf("secret resolution disabled (-secrets none): %q", request.Name)
}

// runSchema implements `chctl schema sync <host>`.
//
// The <host> positional and the flags may appear in any order (the
// pre-scan strips positionals before fs.Parse — Go's flag package stops
// at the first positional otherwise).
func runSchema(args []string) error {
	sub, rest, err := splitSubcommand("schema", args)
	if err != nil {
		return err
	}
	if sub != "sync" {
		return fmt.Errorf("unknown schema subcommand %q", sub)
	}
	fs, cluster, user, password, hosts, portNum, secure, spec, err := newSchemaFlagSet("schema sync")
	if err != nil {
		return err
	}
	var (
		retries        int
		pruneStaleSelf bool
	)
	fs.IntVar(&retries, "retries", service.DefaultSchemaSyncRetries,
		"full create passes (min 1; a pass that succeeds ends the loop early). A materialized view whose "+
			"TO-target table lands later in the same batch needs the second pass")
	fs.BoolVar(&pruneStaleSelf, "prune-stale-self", false,
		"when the create pass fails with Code 253 (this host's replica registration already exists in Keeper — "+
			"an ASG/VMSS replacement re-claiming an ordinal), drop THAT registration and retry; only the "+
			"target's own name is ever dropped")
	flagArgs, positional := separatePositionals(fs, rest)
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	parsed, err := finishSchemaFlags(fs, cluster, user, password, hosts, portNum, secure, spec)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: chctl schema sync <host-name> (got %d positional argument(s): %v)",
			len(positional), positional)
	}
	if retries < 1 {
		return fmt.Errorf("-retries must be at least 1, got %d", retries)
	}
	return newSchemaService().SyncSchema(
		context.Background(), parsed,
		domain.SchemaSyncRequest{
			HostName:       positional[0],
			Retries:        retries,
			PruneStaleSelf: pruneStaleSelf,
		},
	)
}

// runReplica implements `chctl replica drop|sync|gc`.
func runReplica(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: chctl replica <drop|sync|gc> ...")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "drop":
		return runReplicaDrop(rest)
	case "sync":
		return runReplicaSync(rest)
	case "gc":
		return runReplicaGc(rest)
	default:
		return fmt.Errorf("unknown replica subcommand %q", sub)
	}
}

// runReplicaDrop implements `chctl replica drop <host> -on <peer>`.
// Positional and flags may appear in any order.
func runReplicaDrop(args []string) error {
	fs, cluster, user, password, hosts, portNum, secure, spec, err := newSchemaFlagSet("replica drop")
	if err != nil {
		return err
	}
	var runOn string
	fs.StringVar(&runOn, "on", "", "peer host that executes the operation, required")
	flagArgs, positional := separatePositionals(fs, args)
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	parsed, err := finishSchemaFlags(fs, cluster, user, password, hosts, portNum, secure, spec)
	if err != nil {
		return err
	}
	if runOn == "" {
		return fmt.Errorf("-on is required")
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: chctl replica drop <host-name> -on <peer> (got %d positional argument(s): %v)",
			len(positional), positional)
	}
	return newSchemaService().DropReplica(
		context.Background(), parsed,
		domain.ReplicaDropRequest{HostName: positional[0], RunOn: runOn},
	)
}

// runReplicaSync implements `chctl replica sync <host>`.
// Positional and flags may appear in any order.
func runReplicaSync(args []string) error {
	fs, cluster, user, password, hosts, portNum, secure, spec, err := newSchemaFlagSet("replica sync")
	if err != nil {
		return err
	}
	flagArgs, positional := separatePositionals(fs, args)
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	parsed, err := finishSchemaFlags(fs, cluster, user, password, hosts, portNum, secure, spec)
	if err != nil {
		return err
	}
	if len(positional) != 1 {
		return fmt.Errorf("usage: chctl replica sync <host-name> (got %d positional argument(s): %v)",
			len(positional), positional)
	}
	return newSchemaService().SyncReplica(
		context.Background(), parsed, domain.ReplicaSyncRequest{HostName: positional[0]},
	)
}

// runReplicaGc implements `chctl replica gc -on <peer>`: detect stale
// replica registrations on the peer and drop them in one pass.
//
// gc is flag-only — it takes no positional. The pre-scan still runs so a
// stray positional fails with a clear usage error instead of being
// silently swallowed as a flag value.
//
// Output contract (the systemd timer logs both streams): dropped hosts
// go to stdout as TSV, one per line — the pipe format the previous
// bash's consumers already parse; per-host failures and the
// detection/drop summary go to stderr. Exit 3 when any drop failed, so
// the timer unit surfaces it while the dropped hosts remain visible.
func runReplicaGc(args []string) error {
	fs, cluster, user, password, hosts, portNum, secure, spec, err := newSchemaFlagSet("replica gc")
	if err != nil {
		return err
	}
	var (
		runOn       string
		namePattern string
		excludes    excludeList
	)
	fs.StringVar(&runOn, "on", "", "peer host that executes the operation, required")
	fs.StringVar(&namePattern, "name-pattern", "reader-*",
		"glob (path.Match: * and ? wildcards, case-sensitive) a stale host must match to be dropped; "+
			"non-matching hosts are reported but never dropped (keeps writers/keepers safe)")
	fs.Var(&excludes, "exclude",
		"host name never to drop, even when the pattern matches and it is stale (repeatable). "+
			"The liveness guard for the writer pass: chinit lists the cluster's active ordinal leases "+
			"and excludes every live writer so a mid-boot re-join never loses its registration")
	flagArgs, positional := separatePositionals(fs, args)
	if err := fs.Parse(flagArgs); err != nil {
		return err
	}
	parsed, err := finishSchemaFlags(fs, cluster, user, password, hosts, portNum, secure, spec)
	if err != nil {
		return err
	}
	if len(positional) != 0 {
		return fmt.Errorf("usage: chctl replica gc -on <peer> (no positional argument; got %v)", positional)
	}
	if runOn == "" {
		return fmt.Errorf("-on is required")
	}

	result, err := newSchemaService().GcReplica(
		context.Background(), parsed,
		domain.ReplicaGcRequest{RunOn: runOn, NamePattern: namePattern, ExcludeHosts: excludes.values},
	)
	if err != nil {
		// Exit code 3: operational failure — detection error, bad
		// flags, unreachable peer.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitOpError)
		return nil
	}

	printGcResult(os.Stdout, os.Stderr, result)
	if len(result.Failed) > 0 {
		os.Exit(exitOpError)
	}
	return nil
}

// printGcResult writes the GC outcome in the bash timer's format: dropped
// hosts as TSV to stdout (one per line), failures and the summary to
// stderr. Extracted so the output shape is unit-testable.
func printGcResult(stdout, stderr io.Writer, result *service.ReplicaGcResult) {
	for _, host := range result.Dropped {
		fmt.Fprintln(stdout, host)
	}
	for _, host := range result.Failed {
		fmt.Fprintf(stderr, "gc: drop failed for %q\n", host)
	}
	fmt.Fprintf(stderr, "gc: %d stale replica(s) detected, %d dropped, %d failed\n",
		len(result.Stale), len(result.Dropped), len(result.Failed))
}

// runKeeper implements `chctl keeper safe-to-stop|mntr`.
func runKeeper(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: chctl keeper <safe-to-stop|mntr> ...")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "safe-to-stop":
		return runKeeperSafeToStop(rest)
	case "mntr":
		return runKeeperMntr(rest)
	default:
		return fmt.Errorf("unknown keeper subcommand %q", sub)
	}
}

// runKeeperSafeToStop implements `chctl keeper safe-to-stop`.
func runKeeperSafeToStop(args []string) error {
	fs := flag.NewFlagSet("keeper safe-to-stop", flag.ContinueOnError)
	var (
		keeperAddr string
		members    int
		pollIn     string
		waitIn     string
	)
	fs.StringVar(&keeperAddr, "keeper", "", "leader/voting-member mntr address (host:port), required")
	fs.IntVar(&members, "members", 0, "ensemble size (0 = derive from mntr voting membership)")
	fs.StringVar(&pollIn, "poll-interval", "5s", "poll interval while waiting for quorum headroom")
	fs.StringVar(&waitIn, "wait-timeout", "2m", "total wait budget before refusing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if keeperAddr == "" {
		return fmt.Errorf("-keeper is required")
	}
	poll, err := time.ParseDuration(pollIn)
	if err != nil {
		return fmt.Errorf("parse -poll-interval: %w", err)
	}
	wait, err := time.ParseDuration(waitIn)
	if err != nil {
		return fmt.Errorf("parse -wait-timeout: %w", err)
	}

	svc := service.NewKeeperSafetyService(keeper.NewProbe())
	err = svc.SafeToStop(context.Background(), service.SafeToStopRequest{
		KeeperAddress:  keeperAddr,
		Members:        members,
		PollInterval:   poll,
		WaitTimeout:    wait,
		TargetIsSynced: true,
	})
	if err == nil {
		fmt.Println("safe to stop: ensemble retains Raft quorum")
		return nil
	}
	if errors.Is(err, service.ErrNotSafeToDisrupt) {
		// Exit code 2: the gate held the node — the hook should
		// abandon/heartbeat, not alarm.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitQuorumHeld)
	}
	// Exit code 3: operational failure — probe unreachable, bad flags.
	fmt.Fprintln(os.Stderr, err)
	os.Exit(exitOpError)
	return nil
}

// runKeeperMntr implements `chctl keeper mntr`: fetch the mntr payload
// and print it as sorted key=value lines (or one JSON object with
// -json). Replaces `echo mntr | nc host 9181 | awk` parsing in node
// bootstrap and health scripts; exit codes match safe-to-stop so hooks
// need no special-casing (0 healthy, 3 keeper unreachable/empty).
func runKeeperMntr(args []string) error {
	fs := flag.NewFlagSet("keeper mntr", flag.ContinueOnError)
	var (
		keeperAddr string
		timeoutIn  string
		asJSON     bool
	)
	fs.StringVar(&keeperAddr, "keeper", "", "mntr address (host:port), required")
	fs.StringVar(&timeoutIn, "timeout", "5s", "mntr query timeout")
	fs.BoolVar(&asJSON, "json", false, "emit one JSON object instead of key=value lines")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if keeperAddr == "" {
		return fmt.Errorf("usage: chctl keeper mntr -keeper <host:port> [-timeout 5s] [-json]")
	}
	timeout, err := time.ParseDuration(timeoutIn)
	if err != nil {
		return fmt.Errorf("parse -timeout: %w", err)
	}
	if timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, got %s", timeout)
	}

	svc := service.NewKeeperMntrService(&keeper.Probe{Timeout: timeout})
	result, err := svc.Mntr(context.Background(), service.MntrRequest{
		KeeperAddress: keeperAddr,
	})
	if err != nil {
		// Exit code 3: operational failure — keeper down or empty
		// response; scripts read this as "unhealthy", never as empty
		// success.
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitOpError)
	}

	out := result.FormatLines()
	if asJSON {
		out, err = result.FormatJSON()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(exitOpError)
		}
	}
	fmt.Println(out)
	return nil
}

// runHealth implements `chctl health`: the one verification command the
// stack gates boots, refreshes and runbooks on — server reachability,
// replica writability, keeper mntr, spec tables, self registration.
//
// Check fan-out: the server check pings EVERY inventory host (a cluster
// health command must not inherit a load balancer's blind spot) and
// reports each down host by name. The replicas fan-out runs on the
// first reachable host via clusterAllReplicas with
// skip_unavailable_shards=1 — down peers are already named by the
// server check, so the sweep reports the rest.
//
// Exit codes (the safe-to-stop contract): 0 every executed check
// passed (skips don't count — an optional check that was not requested
// is not a failure); 3 any check degraded; 1 usage error. Scripts read
// 3 as "unhealthy", never as empty success.
//
// -spec is OPTIONAL here (unlike schema/replica commands): without it
// the tables check is skipped; with it the tables the spec's .sql files
// declare are verified present and the -cluster name is whitelisted
// against the CR (the fan-out SQL interpolates it — same gate as
// replica gc).
//
// -self <name> names this node's replica identity (the {replica} macro
// value, e.g. reader-0). The check reads it from the replicas fan-out
// (cluster-wide system.replicas rows), so the full inventory stays
// valid — no separate host flag needed.
func runHealth(args []string) error {
	report, err := runHealthWith(os.Stdout, args)
	if err != nil {
		return err
	}
	if !report.Healthy() {
		// Exit code 3: operational degradation — a check failed. The
		// gate contract: scripts alarm/hold, never treat as success.
		os.Exit(exitOpError)
	}
	return nil
}

// runHealthWith writes the report to w instead of stdout and returns
// the report instead of exiting — the shape the CLI tests drive (they
// assert the human/JSON output and the degraded/healthy verdict
// without an os.Exit in the test process). The exit-code decision is
// the caller's: Healthy() false maps to exit 3.
func runHealthWith(w io.Writer, args []string) (*service.HealthReport, error) {
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	var (
		cluster    string
		user       string
		password   string
		portNum    int
		secure     bool
		specFile   string
		keeperAddr string
		selfName   string
		asJSON     bool
		timeoutIn  string
	)
	hosts := &hostList{}
	fs.StringVar(&cluster, "cluster", "", "ClickHouse cluster name (remote_servers), required")
	fs.Var(hosts, "host", "cluster host as name=address (repeatable)")
	fs.StringVar(&user, "user", "default", "ClickHouse user for health queries")
	fs.StringVar(&password, "password", "", "ClickHouse password (prefer env/secrets over flags)")
	fs.IntVar(&portNum, "port", 8123, "ClickHouse HTTP port")
	fs.BoolVar(&secure, "secure", false, "use HTTPS scheme")
	fs.StringVar(&specFile, "spec", "", "CHI spec file (YAML); enables the tables-present check and gates -cluster against the CR's clusters")
	fs.StringVar(&keeperAddr, "keeper", "", "keeper mntr address host:port; enables the keeper check")
	fs.StringVar(&selfName, "self", "", "this node's replica name ({replica} macro value, e.g. reader-0); enables the self-registration check")
	fs.BoolVar(&asJSON, "json", false, `emit {"status":...,"checks":[{name,status,detail}...]} instead of the human lines`)
	fs.StringVar(&timeoutIn, "timeout", "5s", "per-probe timeout")
	flagArgs, positional := separatePositionals(fs, args)
	if err := fs.Parse(flagArgs); err != nil {
		return nil, err
	}
	if len(positional) != 0 {
		return nil, fmt.Errorf("usage: chctl health takes no positional argument (got %v)", positional)
	}
	if cluster == "" {
		return nil, fmt.Errorf("-cluster is required")
	}
	if len(hosts.values) == 0 {
		return nil, fmt.Errorf("-host <name=address> is required (the inventory to check)")
	}
	timeout, err := time.ParseDuration(timeoutIn)
	if err != nil {
		return nil, fmt.Errorf("parse -timeout: %w", err)
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("-timeout must be positive, got %s", timeout)
	}

	// Inventory: every -host must be name=address (same shape as the
	// schema/replica commands).
	inventory := make([]domain.HostIdentity, 0, len(hosts.values))
	for _, h := range hosts.values {
		name, address, ok := strings.Cut(h, "=")
		if !ok || name == "" || address == "" {
			return nil, fmt.Errorf("host %q must be name=address", h)
		}
		inventory = append(inventory, domain.HostIdentity{Name: name, Address: address})
	}

	// -spec is optional: given, the tables check runs against the
	// expected tables parsed from the CR's .sql files, and the cluster
	// name must be one the CR declares (the replicas fan-out SQL
	// interpolates it; only CR-declared names may reach the SQL).
	var normalizedCR any
	expectedTables := func() []domain.TableRef { return nil }
	if specFile != "" {
		specData, err := os.ReadFile(specFile)
		if err != nil {
			return nil, fmt.Errorf("read spec: %w", err)
		}
		normalizedCR, err = operator.NewSpecLoader().LoadCHI(context.Background(), port.LoadCHIRequest{
			Spec: domain.SpecReference{Data: specData},
		})
		if err != nil {
			return nil, err
		}
		declared, err := clusterDeclaredByCR(normalizedCR, cluster)
		if err != nil {
			return nil, err
		}
		if !declared {
			return nil, fmt.Errorf("cluster %q not declared by the CR's spec", cluster)
		}
		crTables := operator.ExpectedTablesFromCR(normalizedCR)
		expectedTables = func() []domain.TableRef { return crTables }
	}

	// -self needs no host resolution: the check consumes the replicas
	// fan-out (cluster-wide system.replicas rows), so the full
	// inventory stays valid.

	svc := service.NewHealthService(operator.NewHealthProbe(), &keeper.Probe{Timeout: timeout}, expectedTables)
	report, err := svc.Check(context.Background(), service.HealthRequest{
		ClusterName:   cluster,
		Hosts:         inventory,
		Credentials:   domain.Credentials{Username: user, Password: password},
		HTTPPort:      portNum,
		Secure:        secure,
		Timeout:       timeout,
		KeeperAddress: keeperAddr,
		CheckTables:   specFile != "",
		SelfName:      selfName,
	})
	if err != nil {
		return nil, err
	}

	if asJSON {
		out, err := json.Marshal(report.ToJSONShape())
		if err != nil {
			return nil, fmt.Errorf("marshal health report: %w", err)
		}
		fmt.Fprintln(w, string(out))
	} else {
		fmt.Fprint(w, report.FormatHuman())
	}
	return report, nil
}

// clusterDeclaredByCR reports whether the loaded CR declares the named
// cluster — the whitelist that guards every SQL interpolation of the
// cluster name (the same gate the schema runner applies).
func clusterDeclaredByCR(cr any, name string) (bool, error) {
	chi, ok := cr.(*v1.ClickHouseInstallation)
	if !ok || chi == nil {
		return false, fmt.Errorf("normalized CR is not a ClickHouseInstallation")
	}
	for _, cluster := range chi.GetSpecT().GetConfiguration().GetClusters() {
		if cluster != nil && cluster.Name == name {
			return true, nil
		}
	}
	return false, nil
}

// schemaFlagSet holds the parsed pointers of the shared schema flags.
type schemaFlagSet struct {
	fs       *flag.FlagSet
	cluster  *string
	user     *string
	password *string
	hosts    *hostList
	portNum  *int
	secure   *bool
}

// newSchemaFlagSet builds the shared cluster-context flag set for schema
// and replica operations. Returns the set and its bound variables.
func newSchemaFlagSet(name string) (*flag.FlagSet, *string, *string, *string, *hostList, *int, *bool, *string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	cluster := fs.String("cluster", "", "ClickHouse cluster name (remote_servers), required")
	user := fs.String("user", "default", "ClickHouse user for schema operations")
	password := fs.String("password", "", "ClickHouse password (prefer env/secrets over flags)")
	hosts := &hostList{}
	fs.Var(hosts, "host", "cluster host as name=address (repeatable)")
	portNum := fs.Int("port", 8123, "ClickHouse HTTP port")
	secure := fs.Bool("secure", false, "use HTTPS scheme")
	spec := fs.String("spec", "", "CHI spec file (YAML) — required; the schemer names replicas from it")
	return fs, cluster, user, password, hosts, portNum, secure, spec, nil
}

// finishSchemaFlags validates shared flags, loads the spec and assembles
// the request with the normalized CR.
func finishSchemaFlags(fs *flag.FlagSet, cluster, user, password *string, hosts *hostList, portNum *int, secure *bool, spec *string) (service.SchemaRequest, error) {
	if *cluster == "" {
		return service.SchemaRequest{}, fmt.Errorf("-cluster is required")
	}
	if *spec == "" {
		return service.SchemaRequest{}, fmt.Errorf("-spec is required (the schemer names replicas from the CHI spec)")
	}
	specData, err := os.ReadFile(*spec)
	if err != nil {
		return service.SchemaRequest{}, fmt.Errorf("read spec: %w", err)
	}
	crAny, err := operator.NewSpecLoader().LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: specData},
	})
	if err != nil {
		return service.SchemaRequest{}, err
	}
	request := service.SchemaRequest{
		ClusterName:  *cluster,
		Credentials:  domain.Credentials{Username: *user, Password: *password},
		HTTPPort:     *portNum,
		Secure:       *secure,
		NormalizedCR: crAny,
	}
	for _, h := range hosts.values {
		name, address, ok := strings.Cut(h, "=")
		if !ok || name == "" || address == "" {
			return service.SchemaRequest{}, fmt.Errorf("host %q must be name=address", h)
		}
		request.Hosts = append(request.Hosts, domain.HostIdentity{Name: name, Address: address})
	}
	return request, nil
}

// hostList collects repeated -host flag values.
type hostList struct {
	values []string
}

// String implements flag.Value.
func (h hostList) String() string {
	return strings.Join(h.values, ", ")
}

// Set implements flag.Value.
// Set implements flag.Value. Pointer receiver is mandatory: a value
// receiver would append to a copy and every -host flag would silently
// vanish (the "host inventory is empty" live-harness finding).
func (h *hostList) Set(value string) error {
	h.values = append(h.values, value)
	return nil
}

// excludeList collects repeated -exclude flag values (exact host names
// the GC must never drop). Pointer receiver, same as hostList: a value
// receiver would drop every entry after the first flag.
type excludeList struct {
	values []string
}

// String implements flag.Value.
func (e excludeList) String() string {
	return strings.Join(e.values, ", ")
}

// Set implements flag.Value.
func (e *excludeList) Set(value string) error {
	e.values = append(e.values, value)
	return nil
}

// newSchemaService wires the schema domain service with the operator
// adapter.
func newSchemaService() *service.SchemaService {
	return service.NewSchemaService(operator.NewSchemaClientFactory())
}

// printRendered lists rendered files.
func printRendered(rendered []port.RenderedFile) {
	for _, file := range rendered {
		fmt.Printf("%s: %s\n", file.Group, file.Path)
	}
}
