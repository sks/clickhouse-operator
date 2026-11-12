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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/service"
	awsinfra "github.com/altinity/clickhouse-operator/pkg/chctl/infra/aws"
	"github.com/altinity/clickhouse-operator/pkg/chctl/infra/keeper"
	"github.com/altinity/clickhouse-operator/pkg/chctl/infra/operator"
)

// Exit codes for scripts and lifecycle hooks. Keeping the two failure modes
// apart stops a broken probe from being misread as "safe" or as a gate
// hold.
const (
	exitOK         = 0
	exitUsageError = 1
	exitQuorumHeld = 2
	exitOpError    = 3
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
	case "help", "-h", "--help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// usage prints the top-level help.
func usage() {
	usageWith(os.Stdout)
}

// usageWith writes the top-level help to a writer (testability).
func usageWith(w io.Writer) {
	fmt.Fprint(w, `chctl — off-cluster ClickHouse control, powered by the operator's own code

Usage:
  chctl config render -spec chi.yaml [-host 0-0] [-host name=address]... [-replica name=cloudname]...
                        [-user-password name=path]... [-out config.d] [-users-dir users.d] [-dry-run] [-keeper]
                        [-remote-user <name> -remote-password-env <VAR> | -remote-password-file <path>]
                        keeper mode: [-keeper] -host <host> [-server-id <n>] [-single-file <path>]
  chctl schema sync <host> -cluster <name> -host <name=addr>... [-user u] [-password p] [-port n] [-secure]
  chctl replica drop <host> -on <peer> -cluster <name> -host <name=addr>...
  chctl replica sync <host> -cluster <name> -host <name=addr>...
  chctl replica gc -on <peer> -cluster <name> -host <name=addr>... [-name-pattern reader-*]
  chctl keeper safe-to-stop -keeper <host:port> [-members n] [-wait-timeout 2m]
  chctl keeper mntr -keeper <host:port> [-timeout 5s] [-json]

Commands:
  config    render config.d/users.d XML for a cluster or host from a CHI/CHK spec
            -user-password injects runtime passwords: each file is read, sha256-hashed
            and emitted as password_sha256_hex for that user (the spec declares the
            user, the secret store supplies the password — never the spec file)
            -remote-user with -remote-password-env/-remote-password-file injects
            <user>/<password> into every remote_servers <replica> so
            distributed queries authenticate (never argv passwords)
            keeper mode: -single-file merges common+host sections into ONE complete
            <clickhouse>-rooted config for nodes running clickhouse-keeper with an
            explicit --config (which disables config.d auto-include); -server-id
            overrides the CR's zero-based replica index with the myid file's
            authoritative identity
  schema    create all schema objects on a host from cluster peers (new-reader boot)
  replica   drop a dead replica's registration / re-sync a restarted one / gc stale ones
  keeper    Raft-quorum safety gate for planned keeper stop/replacement,
            plus mntr payload inspection for health scripts

Docs: docs/chctl.md
`)
}

// runConfig implements `chctl config render`.
func runConfig(args []string) error {
	if len(args) == 0 || args[0] != "render" {
		return fmt.Errorf("usage: chctl config render -spec chi.yaml ...")
	}
	fs := flag.NewFlagSet("config render", flag.ContinueOnError)
	var (
		specFile   string
		outDir     string
		usersDir   string
		dryRun     bool
		isKeeper   bool
		serverID   int
		singleFile string
	)
	var (
		hostFlags          hostList
		replicaMap         replicaList
		keeperAddrs        keeperAddrList
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
	fs.BoolVar(&isKeeper, "keeper", false, "spec is a ClickHouseKeeperInstallation (CHK)")
	fs.Var(&replicaMap, "replica", "CR host name to cloud short hostname mapping name=cloudname (repeatable; the rendered macros <replica> carries the cloud name)")
	fs.Var(&keeperAddrs, "keeper-address", "CHK host cloud DNS mapping crName=address (repeatable; raft server entries use the cloud names)")
	fs.IntVar(&serverID, "server-id", 0, "keeper mode: overrides the rendered <server_id> for the selected host (e.g. the myid file's 1-based value; 0 keeps the CR-derived replica index)")
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
		// The server-id override targets the selected host's
		// <server_id> section; without a host there is no section to
		// shape.
		if serverID != 0 && hostName == "" {
			return fmt.Errorf("-server-id requires a keeper host: pass -host (name or name=address)")
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
			DryRun:          dryRun,
			KeeperAddresses: keeperMap,
			ServerID:        serverID,
			SingleFilePath:  singleFile,
		})
		if err != nil {
			return err
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
		DryRun:         dryRun,
	})
	if err != nil {
		return err
	}
	printRendered(rendered)
	return nil
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
	if err := fs.Parse(rest); err != nil {
		return err
	}
	parsed, err := finishSchemaFlags(fs, cluster, user, password, hosts, portNum, secure, spec)
	if err != nil {
		return err
	}
	positional := fs.Args()
	if len(positional) != 1 {
		return fmt.Errorf("usage: chctl schema sync <host-name>")
	}
	return newSchemaService().SyncSchema(
		context.Background(), parsed, domain.SchemaSyncRequest{HostName: positional[0]},
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
func runReplicaDrop(args []string) error {
	fs, cluster, user, password, hosts, portNum, secure, spec, err := newSchemaFlagSet("replica drop")
	if err != nil {
		return err
	}
	var runOn string
	fs.StringVar(&runOn, "on", "", "peer host that executes the operation, required")
	if err := fs.Parse(args); err != nil {
		return err
	}
	parsed, err := finishSchemaFlags(fs, cluster, user, password, hosts, portNum, secure, spec)
	if err != nil {
		return err
	}
	if runOn == "" {
		return fmt.Errorf("-on is required")
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("usage: chctl replica drop <host-name> -on <peer>")
	}
	return newSchemaService().DropReplica(
		context.Background(), parsed,
		domain.ReplicaDropRequest{HostName: rest[0], RunOn: runOn},
	)
}

// runReplicaSync implements `chctl replica sync <host>`.
func runReplicaSync(args []string) error {
	fs, cluster, user, password, hosts, portNum, secure, spec, err := newSchemaFlagSet("replica sync")
	if err != nil {
		return err
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	parsed, err := finishSchemaFlags(fs, cluster, user, password, hosts, portNum, secure, spec)
	if err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return fmt.Errorf("usage: chctl replica sync <host-name>")
	}
	return newSchemaService().SyncReplica(
		context.Background(), parsed, domain.ReplicaSyncRequest{HostName: rest[0]},
	)
}

// runReplicaGc implements `chctl replica gc -on <peer>`: detect stale
// replica registrations on the peer and drop them in one pass.
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
	)
	fs.StringVar(&runOn, "on", "", "peer host that executes the operation, required")
	fs.StringVar(&namePattern, "name-pattern", "reader-*",
		"glob (path.Match: * and ? wildcards, case-sensitive) a stale host must match to be dropped; "+
			"non-matching hosts are reported but never dropped (keeps writers/keepers safe)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	parsed, err := finishSchemaFlags(fs, cluster, user, password, hosts, portNum, secure, spec)
	if err != nil {
		return err
	}
	if runOn == "" {
		return fmt.Errorf("-on is required")
	}

	result, err := newSchemaService().GcReplica(
		context.Background(), parsed,
		domain.ReplicaGcRequest{RunOn: runOn, NamePattern: namePattern},
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
