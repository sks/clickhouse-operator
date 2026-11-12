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
                        [-out config.d] [-users-dir users.d] [-dry-run] [-keeper]
  chctl schema sync <host> -cluster <name> -host <name=addr>... [-user u] [-password p] [-port n] [-secure]
  chctl replica drop <host> -on <peer> -cluster <name> -host <name=addr>...
  chctl replica sync <host> -cluster <name> -host <name=addr>...
  chctl replica gc -on <peer> -cluster <name> -host <name=addr>... [-name-pattern reader-*]
  chctl keeper safe-to-stop -keeper <host:port> [-members n] [-wait-timeout 2m]
  chctl keeper mntr -keeper <host:port> [-timeout 5s] [-json]

Commands:
  config    render config.d/users.d XML for a cluster or host from a CHI/CHK spec
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
		specFile string
		outDir   string
		usersDir string
		dryRun   bool
		isKeeper bool
	)
	var (
		hostFlags   hostList
		replicaMap  replicaList
		keeperAddrs keeperAddrList
	)
	fs.StringVar(&specFile, "spec", "", "CHI/CHK spec file (YAML), required")
	fs.Var(&hostFlags, "host", "render host-scoped files for this host; with name=address syntax (repeatable) also maps the CR host to its cloud DNS name/IP")
	fs.StringVar(&outDir, "out", "out", "output directory for rendered files")
	fs.StringVar(&usersDir, "users-dir", "", "output directory for users-group files (defaults to -out)")
	fs.BoolVar(&dryRun, "dry-run", false, "print rendered files without writing")
	fs.BoolVar(&isKeeper, "keeper", false, "spec is a ClickHouseKeeperInstallation (CHK)")
	fs.Var(&replicaMap, "replica", "CR host name to cloud short hostname mapping name=cloudname (repeatable; the rendered macros <replica> carries the cloud name)")
	fs.Var(&keeperAddrs, "keeper-address", "CHK host cloud DNS mapping crName=address (repeatable; raft server entries use the cloud names)")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if specFile == "" {
		return fmt.Errorf("-spec is required")
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

	specData, err := os.ReadFile(specFile)
	if err != nil {
		return fmt.Errorf("read spec: %w", err)
	}
	specRef := domain.SpecReference{Data: specData}
	loader := operator.NewSpecLoader()
	renderer := operator.NewConfigRenderer()
	ctx := context.Background()

	if isKeeper {
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
		NormalizedCR: cr,
		HostName:     hostName,
		Hosts:        hosts,
		Replicas:     replicas,
		OutputDir:    outDir,
		UsersDir:     usersDir,
		DryRun:       dryRun,
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
