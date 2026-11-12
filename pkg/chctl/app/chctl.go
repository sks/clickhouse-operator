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
	"os"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/service"
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
	fmt.Print(`chctl — off-cluster ClickHouse control, powered by the operator's own code

Usage:
  chctl config render -spec chi.yaml [-host 0-0] [-out config.d] [-users-dir users.d] [-dry-run] [-keeper]
  chctl schema sync <host> -cluster <name> -host <name=addr>... [-user u] [-password p] [-port n] [-secure]
  chctl replica drop <host> -on <peer> -cluster <name> -host <name=addr>...
  chctl replica sync <host> -cluster <name> -host <name=addr>...
  chctl keeper safe-to-stop -keeper <host:port> [-members n] [-wait-timeout 2m]

Commands:
  config    render config.d/users.d XML for a cluster or host from a CHI/CHK spec
  schema    create all schema objects on a host from cluster peers (new-reader boot)
  replica   drop a dead replica's registration / re-sync a restarted one
  keeper    Raft-quorum safety gate for planned keeper stop/replacement

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
		hostName string
		outDir   string
		usersDir string
		dryRun   bool
		isKeeper bool
	)
	fs.StringVar(&specFile, "spec", "", "CHI/CHK spec file (YAML), required")
	fs.StringVar(&hostName, "host", "", "render host-scoped files for this host (empty = common+users only)")
	fs.StringVar(&outDir, "out", "out", "output directory for rendered files")
	fs.StringVar(&usersDir, "users-dir", "", "output directory for users-group files (defaults to -out)")
	fs.BoolVar(&dryRun, "dry-run", false, "print rendered files without writing")
	fs.BoolVar(&isKeeper, "keeper", false, "spec is a ClickHouseKeeperInstallation (CHK)")
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
	specRef := domain.SpecReference{Data: specData}
	loader := operator.NewSpecLoader()
	renderer := operator.NewConfigRenderer()
	ctx := context.Background()

	if isKeeper {
		cr, err := loader.LoadCHK(ctx, port.LoadCHKRequest{Spec: specRef})
		if err != nil {
			return err
		}
		rendered, err := renderer.RenderCHK(ctx, port.RenderCHKRequest{
			NormalizedCR: cr,
			HostName:     hostName,
			OutputDir:    outDir,
			DryRun:       dryRun,
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

// runReplica implements `chctl replica drop|sync`.
func runReplica(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: chctl replica <drop|sync> ...")
	}
	sub, rest := args[0], args[1:]
	switch sub {
	case "drop":
		return runReplicaDrop(rest)
	case "sync":
		return runReplicaSync(rest)
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

// runKeeper implements `chctl keeper safe-to-stop`.
func runKeeper(args []string) error {
	if len(args) == 0 || args[0] != "safe-to-stop" {
		return fmt.Errorf("usage: chctl keeper safe-to-stop -keeper <host:port>")
	}
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
	if err := fs.Parse(args[1:]); err != nil {
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
