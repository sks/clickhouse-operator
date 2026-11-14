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

// Package app implements the chinit CLI: flag parsing, subcommand
// dispatch, provider factory wiring and the boot orchestration entry.
//
// chinit IS the cloud-init stage-2 (and stage-1: it fetches its own
// bundle) for ClickHouse nodes. The binary is cloud-agnostic; the
// provider registry adapts it — detection picks the provider, and the
// optional-capability ports degrade loudly where a cloud lacks a
// feature. Recurring jobs are subcommands (schema-sync, replica-sync,
// lease-renew, spot-watch, …) so systemd units are one ExecStart line.
package app

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// Version is the chinit build version (release workflow overrides at
// build time via -ldflags "-X .../app.Version=<tag>"; locally "dev").
var Version = "dev"

// Exit codes mirror chctl's contract style: scripts and lifecycle hooks
// must tell the failure modes apart without parsing output.
const (
	exitOK           = 0
	exitUsageError   = 1
	exitBootFailed   = 2
	exitDrainFailure = 3
)

// coder is an error that dictates its own process exit code.
type coder interface {
	ExitCode() int
}

// exitCodeError carries a process exit code (a fatal boot failure is
// distinct from a usage error: the ASG's boot-loop diagnostics).
type exitCodeError struct {
	Code int
	Err  error
}

func (e *exitCodeError) Error() string { return e.Err.Error() }
func (e *exitCodeError) Unwrap() error { return e.Err }
func (e *exitCodeError) ExitCode() int { return e.Code }

// Run parses arguments and dispatches to a subcommand.
func Run(args []string) error {
	if len(args) == 0 {
		usage(os.Stdout)
		return fmt.Errorf("no command given")
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "boot":
		return runBoot(rest)
	case "schema-sync":
		return runSchemaSync(rest)
	case "replica-sync":
		return runReplicaSync(rest)
	case "lease-renew":
		return runLeaseRenew(rest)
	case "keeper-health":
		return runKeeperHealth(rest)
	case "dead-replica-gc":
		return runDeadReplicaGc(rest)
	case "orphaned-gc":
		return runOrphanedGc(rest)
	case "backup-daily":
		return runBackupDaily(rest)
	case "spot-watch":
		return runSpotWatch(rest)
	case "drain":
		return runDrain(rest)
	case "version":
		fmt.Printf("chinit %s\n", Version)
		return nil
	case "help", "-h", "--help":
		usage(os.Stdout)
		return nil
	default:
		usage(os.Stderr)
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// Main is the process entry: prints the error, honors exit codes.
func Main(args []string) {
	// Structured logging on every entry path (boot + all subcommands):
	// INFO records land on stdout, WARN/ERROR on stderr — the levels
	// the journal contract already splits on.
	InitLogging()
	if err := Run(args); err == nil {
		return
	} else {
		fmt.Fprintln(os.Stderr, err)
		var coded coder
		if errors.As(err, &coded) {
			os.Exit(coded.ExitCode())
		}
		var fatal *fail.FatalError
		if errors.As(err, &fatal) {
			os.Exit(exitBootFailed)
		}
		os.Exit(exitUsageError)
	}
}

// usage prints the help.
func usage(w *os.File) {
	fmt.Fprintf(w, `chinit %s — cloud-adaptive ClickHouse node bootstrapper

chinit replaces the cloud-init bash script: it detects the cloud it runs
on (or takes -cloud), fetches its config bundle, converges the node to
the BootSpec (chinit.yaml) and installs its own recurring jobs as
subcommands.

Usage:
  chinit boot [-spec chinit.yaml] [-cloud aws|azure] [-dry-run]
  chinit schema-sync [-retries N] [-prune-stale-self]
  chinit replica-sync
  chinit lease-renew
  chinit keeper-health
  chinit dead-replica-gc
  chinit orphaned-gc
  chinit backup-daily
  chinit spot-watch
  chinit drain [-reason spot|termination-hook] [-timeout seconds]
  chinit version

The spec is chinit.yaml (fetched with the config bundle when booted via
cloud-init; -spec points at it for manual runs). Identity and state live
in /var/lib/chinit/state.json — subcommands reload it, never re-derive.
`, Version)
}

// bootFlags is the `boot` flag surface.
type bootFlags struct {
	specPath  string
	cloud     string
	dryRun    bool
	statePath string
}

func parseBootFlags(args []string) (*bootFlags, error) {
	fs := flag.NewFlagSet("chinit boot", flag.ContinueOnError)
	f := &bootFlags{}
	fs.StringVar(&f.specPath, "spec", "/var/lib/clickhouse-bootstrap/chinit.yaml", "path to chinit.yaml")
	fs.StringVar(&f.cloud, "cloud", "", "cloud provider override (aws|azure); empty = detect")
	fs.BoolVar(&f.dryRun, "dry-run", false, "validate the spec and print the plan without acting")
	fs.StringVar(&f.statePath, "state", "/var/lib/chinit/state.json", "state file path")
	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() > 0 {
		return nil, fmt.Errorf("unexpected positional arguments: %v", fs.Args())
	}
	return f, nil
}
