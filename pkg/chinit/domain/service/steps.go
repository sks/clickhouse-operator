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

package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	chctlport "github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/system"
)

// Retry policies: named once, referenced by step (the bash script's 16
// ad-hoc loops become four deliberate shapes).
var (
	// apiRetry: transient API failures (throttling, DNS, IMDS lag).
	apiRetry = Policy{Attempts: 5, BaseDelay: 2 * time.Second, MaxDelay: 10 * time.Second}
	// bootWait: waiting on the system (services, disks) — patient.
	bootWait = Policy{Attempts: 30, BaseDelay: 2 * time.Second, MaxDelay: 10 * time.Second}
	// onceFatal: config-class operations where a retry masks a bug
	// (SHA mismatch, malformed XML): one attempt, no retry.
	onceFatal = Policy{Attempts: 1}
	// claimSweep: bounded by one lease window (16 min > 15-min TTL).
	claimSweep = Policy{Attempts: 3, BaseDelay: 5 * time.Second, MaxDelay: 60 * time.Second, Deadline: 16 * time.Minute}
)

// Steps builds the ordered boot pipeline. The order is load-bearing and
// documented inline where a finding pinned it; the engine adds resume,
// retry classification and the fatal/best-effort split.
func (b *Boot) Steps(ctx context.Context) ([]Step, error) {
	runner := system.ExecRunner{}
	disks := system.NewDiskManager(runner).WithPlatform(b.Provider.Name())
	tuner := system.NewOSTuner(runner)
	configs := system.NewStaticConfigInstaller(runner).WithRole(b.Spec.Role)

	steps := []Step{
		// 1. Fetch the config bundle (replaces bootstrap.sh stage-1).
		// SHA mismatch inside is Fatal — no retry.
		StepFunc("fetch_config_bundle", Critical, apiRetry, func(ctx context.Context) error {
			_, err := b.Provider.FetchBundle(ctx, b.Spec.ConfigBundle, BootstrapDir)
			return err
		}),

		// 2-3. OS tuning + binary presence. THP/limits are convergent;
		// a missing AMI binary is Fatal (the ASG must replace the node).
		StepFunc("os_optimizations", Critical, onceFatal, func(ctx context.Context) error {
			return tuner.Tune(ctx)
		}),
		StepFunc("require_binaries", Critical, onceFatal, func(ctx context.Context) error {
			return tuner.RequireBinaries(ctx, []string{
				"clickhouse-server", "clickhouse-client", "chctl", "aws",
			})
		}),

		// 4. Mount data disks (metadata Critical; cache/keeper degrade
		// inside). EBS volumes attach asynchronously — bootWait covers
		// the window the bash script slept through.
		StepFunc("mount_data_disks", Critical, bootWait, func(ctx context.Context) error {
			_, err := disks.MountAll(ctx, *b.Spec, nil)
			return err
		}),

		// 5. Static configs: validate + install + retire overlays.
		StepFunc("install_static_configs", Critical, onceFatal, func(ctx context.Context) error {
			return configs.Install(ctx, BootstrapDir)
		}),

		// 6. Identity settles BEFORE any render (the ip-* macro bug:
		// readers must claim their ordinal before macros render). The
		// claimer is an optional capability — static identity (writers,
		// keepers, backup) needs no claim.
		StepFunc("settle_identity", Critical, claimSweep, b.settleIdentity),

		// 7. Passwords land BEFORE the render (passwordFrom refs and
		// the distributed-query password files the render consumes).
		StepFunc("retrieve_passwords", Critical, apiRetry, b.retrievePasswords),

		// 8. Render the node config — chctl's library, not its CLI.
		StepFunc("render_node_config", Critical, onceFatal, b.renderNodeConfig),

		// 9. Keeper config (keeper nodes only) + state cleanup.
		StepFunc("configure_keeper", BestEffort, onceFatal, b.configureKeeper),

		// 10. The identity contract file for legacy unit consumers.
		StepFunc("write_node_env", BestEffort, onceFatal, func(ctx context.Context) error {
			if b.Spec.Role == model.RoleKeeper {
				return nil // keepers wrote theirs in configure_keeper
			}
			return b.WriteNodeEnv(NodeEnvPath)
		}),

		// 11. Start clickhouse-server (CRITICAL; keepers skip inside).
		StepFunc("start_clickhouse", Critical, onceFatal, b.startClickhouse),

		// 12. Health gate: NON-FATAL by design — a reader booting while
		// a peer is down (fleet rolling, spot churn) must still join; a
		// fatal gate wedges the ASG in a boot loop.
		StepFunc("health_gate", BestEffort, onceFatal, b.healthGate),

		// 13. Prometheus endpoint reload + verification.
		StepFunc("prometheus_reload", BestEffort, bootWait, b.prometheusReload),

		// 14. Recurring-job units (chinit subcommands as ExecStart).
		StepFunc("install_units", BestEffort, onceFatal, b.installUnits),

		// 15. Backup (clickhouse-backup config + timer).
		StepFunc("configure_backup", BestEffort, onceFatal, b.configureBackup),

		// 16. Monitoring agents (OTEL/Fluent Bit; CloudWatch on AWS).
		StepFunc("configure_monitoring", BestEffort, onceFatal, b.configureMonitoring),

		// 17. Spot watcher service (readers).
		StepFunc("install_spot_watch", BestEffort, onceFatal, b.installSpotWatch),

		// 18. boot-finished marker (health verification greps it).
		StepFunc("boot_finished", Critical, onceFatal, func(ctx context.Context) error {
			if err := os.MkdirAll(filepath.Dir(BootFinishedMarker), 0755); err != nil {
				return err
			}
			stamp := time.Now().UTC().Format("2006-01-02T15:04:05Z")
			return os.WriteFile(BootFinishedMarker, []byte(stamp), 0644)
		}),
	}
	return steps, nil
}

// settleIdentity derives or claims the node identity and registers DNS.
// Static nodes (writers/keepers/backup) map the IP octet; lease nodes
// (readers) claim via the provider's OrdinalClaimer — the capability is
// probed once, and its absence under lease identity is Fatal (a reader
// without a claimer would boot with the ephemeral ip-* name and break
// replication identity — the exact production finding).
func (b *Boot) settleIdentity(ctx context.Context) error {
	id := model.NodeIdentity{
		Role:        b.Spec.Role,
		InstanceID:  b.Meta.InstanceID,
		PrivateIP:   b.Meta.PrivateIP,
		Region:      b.Meta.Region,
		ClusterName: b.Spec.ClusterName,
		DNSZone:     b.Spec.DNSZoneName,
		CROrdinal:   -1,
	}

	switch b.Spec.IdentityMode {
	case model.IdentityStatic:
		hostname, ordinal := StaticHostname(b.Meta.PrivateIP)
		if hostname == "" {
			return Fatal(fmt.Errorf("private IP %s does not map to a static role offset (writers +10..19, keepers +20..29, backup +30)", b.Meta.PrivateIP))
		}
		id.Hostname = hostname
		id.CROrdinal = ordinal
		// Keepers' CROrdinal is their own ordinal; server nodes get the
		// CR name from CRName() at render time.
	case model.IdentityLease:
		claimer := chinitport.AsOrdinalClaimer(b.Provider)
		if claimer == nil {
			return Fatal(fmt.Errorf("cloud %s has no ordinal-lease capability but spec demands identity_mode: lease", b.Provider.Name()))
		}
		ordinal, err := claimer.ClaimOrdinal(ctx, chinitport.ClaimRequest{
			Table:           os.Getenv("CHINIT_LEASE_TABLE"),
			Prefix:          b.Spec.ReaderHostPrefix,
			Count:           b.Spec.ReaderCount,
			InstanceID:      b.Meta.InstanceID,
			PrivateIP:       b.Meta.PrivateIP,
			LeaseSeconds:    900,
			DeadlineSeconds: 960,
		})
		if err != nil {
			return err
		}
		id.Hostname = fmt.Sprintf("%s%d", b.Spec.ReaderHostPrefix, ordinal)
		id.CROrdinal = ordinal
		id.LeaseTable = os.Getenv("CHINIT_LEASE_TABLE")
	}

	// The CR host name derives from the settled role + ordinal (writers
	// 00-N, readers 01-N; backup and keepers carry none).
	b.Identity = id
	if crName, ord := b.CRName(); crName != "" {
		id.CRName = crName
		id.CROrdinal = ord
	}

	// Set the hostname (static and transient) so macros/replication see
	// the settled name. Best-effort per the bash script (hostnamectl
	// can fail inside cloud-init contexts) but a missing identity is
	// Fatal — this is the cluster's view of who we are.
	b.Identity = id
	if b.PersistIdentity != nil {
		// Persist the settled identity IMMEDIATELY (before renders and
		// the server start): a crash later must not lose it, and the
		// job subcommands read it from the state file.
		b.PersistIdentity(id)
	}
	if err := setSystemHostname(ctx, id.Hostname); err != nil {
		// Best-effort (hostnamectl can fail inside cloud-init); the
		// identity itself is settled and persisted.
		fmt.Fprintf(os.Stderr, "WARNING: could not set system hostname to %s (%s)\n", id.Hostname, err)
	}

	// DNS registration: optional capability, best-effort with a loud
	// warning (the bash script's `|| echo WARNING`).
	if dns := chinitport.AsDNSRegistrar(b.Provider); dns != nil {
		zoneID, err := dns.ResolveZoneID(ctx, b.Spec.DNSZoneName)
		if err == nil && zoneID != "" {
			id.ZoneID = zoneID
			b.Identity = id
			if err := dns.UpsertARecord(ctx, chinitport.ARecordRequest{
				ZoneID:     zoneID,
				ZoneName:   b.Spec.DNSZoneName,
				RecordName: id.Hostname,
				IP:         id.PrivateIP,
				TTL:        60,
			}); err != nil {
				// Best-effort: registration failure must not fail the
				// boot; the peer-hosts entries carry discovery.
				fmt.Fprintf(os.Stderr, "WARNING: DNS registration failed for %s (%s); boot continues\n", id.Hostname, err)
			}
		} else if err != nil {
			fmt.Fprintf(os.Stderr, "WARNING: zone resolution failed for %s (%s); DNS registration skipped\n", b.Spec.DNSZoneName, err)
		}
	}
	return nil
}

// retrievePasswords resolves the boot's secrets into 0600 files the
// render and the post-boot consumers read. Values never enter the
// state file or logs (the file write is the only landing zone).
func (b *Boot) retrievePasswords(ctx context.Context) error {
	secretFiles := map[string]string{
		"default": b.Spec.Secrets.DefaultPassword,
		"admin":   b.Spec.Secrets.AdminPassword,
		"writer":  b.Spec.Secrets.WriterPassword,
	}
	if err := os.MkdirAll(BootstrapDir, 0700); err != nil {
		return Fatal(fmt.Errorf("create %s: %w", BootstrapDir, err))
	}
	for role, secretName := range secretFiles {
		if secretName == "" {
			continue
		}
		value, err := b.Provider.GetSecretValue(ctx, chctlport.SecretRequest{Name: secretName})
		if err != nil {
			return err
		}
		dest := filepath.Join(BootstrapDir, role+"-password")
		if err := os.WriteFile(dest, []byte(value), 0600); err != nil {
			return Fatal(fmt.Errorf("write %s: %w", dest, err))
		}
	}
	return nil
}

// setSystemHostname sets the node's static + transient hostname so
// ClickHouse macros and Keeper replica paths see the settled cluster
// identity instead of the ephemeral cloud-assigned ip-* name (the
// "keeper exits 56" and "ip-10-0-1-101 never joined" findings). Both
// modes: --transient alone does not survive a reboot, --static alone
// can fail inside early cloud-init contexts — try both, warn, never
// fail the boot.
func setSystemHostname(ctx context.Context, hostname string) error {
	if _, err := runCommand(ctx, "hostnamectl", "set-hostname", hostname, "--static"); err != nil {
		if _, fallbackErr := runCommand(ctx, "hostname", hostname); fallbackErr != nil {
			return err
		}
	}
	_, _ = runCommand(ctx, "hostnamectl", "set-hostname", hostname, "--transient")
	return nil
}
