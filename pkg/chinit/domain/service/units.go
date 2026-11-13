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
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/system"
)

// chinitUnit templates the recurring-job units. ExecStart is always
// chinit itself — the subcommands share the boot's provider registry,
// state file and secrets surface, so the era of embedded bash in
// ExecStart is over. Identity arrives via the state file (the
// subcommands reload it), never via sed tokens.
func (b *Boot) chinitUnit(name, description, args string) system.UnitFile {
	return system.UnitFile{
		Name: name,
		Content: fmt.Sprintf(`[Unit]
Description=%s
After=network-online.target clickhouse-server.service
Wants=network-online.target

[Service]
Type=oneshot
ExecStart=/usr/local/bin/chinit %s
TimeoutStartSec=600

[Install]
WantedBy=multi-user.target
`, description, args),
	}
}

// chinitTimer wraps a unit with a systemd timer.
func chinitTimer(name, description, calendar string) system.UnitFile {
	return system.UnitFile{
		Name: name,
		Content: fmt.Sprintf(`[Unit]
Description=%s

[Timer]
OnCalendar=%s
Persistent=true

[Install]
WantedBy=timers.target
`, description, calendar),
	}
}

// installUnits writes the recurring-job units (server nodes; keepers
// only carry the keeper-health cron):
//   - replica-sync: oneshot per boot (SYSTEM SYNC REPLICA on every
//     replicated table — operator HostSyncTables parity)
//   - schema-sync: oneshot per boot + hourly safety net (operator
//     ClusterSchemer parity, -prune-stale-self for ASG replacements)
//   - lease-renew: hourly (readers; 15-min TTL, 5-min cadence inside)
//   - keeper-health: keepers
//   - dead-replica-gc / orphaned-data-gc: writer-0 maintenance
func (b *Boot) installUnits(ctx context.Context) error {
	installer := system.NewUnitInstaller("/etc/systemd/system")
	var units []system.UnitFile

	switch b.Spec.Role {
	case model.RoleKeeper:
		units = append(units,
			b.chinitUnit("keeper-health.service", "ClickHouse Keeper ensemble health probe", "keeper-health"),
		)
	case model.RoleWriter, model.RoleReader:
		units = append(units,
			b.chinitUnit("replica-sync.service", "Post-boot replica synchronization (SYSTEM SYNC REPLICA)", "replica-sync"),
			b.chinitUnit("schema-sync.service", "ClickHouse schema auto-propagation from cluster peers", "schema-sync -retries 3 -prune-stale-self"),
			chinitTimer("schema-sync.timer", "Run schema sync periodically", "hourly"),
		)
		if b.Spec.IdentityMode == model.IdentityLease {
			units = append(units,
				b.chinitUnit("lease-renew.service", "Reader ordinal lease renewal", "lease-renew"),
				chinitTimer("lease-renew.timer", "Renew the reader ordinal lease every 5 minutes", "*-*-* *:00/5:00"),
			)
		}
		if b.Spec.Role == model.RoleWriter {
			units = append(units,
				b.chinitUnit("dead-replica-gc.service", "Dead replica GC (DROP REPLICA for gone hosts)", "dead-replica-gc"),
				chinitTimer("dead-replica-gc.timer", "Run dead-replica GC weekly", "Mon *-*-* 03:00:00"),
				b.chinitUnit("orphaned-data-gc.service", "Orphaned S3 parts GC", "orphaned-gc"),
				chinitTimer("orphaned-data-gc.timer", "Run orphaned S3 parts GC weekly", "Sun *-*-* 04:00:00"),
			)
		}
	case model.RoleBackup:
		// The backup replica needs no cluster-maintenance units.
	}

	if _, err := installer.InstallUnits(units); err != nil {
		return err
	}
	if out, err := runCommand(ctx, "systemctl", "daemon-reload"); err != nil {
		return fmt.Errorf("daemon-reload: %w: %s", err, out)
	}
	for _, u := range units {
		if strings.HasSuffix(u.Name, ".timer") {
			if out, err := runCommand(ctx, "systemctl", "enable", "--now", u.Name); err != nil {
				// BestEffort step: enable failures are loud but the
				// boot continues (the bash || echo WARNING shape).
				fmt.Fprintf(os.Stderr, "WARNING: enable %s failed: %s\n", u.Name, out)
			}
		}
	}
	return nil
}

// configureBackup writes clickhouse-backup's config.yml against the
// provider object storage and (on the backup replica) the daily timer.
func (b *Boot) configureBackup(ctx context.Context) error {
	if b.Spec.Role == model.RoleKeeper {
		return nil
	}
	if b.Spec.Backup.Bucket == "" {
		return nil // backups disabled
	}
	if _, err := os.Stat("/usr/local/bin/clickhouse-backup"); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: clickhouse-backup absent (golden AMI expected it) - backups disabled\n")
		return nil
	}
	if err := os.MkdirAll("/etc/clickhouse-backup", 0755); err != nil {
		return err
	}
	retention := b.Spec.Backup.RetentionDays
	if retention == 0 {
		retention = 7
	}
	compressionLevel := 1
	if b.Spec.Backup.CompressionFormat == "tar" || b.Spec.Backup.CompressionFormat == "none" {
		compressionLevel = 0
	}
	prefix := strings.Trim(b.Spec.Backup.Prefix, "/") + "/" + b.Spec.ClusterName

	cfg := fmt.Sprintf(`clickhouse:
  username: default
  password_file: %s/default-password
  host: localhost
  port: 9000
  data_path: /var/lib/clickhouse
  skip_tables:
    - system.*
    - information_schema.*
    - INFORMATION_SCHEMA.*

general:
  remote_storage: s3
  max_file_size: 107374182400
  disable_progress_bar: false
  backups_to_keep_local: 3
  backups_to_keep_remote: %d
  log_level: info
  upload_concurrency: 4
  download_concurrency: 4
  retries_on_failure: 3
  retries_pause: 5s

s3:
  bucket: %s
  prefix: %s
  region: %s
  # Credentials come from the instance role (SDK chain); leave unset
  compression_level: %d
  compression_format: %s
`, BootstrapDir, retention, b.Spec.Backup.Bucket, prefix, b.Identity.Region, compressionLevel, b.Spec.Backup.CompressionFormat)

	if err := os.WriteFile("/etc/clickhouse-backup/config.yml", []byte(cfg), 0600); err != nil {
		return err
	}

	// Daily backup timer on the backup replica only.
	if b.Spec.Role == model.RoleBackup || b.Spec.Backup.IsBackupReplica {
		installer := system.NewUnitInstaller("/etc/systemd/system")
		units := []system.UnitFile{
			{
				Name: "clickhouse-backup-daily.service",
				Content: `[Unit]
Description=ClickHouse Daily Backup

[Service]
Type=oneshot
ExecStart=/usr/local/bin/chinit backup-daily
User=root
`,
			},
			chinitTimer("clickhouse-backup-daily.timer", "Run ClickHouse backup daily", "*-*-* 02:00:00"),
		}
		if _, err := installer.InstallUnits(units); err != nil {
			return err
		}
		if out, err := runCommand(ctx, "systemctl", "daemon-reload"); err != nil {
			return fmt.Errorf("daemon-reload: %w: %s", err, out)
		}
		if out, err := runCommand(ctx, "systemctl", "enable", "--now", "clickhouse-backup-daily.timer"); err != nil {
			return fmt.Errorf("enable backup timer: %w: %s", err, out)
		}
	}
	return nil
}

// configureMonitoring writes the OTEL/Fluent Bit configs for remote log
// shipping (disabled silently when the spec's endpoint is empty; the
// provider-specific agent — CloudWatch on AWS, AMA on Azure — is baked
// by the image or VM extension, not by chinit).
func (b *Boot) configureMonitoring(ctx context.Context) error {
	if b.Spec.LogShipping.Endpoint == "" {
		return nil // remote shipping disabled
	}
	switch b.Spec.LogShipping.Agent {
	case "otel":
		if err := os.MkdirAll("/etc/otelcol", 0755); err != nil {
			return err
		}
		cfg := fmt.Sprintf(`receivers:
  filelog:
    include: [/var/log/clickhouse-server/*.log, /var/log/clickhouse-server/*-*.log]

processors:
  resource:
    attributes:
      - key: host.name
        value: %s
        action: upsert

exporters:
  otlphttp:
    endpoint: %s

service:
  pipelines:
    logs:
      receivers: [filelog]
      processors: [resource]
      exporters: [otlphttp]
`, b.Identity.Hostname, b.Spec.LogShipping.Endpoint)
		if err := os.WriteFile("/etc/otelcol/config.yaml", []byte(cfg), 0644); err != nil {
			return err
		}
	case "fluentbit":
		if err := os.MkdirAll("/etc/fluent-bit", 0755); err != nil {
			return err
		}
		output := b.Spec.LogShipping.FluentbitOutput
		if output == "" {
			output = "http"
		}
		cfg := fmt.Sprintf(`[SERVICE]
    Flush        5
    Daemon       Off

[INPUT]
    Name         tail
    Path         /var/log/clickhouse-server/*.log

[OUTPUT]
    Name         %s
    Match        *
    Endpoint     %s
`, output, b.Spec.LogShipping.Endpoint)
		if err := os.WriteFile("/etc/fluent-bit/fluent-bit.conf", []byte(cfg), 0644); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown log shipping agent %q (want otel or fluentbit)", b.Spec.LogShipping.Agent)
	}
	return nil
}

// installSpotWatch writes the spot-watch service (readers): a
// long-running chinit daemon that drains on the interruption notice.
func (b *Boot) installSpotWatch(ctx context.Context) error {
	if b.Spec.Role != model.RoleReader {
		return nil
	}
	watcher := chinitport.AsSpotSignals(b.Provider)
	if watcher == nil {
		fmt.Fprintf(os.Stderr, "WARNING: cloud %s has no spot-watch capability; no drain handler installed\n", b.Provider.Name())
		return nil
	}
	installer := system.NewUnitInstaller("/etc/systemd/system")
	units := []system.UnitFile{{
		Name: "chinit-spot-watch.service",
		Content: `[Unit]
Description=Spot interruption drain watcher (chinit spot-watch)
After=network-online.target clickhouse-server.service

[Service]
Type=simple
ExecStart=/usr/local/bin/chinit spot-watch
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
`,
	}}
	if _, err := installer.InstallUnits(units); err != nil {
		return err
	}
	if out, err := runCommand(ctx, "systemctl", "daemon-reload"); err != nil {
		return fmt.Errorf("daemon-reload: %w: %s", err, out)
	}
	if out, err := runCommand(ctx, "systemctl", "enable", "--now", "chinit-spot-watch.service"); err != nil {
		return fmt.Errorf("enable chinit-spot-watch: %w: %s", err, out)
	}
	return nil
}
