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
//   - lease-renew: hourly (lease-mode nodes — readers AND writers; the
//     15-min TTL gets a 5-min cadence inside)
//   - keeper-health: keepers
//   - dead-replica-gc / orphaned-data-gc: writers (the service gates
//     itself on the ordinal-0 holder — every writer installs the
//     units, whichever machine holds writer-{prefix}0 does the work)
//
// chinitUnits is the pure assembler (the matrix's test seam); this
// method only installs what it returns.
func (b *Boot) chinitUnits() []system.UnitFile {
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
		// Identity-mode gate, never a role gate: the ordinal lease is
		// claimed by BOTH ASG roles (writers since the writer-ASG
		// rollout), so lease renewal installs on any lease-mode node.
		// The subcommand renews through the state's LeaseTable/hostname
		// — role data it already carries.
		if b.Spec.IdentityMode == model.IdentityLease {
			units = append(units,
				b.chinitUnit("lease-renew.service", "Node ordinal lease renewal", "lease-renew"),
				chinitTimer("lease-renew.timer", "Renew the ordinal lease every 5 minutes", "*-*-* *:00/5:00"),
			)
		}
		if b.Spec.Role == model.RoleWriter {
			// The maintenance units install on EVERY writer: with ASG
			// writers "writer-0" is a lease ordinal (whoever holds it),
			// not a machine, so the unit set must survive ordinal
			// handoffs. runDeadReplicaGc/runOrphanedGc no-op exit 0 on
			// non-ordinal-0 holders — the timers fire everywhere, the
			// work happens on the holder.
			units = append(units,
				b.chinitUnit("dead-replica-gc.service", "Dead replica GC (ordinal-0 writer only)", "dead-replica-gc"),
				chinitTimer("dead-replica-gc.timer", "Run dead-replica GC weekly", "Mon *-*-* 03:00:00"),
				b.chinitUnit("orphaned-data-gc.service", "Orphaned S3 parts GC (ordinal-0 writer only)", "orphaned-gc"),
				chinitTimer("orphaned-data-gc.timer", "Run orphaned S3 parts GC weekly", "Sun *-*-* 04:00:00"),
			)
		}
	case model.RoleBackup:
		// The backup replica needs no cluster-maintenance units.
	}
	return units
}

func (b *Boot) installUnits(ctx context.Context) error {
	units := b.chinitUnits()
	installer := system.NewUnitInstaller("/etc/systemd/system")
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
// shipping (disabled silently when the spec's endpoint is empty; on AWS
// the local CloudWatch agent is configured separately by
// configureCloudWatch — the image only installs the deb, chinit owns
// the config).
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
//
// Reader-only BY DESIGN: writers are on-demand ASG members with no
// spot interruption endpoint to watch — their scale-in drain arrives
// through the ASG termination lifecycle hook (EventBridge → drain
// Lambda → SSM `chinit drain`), not a 2-minute notice.
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

// cloudWatchConfig is the pure config-shape builder configureCloudWatch
// writes to disk — split out so the contracts (role gating, namespace
// split, identity-label placement) are testable without executing
// system commands.
type cloudWatchConfig struct {
	PromCfg         string
	AgentJSON       string
	ChNamespace     string
	MetricSelectors string
}

// cloudWatchConfig renders the agent's prometheus scrape + JSON config
// for a role. Custom identity labels live ONLY in the Prometheus EMF
// dimensions: append_dimensions with custom keys fails agent config
// validation on EC2, so node-metric alarms must key on the
// auto-appended InstanceID dimension instead.
func (b *Boot) cloudWatchConfig(role model.Role, cluster string) cloudWatchConfig {
	cfgDir := "/opt/aws/amazon-cloudwatch-agent/etc"

	// Exact exposition names from ClickHouse 26.8 :9363/metrics — the
	// operator's alert-catalog thresholds key on these. Keepers have no
	// server logs and servers no keeper logs; pointing the agent at
	// files a role cannot exist with makes it retry forever.
	chNamespace := "ClickHouse/Server"
	metricSelectors := `^ClickHouse(AsyncMetrics_)?(Uptime|ReplicasMaxAbsoluteDelay|MaxPartCountForPartition)$|^ClickHouseMetrics_(ReadonlyReplica|DistributedFilesToInsert|Query)$`
	logEntries := `,
                    {
                        "file_path": "/var/log/clickhouse-server/clickhouse-server.log",
                        "log_group_name": "/clickhouse/` + cluster + `/server",
                        "log_stream_name": "{instance_id}"
                    },
                    {
                        "file_path": "/var/log/clickhouse-server/clickhouse-server.err.log",
                        "log_group_name": "/clickhouse/` + cluster + `/error",
                        "log_stream_name": "{instance_id}"
                    }`
	if role == model.RoleKeeper {
		chNamespace = "ClickHouse/Keeper"
		metricSelectors = `^ClickHouse(AsyncMetrics_)?Keeper[A-Za-z]*$|^ClickHouseProfileEvents_KeeperSnapshot[A-Za-z]*$|^ClickHouseMetrics_KeeperOutstandingRequests$`
		logEntries = `,
                    {
                        "file_path": "/var/log/clickhouse-keeper/clickhouse-keeper.log",
                        "log_group_name": "/clickhouse/` + cluster + `/keeper",
                        "log_stream_name": "{instance_id}"
                    },
                    {
                        "file_path": "/var/log/clickhouse-keeper/clickhouse-keeper.err.log",
                        "log_group_name": "/clickhouse/` + cluster + `/keeper",
                        "log_stream_name": "{instance_id}"
                    },
                    {
                        "file_path": "/var/log/keeper_health.log",
                        "log_group_name": "/clickhouse/` + cluster + `/keeper",
                        "log_stream_name": "{instance_id}"
                    }`
	}

	promCfg := `global:
  scrape_interval: 60s
  scrape_timeout: 10s
scrape_configs:
  - job_name: clickhouse
    static_configs:
      - targets:
          - 127.0.0.1:9363
        labels:
          ClusterName: "` + cluster + `"
          NodeType: "` + string(role) + `"
`
	agentCfg := `{
    "agent": {
        "metrics_collection_interval": 60,
        "run_as_user": "cwagent"
    },
    "metrics": {
        "namespace": "ClickHouse/Node",
        "metrics_collected": {
            "cpu": { "measurement": ["cpu_usage_idle", "cpu_usage_iowait"], "resources": ["*"] },
            "mem": { "measurement": ["mem_used_percent", "mem_available"] },
            "disk": { "measurement": ["used_percent"], "resources": ["/", "/var/lib/clickhouse"] },
            "swap": { "measurement": ["swap_used_percent"] },
            "net": { "measurement": ["bytes_recv", "bytes_sent"], "resources": ["*"] }
        }
    },
    "logs": {
        "metrics_collected": {
            "prometheus": {
                "cluster_name": "` + cluster + `",
                "log_group_name": "/clickhouse/` + cluster + `/prometheus",
                "prometheus_config_path": "` + cfgDir + `/prometheus.yaml",
                "emf_processor": {
                    "metric_namespace": "` + chNamespace + `",
                    "metric_declaration": [
                        {
                            "source_labels": ["job"],
                            "label_matcher": "^clickhouse$",
                            "metric_selectors": ["` + metricSelectors + `"],
                            "dimensions": [["ClusterName", "NodeType"], ["ClusterName"]]
                        }
                    ]
                }
            }
        },
        "logs_collected": {
            "files": {
                "collect_list": [
                    {
                        "file_path": "/var/log/bootstrap.log",
                        "log_group_name": "/clickhouse/` + cluster + `/bootstrap",
                        "log_stream_name": "{instance_id}"
                    }` + logEntries + `
                ]
            }
        }
    }
}
`
	return cloudWatchConfig{
		PromCfg:         promCfg,
		AgentJSON:       agentCfg,
		ChNamespace:     chNamespace,
		MetricSelectors: metricSelectors,
	}
}

// configureCloudWatch configures the AWS node-metrics + log-shipping
// agent — the step the bash-era configure_cloudwatch_agent() carried
// (deleted in the chinit cutover). Nothing else configures the agent:
// packer only INSTALLS the deb, so without this step every node is
// invisible to every alarm while its logs pile up unreadable on local
// disk (observed live: a full-day outage whose 2,328 errors existed
// only on the node).
//
// Why chinit and not the launch template: instances come and go — an
// ASG instance refresh or replacement must boot with visibility
// configured, or the fix is a per-node manual patch that dies with the
// instance. The config is the verbatim agent JSON from the bash era:
// node metrics (ClickHouse/Node), the :9363 Prometheus EMF bridge
// (ClickHouse/Server or ClickHouse/Keeper by role), and the per-role
// log collect_list into /clickhouse/<cluster>/{server,error,keeper,
// bootstrap,prometheus}.
//
// AWS-only (the Provider.Name() branch the aws-cli binary requirement
// already uses); a no-op on other clouds (Azure's AMA keeps its
// VM-extension path).
func (b *Boot) configureCloudWatch(ctx context.Context) error {
	if b.Provider.Name() != "aws" {
		return nil
	}

	// The agent runs as cwagent but ClickHouse writes its logs 640
	// clickhouse:clickhouse — group membership is the standard read
	// grant (keepers may lack the group; the server package creates
	// it on server roles).
	if _, err := runCommand(ctx, "getent", "group", "clickhouse"); err != nil {
		if out, gerr := runCommand(ctx, "groupadd", "clickhouse"); gerr != nil {
			return fmt.Errorf("groupadd clickhouse: %w: %s", gerr, out)
		}
	}
	// Best-effort: the cwagent user exists on the golden AMI.
	_, _ = runCommand(ctx, "usermod", "-aG", "clickhouse", "cwagent")

	cfgDir := "/opt/aws/amazon-cloudwatch-agent/etc"
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		return err
	}

	cluster := b.Identity.ClusterName
	if cluster == "" {
		cluster = b.Spec.ClusterName
	}

	cfg := b.cloudWatchConfig(b.Identity.Role, cluster)
	if err := os.WriteFile(cfgDir+"/prometheus.yaml", []byte(cfg.PromCfg), 0644); err != nil {
		return err
	}
	agentPath := cfgDir + "/amazon-cloudwatch-agent.json"
	if err := os.WriteFile(agentPath, []byte(cfg.AgentJSON), 0644); err != nil {
		return err
	}

	if out, err := runCommand(ctx, "/opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl",
		"-a", "fetch-config", "-m", "ec2", "-c", "file:"+agentPath, "-s"); err != nil {
		return fmt.Errorf("cw-agent fetch-config: %w: %s", err, out)
	}
	if out, err := runCommand(ctx, "systemctl", "enable", "amazon-cloudwatch-agent"); err != nil {
		return fmt.Errorf("enable amazon-cloudwatch-agent: %w: %s", err, out)
	}
	return nil
}
