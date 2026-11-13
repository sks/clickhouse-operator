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

// Package model holds chinit's domain types: the BootSpec (the declarative
// input the node receives instead of a templated bash script), the settled
// NodeIdentity, the persistent boot state file and the step results the
// retry engine records.
//
// Why a typed spec: the bash script interpolated ~40 Terraform variables
// into 3,170 lines of shell at template time. The spec is data — YAML
// fetched (with a SHA256 gate) from the provider's object storage — so the
// binary is identical across roles and clouds, and every field is
// validated once at load.
package model

// Role is the node's role in the cluster. The step pipeline branches on it
// (readers claim ordinals; keepers skip clickhouse-server).
type Role string

const (
	RoleWriter Role = "writer"
	RoleReader Role = "reader"
	RoleKeeper Role = "keeper"
	RoleBackup Role = "backup"
)

// IdentityMode selects how the node derives its hostname:
//   - static: Terraform allocates the private IP at a fixed offset and the
//     node maps the last octet to writer-N/keeper-N/backup-0. AWS
//     writers/keepers/backup use this.
//   - lease: an ASG member claims reader-N via the provider's ordinal
//     lease table before any render (the "ip-10-0-1-101 never joined the
//     cluster" production finding: identity MUST settle before macros).
type IdentityMode string

const (
	IdentityStatic IdentityMode = "static"
	IdentityLease  IdentityMode = "lease"
)

// BootSpec is the complete, validated input to a node boot. It is the Go
// replacement for every Terraform `${var}` interpolation in cloud-init.sh.
type BootSpec struct {
	// Cloud, when set, overrides cloud detection (used by packer builds
	// and tests; the booting node normally relies on detection).
	Cloud string `yaml:"cloud,omitempty" json:"cloud,omitempty"`

	// Node identity.
	Role             Role         `yaml:"role"                        json:"role"`
	IdentityMode     IdentityMode `yaml:"identity_mode"               json:"identity_mode"`
	ReaderHostPrefix string       `yaml:"reader_hostname_prefix,omitempty" json:"reader_hostname_prefix,omitempty"`
	ReaderCount      int          `yaml:"reader_count,omitempty"      json:"reader_count,omitempty"`

	// Cluster topology (rendered into the CHI/keeper spec macros).
	ClusterName string   `yaml:"cluster_name"             json:"cluster_name"`
	DNSZoneName string   `yaml:"dns_zone_name"            json:"dns_zone_name"`
	WriterCount int      `yaml:"writer_count"             json:"writer_count"`
	WriterNodes []string `yaml:"writer_nodes,omitempty"  json:"writer_nodes,omitempty"`
	KeeperNodes []string `yaml:"keeper_nodes,omitempty"   json:"keeper_nodes,omitempty"`

	// Config bundle: the S3/Blob prefix bootstrap.sh downloaded from.
	// chinit fetches it itself now (config_bundle_sha256 pins the exact
	// content: a spec edit rolls the hash and the fleet follows).
	ConfigBundle ConfigBundle `yaml:"config_bundle" json:"config_bundle"`

	// S3 data bucket (the tiered storage's raw disk endpoint root) and
	// the node timezone: the boot writes both into the server's
	// EnvironmentFile (storage.xml reads the endpoint via from_env —
	// an unset variable is a START error, Code 36 Empty protocol in
	// the URL, not a warning).
	S3DataBucket string `yaml:"s3_data_bucket,omitempty" json:"s3_data_bucket,omitempty"`
	Timezone     string `yaml:"timezone,omitempty"        json:"timezone,omitempty"`

	// ClaimerTableName is the provider's ordinal-lease table (readers'
	// identity: DynamoDB on AWS, Table storage on Azure). The settle
	// step claims reader-N from it.
	ClaimerTableName string `yaml:"claimer_table_name,omitempty" json:"claimer_table_name,omitempty"`

	// Disk labels (mount by LABEL, never device path — NVMe renaming).
	Disks Disks `yaml:"disks" json:"disks"`

	// Secrets to resolve via the provider's SecretSource. Names only;
	// values never enter the spec, the state file or logs.
	Secrets Secrets `yaml:"secrets" json:"secrets"`

	// Network access lists (ClickHouse <networks> sources).
	Network Network `yaml:"network" json:"network"`

	// Backup (clickhouse-backup) — rendered against the provider's
	// object storage; empty bucket = backups disabled.
	Backup Backup `yaml:"backup" json:"backup"`

	// Remote log shipping (optional; empty endpoint = disabled).
	LogShipping LogShipping `yaml:"log_shipping" json:"log_shipping"`

	// Providers append their region/zone here after metadata probing.
	Region string `yaml:"region"         json:"region"`
	Zone   string `yaml:"zone,omitempty" json:"zone,omitempty"`
}

// ConfigBundle locates and pins the node's configuration bundle in the
// provider's object storage.
type ConfigBundle struct {
	Bucket string `yaml:"bucket"     json:"bucket"`
	Prefix string `yaml:"prefix"     json:"prefix"`
	// SHA256 of the bundle contents; empty skips the gate (dev only).
	SHA256 string `yaml:"sha256,omitempty" json:"sha256,omitempty"`
}

// Disks are the volume labels the mount step resolves (label survives
// NVMe device renaming; first boot falls back to the EBS device path).
type Disks struct {
	MetadataLabel string `yaml:"metadata_label" json:"metadata_label"`
	CacheLabel    string `yaml:"cache_label,omitempty"   json:"cache_label,omitempty"`
	KeeperLabel   string `yaml:"keeper_label,omitempty"  json:"keeper_label,omitempty"`
}

// Secrets names the secret-store entries the boot must resolve.
type Secrets struct {
	DefaultPassword  string `yaml:"default_password"           json:"default_password"`
	AdminPassword    string `yaml:"admin_password"             json:"admin_password"`
	WriterPassword   string `yaml:"writer_password,omitempty"   json:"writer_password,omitempty"`
	TLSCertificate   string `yaml:"tls_certificate,omitempty"   json:"tls_certificate,omitempty"`
	TLSPrivateKey    string `yaml:"tls_private_key,omitempty"   json:"tls_private_key,omitempty"`
	TLSCACertificate string `yaml:"tls_ca_certificate,omitempty" json:"tls_ca_certificate,omitempty"`
}

// Network carries the per-role network allow lists and the subnet CIDRs
// the <networks> sections consume.
type Network struct {
	SubnetCIDR                 string   `yaml:"subnet_cidr"                    json:"subnet_cidr"`
	NLBSubnetCIDR              string   `yaml:"nlb_subnet_cidr,omitempty"       json:"nlb_subnet_cidr,omitempty"`
	AdminAllowedNetworks       []string `yaml:"admin_allowed_networks,omitempty"       json:"admin_allowed_networks,omitempty"`
	WriterAllowedNetworks      []string `yaml:"writer_allowed_networks,omitempty"      json:"writer_allowed_networks,omitempty"`
	DefaultUserAllowedNetworks []string `yaml:"default_user_allowed_networks,omitempty" json:"default_user_allowed_networks,omitempty"`
}

// Backup configures clickhouse-backup against provider object storage.
type Backup struct {
	Bucket                string `yaml:"bucket"                 json:"bucket"`
	Prefix                string `yaml:"prefix,omitempty"        json:"prefix,omitempty"`
	RetentionDays         int    `yaml:"retention_days,omitempty" json:"retention_days,omitempty"`
	CompressionFormat     string `yaml:"compression_format,omitempty" json:"compression_format,omitempty"`
	IsBackupReplica       bool   `yaml:"is_backup_replica"       json:"is_backup_replica"`
	BackupReplicaHostname string `yaml:"backup_replica_hostname,omitempty" json:"backup_replica_hostname,omitempty"`
}

// LogShipping is the optional remote log pipeline (OTEL or Fluent Bit).
type LogShipping struct {
	Agent         string `yaml:"agent,omitempty"       json:"agent,omitempty"`
	Endpoint      string `yaml:"endpoint,omitempty"     json:"endpoint,omitempty"`
	Headers       string `yaml:"headers,omitempty"      json:"headers,omitempty"`
	FluentbitOutput string `yaml:"fluentbit_output,omitempty" json:"fluentbit_output,omitempty"`
}

// Validate checks the invariants every role shares. Role-specific shapes
// (readers need reader_count, keepers need keeper_nodes) are validated
// where consumed; this catches the malformed-spec class early, before any
// provider client is built.
func (s *BootSpec) Validate() error {
	switch s.Role {
	case RoleWriter, RoleReader, RoleKeeper, RoleBackup:
	default:
		return &SpecError{Field: "role", Reason: "unknown role " + string(s.Role)}
	}
	switch s.IdentityMode {
	case IdentityStatic, IdentityLease:
	case "":
		return &SpecError{Field: "identity_mode", Reason: "required"}
	default:
		return &SpecError{Field: "identity_mode", Reason: "unknown mode " + string(s.IdentityMode)}
	}
	if s.Role == RoleReader && s.IdentityMode == IdentityLease && s.ReaderCount <= 0 {
		return &SpecError{Field: "reader_count", Reason: "lease identity requires reader_count > 0"}
	}
	if s.ConfigBundle.Bucket == "" {
		return &SpecError{Field: "config_bundle.bucket", Reason: "required"}
	}
	if s.Disks.MetadataLabel == "" {
		return &SpecError{Field: "disks.metadata_label", Reason: "required (metadata volume must be mounted)"}
	}
	if s.ClusterName == "" {
		return &SpecError{Field: "cluster_name", Reason: "required"}
	}
	if s.Secrets.DefaultPassword == "" {
		return &SpecError{Field: "secrets.default_password", Reason: "required"}
	}
	return nil
}

// SpecError names the spec field that failed validation so the operator
// reading boot logs can fix the tfvars, not the code.
type SpecError struct {
	Field  string
	Reason string
}

func (e *SpecError) Error() string { return "chinit.yaml: " + e.Field + ": " + e.Reason }

// NodeIdentity is the SETTLED identity every later step and unit uses.
// It is snapshotted after the hostname is claimed/derived — never before
// (the pre-claim ip-* capture is the bug class the node-env contract
// killed; the typed struct makes the same guarantee in Go).
type NodeIdentity struct {
	Role        Role   `json:"role"`
	Hostname    string `json:"hostname"`
	InstanceID  string `json:"instance_id"`
	PrivateIP   string `json:"private_ip"`
	Region      string `json:"region"`
	ClusterName string `json:"cluster_name"`
	DNSZone     string `json:"dns_zone"`
	// CRName is the operator CR host name for server nodes ("01-0"),
	// empty for the backup replica (not in the CR) and keepers.
	CRName string `json:"cr_name,omitempty"`
	// CROrdinal is the numeric suffix of CRName (writers/readers), or
	// the keeper's own ordinal; -1 when not applicable.
	CROrdinal int `json:"cr_ordinal"`
	// ZoneID is the private DNS zone (provider cache of the claim-time
	// resolution); the spot drain releases the record through it.
	ZoneID string `json:"zone_id,omitempty"`
	// LeaseTable is the provider's ordinal lease table when the node
	// holds a lease (readers); the drain releases it.
	LeaseTable string `json:"lease_table,omitempty"`
}

// StepStatus is the recorded outcome of one boot step.
type StepStatus string

const (
	StepPending StepStatus = "pending"
	StepDone    StepStatus = "done"
	StepFailed  StepStatus = "failed"
	StepSkipped StepStatus = "skipped"
)

// StepResult records one step execution for resume and triage. Duration
// feeds the [TIMING] summary the bash script printed (kept verbatim —
// dashboards grep it).
type StepResult struct {
	Name      string     `json:"name"`
	Status    StepStatus `json:"status"`
	Duration  int64      `json:"duration_ms"`
	Error     string     `json:"error,omitempty"`
	StartedAt string     `json:"started_at,omitempty"`
}

// StateFile is the persistent, non-secret boot record. Completed steps
// short-circuit on re-run (convergence); identity is the settled snapshot
// subcommands (schema-sync, spot-watch, …) reload instead of re-deriving.
type StateFile struct {
	Version  int           `json:"version"`
	Identity NodeIdentity `json:"identity"`
	Steps    []StepResult `json:"steps"`
	BootTime string        `json:"boot_time,omitempty"`
}

// FindStep returns the recorded result for a step, or nil when it has
// never run to completion (Pending steps re-run).
func (st *StateFile) FindStep(name string) *StepResult {
	for i := range st.Steps {
		if st.Steps[i].Name == name {
			return &st.Steps[i]
		}
	}
	return nil
}

// RecordStep stores or replaces a step result.
func (st *StateFile) RecordStep(r StepResult) {
	for i := range st.Steps {
		if st.Steps[i].Name == r.Name {
			st.Steps[i] = r
			return
		}
	}
	st.Steps = append(st.Steps, r)
}
