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
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	chctlport "github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
	chctlops "github.com/altinity/clickhouse-operator/pkg/chctl/infra/operator"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// Paths the boot pipeline owns (mirrors the bash script's constants).
const (
	// BootstrapDir is where the fetched bundle lands (and where chinit
	// writes node-env for legacy consumers during migration).
	BootstrapDir = "/var/lib/clickhouse-bootstrap"

	// ConfigD / UsersD are the ClickHouse drop directories.
	ConfigDir = "/etc/clickhouse-server/config.d"
	UsersDir  = "/etc/clickhouse-server/users.d"

	// ChinitStatePath is the state file (resume + subcommand identity).
	ChinitStatePath = "/var/lib/chinit/state.json"

	// NodeEnvPath is the shell identity contract for unit scripts that
	// still source it (kept until every consumer is a chinit
	// subcommand).
	NodeEnvPath = BootstrapDir + "/node-env"

	// BootFinishedMarker is what health verification greps for.
	BootFinishedMarker = "/var/lib/cloud/instance/boot-finished"
)

// Boot is one node's boot pipeline: spec + provider + host. The 19-step
// order encodes the production findings the bash script's comments
// carried — identity settles BEFORE renders (the ip-* macro bug),
// passwords land BEFORE the render (passwordFrom refs), keepers skip
// the server start, and the health gate is non-fatal by design.
type Boot struct {
	Spec     *model.BootSpec
	Provider chinitport.Provider
	// Meta is the probed instance metadata (fetched once, reused).
	Meta chinitport.InstanceMetadata

	// Identity is filled by the identity step; later steps and the
	// node-env writer read it.
	Identity model.NodeIdentity

	// SpecHash gates resume: a new bundle hash invalidates prior step
	// results so the pipeline re-renders.
	SpecHash string

	// PersistIdentity is called by the settle_identity step right after
	// the identity is settled (claimed/derived + DNS): the app wires it
	// to the engine's state so the job subcommands and a crash-recovery
	// re-run always see the SETTLED identity, even if a later critical
	// step fails the boot. Nil (tests) = no persistence.
	PersistIdentity func(model.NodeIdentity)
}

// StaticHostname derives writer-N/keeper-N/backup-0 from the private
// IP's last octet (Terraform allocates at fixed offsets: writers at
// +10..+19, keepers at +20..+29, backup at +30).
//
// Why not the platform hostname: EC2 boots with ip-10-0-1-N; the keeper
// server_id parses garbage from the ip-* name and keeper exits 56.
func StaticHostname(privateIP string) (string, int) {
	octets := strings.Split(privateIP, ".")
	if len(octets) != 4 {
		return "", -1
	}
	last := 0
	if _, err := fmt.Sscanf(octets[3], "%d", &last); err != nil {
		return "", -1
	}
	switch {
	case last >= 10 && last <= 19:
		return fmt.Sprintf("writer-%d", last-10), last - 10
	case last >= 20 && last <= 29:
		return fmt.Sprintf("keeper-%d", last-20), last - 20
	case last == 30:
		return "backup-0", 0
	default:
		return "", -1
	}
}

// CRName computes the operator CR host name for server nodes. The CHI
// spec names its single shard "01" (the Keeper-path/S3-prefix identity
// invariant — an anonymous shardsCount layout would re-key every
// table's keeper path), so CR hosts are "01-0".."01-(N-1)" over the
// FULL server fleet in a fixed order: writers first (01-0.. by writer
// ordinal), then readers (01-(writer_count + reader ordinal)..). The
// same order the bash-era node contract and the chctl inventory flags
// use. Keepers and the backup replica are NOT in the CR (empty name
// says so to every consumer).
func (b *Boot) CRName() (string, int) {
	switch b.Spec.Role {
	case model.RoleWriter:
		ordinal := b.Identity.CROrdinal
		if ordinal < 0 {
			return "", -1
		}
		return fmt.Sprintf("01-%d", ordinal), ordinal
	case model.RoleReader:
		ordinal := b.Identity.CROrdinal
		if ordinal < 0 {
			return "", -1
		}
		// Readers sit AFTER the writers in the shard's replica list.
		return fmt.Sprintf("01-%d", b.Spec.WriterCount+ordinal), b.Spec.WriterCount + ordinal
	case model.RoleBackup:
		return "", -1
	default:
		return "", -1
	}
}

// WriteNodeEnv snapshots the settled identity into the shell contract
// file root-owned, 0600, single-quote-escaped (the bash node_env_kv
// semantics: a value can never inject live shell into a file root
// sources).
func (b *Boot) WriteNodeEnv(path string) error {
	kv := func(key, val string) string {
		escaped := strings.ReplaceAll(val, "'", `'\''`)
		return fmt.Sprintf("%s='%s'\n", key, escaped)
	}
	var sb strings.Builder
	sb.WriteString("# Generated by chinit — the settled node identity contract.\n")
	sb.WriteString(kv("NODE_ROLE", string(b.Spec.Role)))
	sb.WriteString(kv("HOSTNAME", b.Identity.Hostname))
	sb.WriteString(kv("CLUSTER_NAME", b.Spec.ClusterName))
	sb.WriteString(kv("DNS_ZONE_NAME", b.Spec.DNSZoneName))
	sb.WriteString(kv("CR_NAME", b.Identity.CRName))
	sb.WriteString(kv("INSTANCE_ID", b.Identity.InstanceID))
	sb.WriteString(kv("AWS_REGION", b.Identity.Region))
	sb.WriteString(kv("CHCTL_SPEC", filepath.Join(BootstrapDir, "chi.yaml")))
	sb.WriteString(kv("ADMIN_PASSWORD_FILE", filepath.Join(BootstrapDir, "admin-password")))
	if b.Identity.ZoneID != "" {
		sb.WriteString(kv("ROUTE53_ZONE_ID", b.Identity.ZoneID))
	}
	if b.Identity.LeaseTable != "" {
		sb.WriteString(kv("CLAIMER_TABLE_NAME", b.Identity.LeaseTable))
	}
	sb.WriteString(kv("CLOUD_PROVIDER", b.Provider.Name()))

	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	return os.WriteFile(path, []byte(sb.String()), 0600)
}

// LoadCHI loads the CHI spec the bundle delivered (the chctl library
// seam — no process boundary, no flag parsing).
func (b *Boot) LoadCHI(ctx context.Context) (any, error) {
	data, err := os.ReadFile(filepath.Join(BootstrapDir, "chi.yaml"))
	if err != nil {
		return nil, fmt.Errorf("read chi.yaml: %w", err)
	}
	loader := chctlops.NewSpecLoader()
	cr, err := loader.LoadCHI(ctx, chctlport.LoadCHIRequest{
		Spec: domain.SpecReference{
			Name: b.Spec.ClusterName,
			Data: data,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("load chi.yaml: %w", err)
	}
	return cr, nil
}
