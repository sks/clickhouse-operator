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

package operator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	osuser "os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/util"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
)

// UsersRenderer implements port.UsersRenderer: builds the users.d XML from
// the normalized CR's user settings, resolving password references via the
// SecretsResolver and hashing to password_sha256_hex.
//
// Spec convention (cloud-agnostic): a user's password is declared as
//
//	passwordFrom: <secret-name>[/<field>]
//
// (e.g. "ch-int-dev-admin-password" or "myapp/creds/admin"). The renderer
// resolves it through the injected resolver (AWS Secrets Manager, Key
// Vault, ...), SHA-256-hashes the value, and writes
// password_sha256_hex — plaintext never lands in spec, state, or XML.
//
// Why not the operator's env-var substitution path: it wires Secret values
// into POD env vars (AppendAdditionalEnvVar) — meaningless off-cluster.
// The cloud-secret reference + SecretSource port is the off-cluster
// equivalent of the operator's secretKeyRef discipline.
type UsersRenderer struct {
	// secrets resolves passwordFrom references.
	secrets port.SecretsResolver
}

// NewUsersRenderer builds the renderer over a secrets resolver.
func NewUsersRenderer(secrets port.SecretsResolver) *UsersRenderer {
	return &UsersRenderer{secrets: secrets}
}

// RenderUsers writes the users.d XML for a normalized CHI.
//
// Without it: user/password management stays hand-written bash XML
// (configure_users, 106 lines) with plaintext hashes assembled in shell.
func (r *UsersRenderer) RenderUsers(ctx context.Context, request port.RenderUsersRequest) ([]port.RenderedFile, error) {
	cr, ok := request.NormalizedCR.(*v1.ClickHouseInstallation)
	if !ok || cr == nil {
		return nil, fmt.Errorf("normalized CR is not a ClickHouseInstallation")
	}
	if request.OutputDir == "" {
		return nil, fmt.Errorf("output dir is empty")
	}

	userSettings := cr.GetSpecT().GetConfiguration().GetUsers()
	if userSettings == nil || userSettings.Len() == 0 {
		return nil, fmt.Errorf("spec has no users section")
	}

	// Walk every user's settings: resolve passwordFrom into hashed
	// password_sha256_hex; strip the reference field from the emitted tree.
	emitted := v1.NewSettings()
	users := userSettings.Prefixes(false)
	for _, user := range users {
		fields := v1.NewSettingsUser(userSettings, user)
		if err := r.adaptUser(ctx, fields, user, request.Networks[user], emitted); err != nil {
			return nil, err
		}
	}

	// The operator emitter returns <clickhouse><user>...</user>...</clickhouse>
	// (user trees at the ROOT - the operator's users ConfigMap relies on the
	// on-cluster mount layout). A users.d file needs the <users> wrapper:
	// <clickhouse><users><name>...</name></users></clickhouse>. Strip the
	// emitted root and re-wrap - building it by hand here (instead of a
	// second emitter pass) keeps the exact same leaf XML the operator
	// produces.
	raw := emitted.ClickHouseConfig()
	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(raw), "</clickhouse>"))
	body = strings.TrimSpace(strings.TrimPrefix(body, "<clickhouse>"))
	var xml string
	if body != "" {
		xml = "<clickhouse>\n    <users>\n" + body + "\n    </users>\n</clickhouse>\n"
	}

	rendered := []port.RenderedFile{{
		Group:   "users",
		Path:    "z_chctl-users.xml",
		Content: xml,
	}}

	if !request.DryRun {
		path := filepath.Join(request.OutputDir, "z_chctl-users.xml")
		if err := os.MkdirAll(request.OutputDir, 0o755); err != nil {
			return nil, fmt.Errorf("mkdir %s: %w", request.OutputDir, err)
		}
		if err := os.WriteFile(path, []byte(xml), 0o640); err != nil {
			return nil, fmt.Errorf("write %s: %w", path, err)
		}
		// clickhouse:clickhouse ownership (best-effort; ids resolved by
		// name so it works on any distro, and fails silently when run as
		// non-root — the cloud-init step re-owns after)
		if uid, gid, err := lookupClickhouseIDs(); err == nil {
			_ = os.Chown(path, uid, gid)
		}
	}
	return rendered, nil
}

// adaptUser maps one user's settings onto the emitted tree, resolving the
// password reference and appending network allowlist entries.
func (r *UsersRenderer) adaptUser(ctx context.Context, fields *v1.SettingsUser, user string, networks []string, emitted *v1.Settings) error {
	resolvedHash := false
	for _, name := range fieldNames(fields) {
		setting := fields.Get(name)
		if setting == nil {
			continue
		}
		value := setting.String()
		switch {
		case name == "passwordFrom":
			// Resolve secret ref "name[/field]" -> hash -> password_sha256_hex.
			secretName, field := splitSecretRef(value)
			if secretName == "" {
				return fmt.Errorf("user %q: empty passwordFrom", user)
			}
			plaintext, err := r.secrets.ResolveSecret(ctx, port.SecretRequest{Name: secretName, Field: field})
			if err != nil {
				return fmt.Errorf("user %q: %w", user, err)
			}
			hash := sha256.Sum256([]byte(plaintext))
			emitted.Set(user+"/password_sha256_hex", v1.NewSettingScalar(hex.EncodeToString(hash[:])))
			resolvedHash = true
		case name == "password_sha256_hex":
			// The normalizer defaults this to hash("default") for passwordless
			// users; a resolved passwordFrom MUST win over that default.
			if resolvedHash {
				continue
			}
			emitted.Set(user+"/"+name, v1.NewSettingScalar(value))
		default:
			emitted.Set(user+"/"+name, v1.NewSettingScalar(value))
		}
	}
	// Networks: request entries (cloud CIDRs) + localhost always, emitted
	// as one embedded XML block (repeated <ip> entries cannot ride the
	// map-keyed Settings.Set — the last write would win).
	nets := append([]string{}, networks...)
	nets = append(nets, "127.0.0.1", "::1")
	seen := map[string]bool{}
	body := &bytes.Buffer{}
	for _, ip := range nets {
		if seen[ip] {
			continue
		}
		seen[ip] = true
		util.Iline(body, 12, "<ip>%s</ip>", ip)
	}
	emitted.Set(user+"/networks", v1.MustNewSettingScalarFromAny(body.String()).SetEmbed())
	return nil
}

// lookupClickhouseIDs resolves the clickhouse user/group ids by name.
func lookupClickhouseIDs() (int, int, error) {
	u, err := osuser.Lookup("clickhouse")
	if err != nil {
		return 0, 0, err
	}
	g, err := osuser.LookupGroupId(u.Gid)
	if err != nil {
		return 0, 0, err
	}
	uid, _ := strconv.Atoi(u.Uid)
	gid, _ := strconv.Atoi(g.Gid)
	return uid, gid, nil
}

// splitSecretRef splits "name[/field]".
func splitSecretRef(ref string) (string, string) {
	if idx := strings.Index(ref, "/"); idx >= 0 {
		return ref[:idx], ref[idx+1:]
	}
	return ref, ""
}

// fieldNames returns the user's field names in deterministic order.
func fieldNames(fields *v1.SettingsUser) []string {
	names := make([]string, 0, fields.Len())
	fields.WalkSafe(func(name string, _ *v1.Setting) {
		names = append(names, name)
	})
	sort.Strings(names)
	return names
}
