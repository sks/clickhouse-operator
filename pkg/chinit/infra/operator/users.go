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

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chop"
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

// ErrNoUsersSection reports that the spec declares no users section at
// all. Callers composing the users render with other renders (render
// -node) treat it as a benign skip — the generator's own users-group
// files already cover a spec without a users section.
var ErrNoUsersSection = fmt.Errorf("spec has no users section")

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
		return nil, ErrNoUsersSection
	}

	// Walk every SPEC-DECLARED user: resolve passwordFrom into hashed
	// password_sha256_hex; strip the reference field from the emitted
	// tree. The normalizer also injects operator machinery users
	// (clickhouse_operator with its operator profile) into the tree —
	// on-cluster those are real pods' credentials; off-cluster they
	// reference profiles the deployment never defined, and ClickHouse
	// refuses the whole users.d file (Code 347: Profile was not found -
	// the live finding). Only emit users the SPEC declared.
	emitted := v1.NewSettings()
	for _, user := range userSettings.Prefixes(false) {
		if user == chop.Config().ClickHouse.Access.Username {
			continue // operator machinery user - not this deployment's user
		}
		if !specDeclaredUser(userSettings, user) {
			continue
		}
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

	// Profiles: the spec's profiles section (e.g. the ingest profile with
	// synchronous-replication settings) rides the same users.d file —
	// ClickHouse refuses the whole file when a user references a profile
	// it does not define (Code 347: Profile was not found — the live
	// finding for profile ingest).
	profilesXML := ""
	if profiles := cr.GetSpecT().GetConfiguration().GetProfiles(); profiles != nil && profiles.Len() > 0 {
		profRaw := profiles.ClickHouseConfig()
		profRaw = strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(profRaw), "</clickhouse>"))
		profRaw = strings.TrimSpace(strings.TrimPrefix(profRaw, "<clickhouse>"))
		if profRaw != "" {
			// Profile trees need the <profiles> wrapper in a users.d file
			// (the emitter yields the trees at the root; ClickHouse parses
			// profiles only inside <profiles> - the live Code 347 finding).
			profilesXML = "    <profiles>\n" + profRaw + "\n    </profiles>"
		}
	}

	var xml string
	switch {
	case body != "" && profilesXML != "":
		xml = "<clickhouse>\n" + profilesXML + "\n    <users>\n" + body + "\n    </users>\n</clickhouse>\n"
	case body != "":
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

// specDeclaredUser reports whether the user was declared in the spec's
// users section (vs injected by the normalizer's defaulting). The
// normalizer-added users carry only defaulted fields (profile/quota/
// networks) — a spec-declared user has at least one field the spec set
// (passwordFrom, profile, access_management...). Heuristic-free check:
// the raw (pre-normalization) spec is unavailable here, so we treat a
// user as spec-declared when it carries a passwordFrom reference or a
// password/password_sha256_hex (the normalizer's defaults carry the
// hash("default") password_sha256_hex ONLY for passwordless spec users,
// so passwordFrom presence is the strong signal; users with any
// non-default field also count).
func specDeclaredUser(userSettings *v1.Settings, user string) bool {
	fields := v1.NewSettingsUser(userSettings, user)
	declared := false
	fields.WalkSafe(func(name string, setting *v1.Setting) {
		if setting == nil {
			return
		}
		switch name {
		case "passwordFrom", "password", "password_sha256_hex", "access_management":
			declared = true
		case "profile", "quota", "networks":
			// normalizer defaults these too - not a signal
		default:
			declared = true // any other field means the spec set it
		}
	})
	return declared
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
			// Blank the stock plaintext <password> entry, removePlainPassword
			// semantics (normalizer.go): the reference is resolved here, AFTER
			// normalization, so the normalizer cannot have set this stub - and
			// without it ClickHouse (26.8) merges the users.d hash with the
			// stock users.xml <password></password> and refuses to start:
			// Code 36 "Cannot specify multiple authentication methods".
			emitted.Set(user+"/password", v1.NewSettingScalar("").SetAttribute("remove", "1"))
			resolvedHash = true
		case name == "password_sha256_hex":
			// The normalizer defaults this to hash("default") for passwordless
			// users; a resolved passwordFrom MUST win over that default.
			if resolvedHash {
				continue
			}
			emitted.Set(user+"/"+name, v1.NewSettingScalar(value))
		default:
			// Re-wrap as a plain scalar: the emitted tree must not alias the
			// CR's settings. NewSettingScalar(value) drops attributes, so the
			// normalizer's removePlainPassword stub (password remove="1" for
			// the default user's at-normalize hash) would be lost - preserve
			// exactly that one. Other attributes (from_env) are operator-land
			// pod mechanisms and are deliberately not carried over.
			emitted.Set(user+"/"+name, scalarPreservingRemove(setting, value))
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

// scalarPreservingRemove re-wraps a setting's scalar value for the emitted
// tree while preserving exactly the remove="1" attribute - the normalizer's
// removePlainPassword stub for at-normalize hashes. A plain
// NewSettingScalar drops all attributes, which would strip the stub's
// semantics: the emitted file would carry a bare <password></password>
// that ClickHouse merges with the stock users.xml plaintext entry and
// refuses to start (Code 36). Other attributes (from_env) are pod-land
// mechanisms with no meaning off-cluster and stay dropped.
func scalarPreservingRemove(setting *v1.Setting, value string) *v1.Setting {
	emitted := v1.NewSettingScalar(value)
	if setting.HasAttribute("remove") {
		// The stub's only writer is removePlainPassword, which always sets
		// "1" - mirror it exactly (Setting exposes no attribute getter).
		emitted.SetAttribute("remove", "1")
	}
	return emitted
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
