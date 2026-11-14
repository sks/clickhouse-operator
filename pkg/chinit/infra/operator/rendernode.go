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
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"os"
	osuser "os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// renderNodeManifestName is the manifest file render-node keeps in every
// directory it manages: one installed file name per line.
//
// Why a manifest (not a name prefix): the stack's config.d mixes chctl
// output (chop-generated-*.xml AND spec files: entries like
// server-logger.xml — no shared prefix) with hand-maintained overlays
// (network.xml, backup-replica-settings.xml). Only files a render-node
// run itself listed are ever prune candidates; everything else is
// untouchable by construction.
const renderNodeManifestName = ".chctl-render-node.manifest"

// renderNodeStagingDirname is the staging directory each render-node
// install builds inside the destination directory before anything
// installs. Files are written here and renamed into place (same
// directory: atomic on every filesystem a node boots from), so a
// mid-install crash can never leave a truncated file at a destination —
// at worst this one directory lingers, and the next run clears it
// before staging again.
const renderNodeStagingDirname = ".render-node-staging"

// NodeRenderer implements port.NodeRenderer over the operator-backed
// ConfigRenderer + UsersRenderer, owning the full install discipline:
// render → validate → stage → install → own/permission → prune.
type NodeRenderer struct {
	// config renders the config.d groups and the keeper single file.
	config *ConfigRenderer
	// users renders the users.d files (the `chctl users render` path).
	users *UsersRenderer
}

// NewNodeRenderer builds the node renderer over the shared renderers.
func NewNodeRenderer(config *ConfigRenderer, users *UsersRenderer) *NodeRenderer {
	return &NodeRenderer{config: config, users: users}
}

// RenderNode renders one node's complete configuration set and installs
// it with correct ownership and permissions — composing what previously
// took a host-scoped render, an inventory render and a users render.
//
// Discipline, in order:
//
//   - Render every section into memory (nothing on disk yet).
//   - Validate every XML document parses; a malformed section fails the
//     whole run with the section named and NOTHING installed (a bad
//     files: entry used to leave a config.d the server parsed into a
//     boot loop).
//   - Install through a staging directory with atomic renames, chmod
//     the exact mode AFTER write (umask can never soften it) and chown
//     the service account — the chmod/chown the calling stack
//     previously duplicated per site, one site forgetting them being
//     the live root-owned-0640 crash class.
//   - Prune files this command installed on a previous run but no
//     longer renders (tracked in the manifest), so a renamed or removed
//     spec section stops lingering on re-rendered nodes.
//
// Check mode computes the drift against the installed tree and writes
// nothing; dry-run reports the plan and writes nothing.
func (r *NodeRenderer) RenderNode(ctx context.Context, request port.RenderNodeRequest) (port.RenderNodeResult, error) {
	if request.OutputDir == "" {
		return port.RenderNodeResult{}, fmt.Errorf("output dir is empty")
	}
	// The managed directories are known up front (not derived from the
	// plan): a render that stops producing users.d files must still
	// prune the old ones from the users.d manifest, and the keeper's
	// single file lives in its own directory (typically NOT -out).
	managedDirs := []string{request.OutputDir}
	if request.Keeper {
		if request.SingleFilePath == "" {
			return port.RenderNodeResult{}, fmt.Errorf("keeper mode requires -single-file: clickhouse-keeper runs with an explicit --config, so the node needs ONE complete file")
		}
		singleDir := filepath.Dir(request.SingleFilePath)
		if singleDir != "." && singleDir != request.OutputDir {
			managedDirs = append(managedDirs, singleDir)
		}
	} else {
		if request.UsersDir == "" {
			return port.RenderNodeResult{}, fmt.Errorf("users dir is empty")
		}
		if request.UsersDir != request.OutputDir {
			managedDirs = append(managedDirs, request.UsersDir)
		}
	}

	var plan []port.RenderNodeFile
	var err error
	if request.Keeper {
		plan, err = r.renderKeeperPlan(ctx, request)
	} else {
		plan, err = r.renderCHIPlan(ctx, request)
	}
	if err != nil {
		return port.RenderNodeResult{}, err
	}
	if len(plan) == 0 {
		return port.RenderNodeResult{}, fmt.Errorf("render produced no files")
	}

	// Validation gate: every .xml file must parse before anything
	// installs. On the first failure the installed tree is untouched.
	for _, file := range plan {
		if err := validateXMLSection(file.Path, file.Content); err != nil {
			return port.RenderNodeResult{}, err
		}
	}

	// Stale prune list: files a previous render-node run installed (per
	// the manifests) that this render no longer produces.
	renderedNames := make(map[string]bool, len(plan))
	for _, file := range plan {
		renderedNames[filepath.Base(file.Path)] = true
	}
	var pruned []string
	for _, dir := range managedDirs {
		stale, err := staleManifestFiles(dir, renderedNames)
		if err != nil {
			return port.RenderNodeResult{}, err
		}
		pruned = append(pruned, stale...)
	}
	sort.Strings(pruned)

	result := port.RenderNodeResult{
		Files:    plan,
		Pruned:   pruned,
		DryRun:   request.DryRun,
		Check:    request.Check,
		NodeType: request.NodeType,
	}

	// Check mode installs NOTHING: the drift (missing, changed, stale)
	// is reported so a refresher can decide to render before touching
	// the node.
	if request.Check {
		result.Drift = diffInstalled(plan, pruned)
		return result, nil
	}
	if request.DryRun {
		return result, nil
	}

	if err := installPlan(plan); err != nil {
		return port.RenderNodeResult{}, err
	}
	for _, path := range pruned {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return port.RenderNodeResult{}, fmt.Errorf("prune %s: %w", path, err)
		}
	}
	if err := chownPlan(plan, request.OwnerUser); err != nil {
		return port.RenderNodeResult{}, err
	}
	if err := writeManifests(plan, managedDirs); err != nil {
		return port.RenderNodeResult{}, err
	}
	return result, nil
}

// renderCHIPlan composes the three renders the stack previously ran
// separately: the host-selected sections (macros/zookeeper/settings/
// profiles/files), the inventory sections (remote_servers with
// credentials, common groups) and the users.d render — one plan with
// exact destinations and modes.
func (r *NodeRenderer) renderCHIPlan(ctx context.Context, request port.RenderNodeRequest) ([]port.RenderNodeFile, error) {
	rendered, err := r.config.RenderCHI(ctx, port.RenderCHIRequest{
		NormalizedCR:   request.NormalizedCR,
		HostName:       request.HostName,
		Hosts:          request.Hosts,
		Replicas:       request.Replicas,
		UserPasswords:  request.UserPasswords,
		RemoteUser:     request.RemoteUser,
		RemotePassword: request.RemotePassword,
		OutputDir:      request.OutputDir,
		DryRun:         true, // the plan, not the renderer, owns writing
	})
	if err != nil {
		return nil, err
	}
	plan := make([]port.RenderNodeFile, 0, len(rendered)+2)
	for _, file := range rendered {
		// The operator's machinery user (clickhouse_operator) never
		// reaches these files: RenderCHI drops it from the users tree
		// before emission (off-cluster its profile is undefined and
		// ClickHouse refuses the WHOLE users.xml, Code 347 — the live
		// chinit-cutover finding), so no post-render strip exists here.
		// Credential-bearing files stay 0640 (group-readable at most,
		// never world-readable): remote_servers with an injected
		// password, users files with password hashes. Everything else
		// is ordinary configuration — world-readable 0644, the mode
		// the server user and any diagnostic tooling read it with.
		mode := os.FileMode(0o644)
		if isCredentialFile(file) {
			mode = 0o640
		}
		dir := request.OutputDir
		if file.Group == "users" {
			dir = request.UsersDir
		}
		plan = append(plan, port.RenderNodeFile{
			Group:   file.Group,
			Path:    filepath.Join(dir, filepath.Base(file.Path)),
			Mode:    mode,
			Content: file.Content,
		})
	}

	usersPlan, err := r.renderUsersPlan(ctx, request)
	if err != nil {
		return nil, err
	}
	return append(plan, usersPlan...), nil
}

// isCredentialFile reports whether the rendered file carries
// credentials and must stay 0640: any users-group file (password
// hashes ride them) and a remote_servers section with an injected
// password.
func isCredentialFile(file port.RenderedFile) bool {
	if file.Group == "users" {
		return true
	}
	return filepath.Base(file.Path) == "chop-generated-remote_servers.xml" &&
		strings.Contains(file.Content, "<password>")
}

// renderUsersPlan runs the UsersRenderer (the `chctl users render`
// path) into a dry render so its output joins the plan with the same
// 0640 discipline as the generator's own users files.
func (r *NodeRenderer) renderUsersPlan(ctx context.Context, request port.RenderNodeRequest) ([]port.RenderNodeFile, error) {
	if r.users == nil {
		return nil, fmt.Errorf("users renderer is not wired")
	}
	rendered, err := r.users.RenderUsers(ctx, port.RenderUsersRequest{
		NormalizedCR: request.NormalizedCR,
		OutputDir:    request.UsersDir,
		Networks:     request.Networks,
		DryRun:       true, // the plan owns writing
	})
	if err != nil {
		// A spec with no users section at all is a legitimate minimal
		// CHI: the generator's own users-group files (already in the
		// plan) cover it. Any other failure — a missing secret, a
		// malformed reference — is a hard error, exactly as in
		// `chctl users render`.
		if errors.Is(err, ErrNoUsersSection) {
			return nil, nil
		}
		return nil, err
	}
	plan := make([]port.RenderNodeFile, 0, len(rendered))
	for _, file := range rendered {
		plan = append(plan, port.RenderNodeFile{
			Group:   file.Group,
			Path:    filepath.Join(request.UsersDir, filepath.Base(file.Path)),
			Mode:    0o640, // password hashes: group-readable at most
			Content: file.Content,
		})
	}
	return plan, nil
}

// renderKeeperPlan renders the keeper flow: ONE complete merged file
// (common+host sections — raft, listeners, prometheus/logger files
// riding it) with the server-id override. Keepers carry no ClickHouse
// users, so no users render runs.
func (r *NodeRenderer) renderKeeperPlan(ctx context.Context, request port.RenderNodeRequest) ([]port.RenderNodeFile, error) {
	if request.SingleFilePath == "" {
		return nil, fmt.Errorf("keeper mode requires -single-file: clickhouse-keeper runs with an explicit --config, so the node needs ONE complete file")
	}
	if request.HostName == "" {
		return nil, fmt.Errorf("keeper mode requires a host: pass -host (name or name=address)")
	}
	rendered, err := r.config.RenderCHK(ctx, port.RenderCHKRequest{
		NormalizedCR:    request.NormalizedCR,
		HostName:        request.HostName,
		Hosts:           request.Hosts,
		OutputDir:       request.OutputDir,
		DryRun:          true, // the plan owns writing
		KeeperAddresses: request.KeeperAddresses,
		ServerID:        request.ServerID,
		RaftIDs:         request.RaftIDs,
		SingleFilePath:  request.SingleFilePath,
	})
	if err != nil {
		return nil, err
	}
	plan := make([]port.RenderNodeFile, 0, 1)
	for _, file := range rendered {
		if file.Group != "single" {
			// Per-group content is returned for inspection only; the
			// merged single file IS the keeper node's artifact.
			continue
		}
		plan = append(plan, port.RenderNodeFile{
			Group:   file.Group,
			Path:    file.Path,
			Mode:    0o640, // may carry raft TLS material: never world-readable
			Content: file.Content,
		})
	}
	if len(plan) == 0 {
		return nil, fmt.Errorf("keeper single-file render produced no output")
	}
	return plan, nil
}

// validateXMLSection asserts a rendered file parses as XML before
// anything installs. The error names the destination path — the one
// string an operator can act on.
//
// Non-.xml files (a spec may name arbitrary configuration files) are
// exempt: their format is the spec author's choice; only the XML the
// generator emits is validated.
func validateXMLSection(path, content string) error {
	if !strings.HasSuffix(strings.ToLower(path), ".xml") {
		return nil
	}
	decoder := xml.NewDecoder(strings.NewReader(content))
	for {
		_, err := decoder.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("validate %s: malformed XML: %w", path, err)
		}
	}
}

// staleManifestFiles lists the manifest-tracked installed files the
// current render no longer produces. Hand-maintained files never
// appear in the manifest and are therefore never prune candidates.
func staleManifestFiles(dir string, renderedNames map[string]bool) ([]string, error) {
	listed, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	stale := make([]string, 0, len(listed))
	for name := range listed {
		if renderedNames[name] {
			continue
		}
		path := filepath.Join(dir, name)
		if _, err := os.Stat(path); err != nil {
			continue // already gone — nothing to prune, not drift either
		}
		stale = append(stale, path)
	}
	return stale, nil
}

// readManifest loads the installed-file names from one managed
// directory's manifest. A missing manifest (first run, or a directory
// the previous flow never touched) yields an empty set — never an
// error, so a fresh node renders exactly like a re-rendered one.
func readManifest(dir string) (map[string]bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, renderNodeManifestName))
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]bool{}, nil
		}
		return nil, fmt.Errorf("read manifest %s: %w", dir, err)
	}
	listed := make(map[string]bool)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			listed[line] = true
		}
	}
	return listed, nil
}

// writeManifests records the installed file names per managed
// directory, overwriting the previous run's list. The manifest is the
// ONLY prune authority: a failed write must fail the run rather than
// silently leaving a stale list a future prune would trust.
//
// Only directories with installed files get a manifest: keeper mode
// writes its single file elsewhere and must leave -out completely
// untouched.
func writeManifests(plan []port.RenderNodeFile, managedDirs []string) error {
	namesByDir := make(map[string][]string, len(managedDirs))
	for _, file := range plan {
		dir := filepath.Dir(file.Path)
		namesByDir[dir] = append(namesByDir[dir], filepath.Base(file.Path))
	}
	for dir, names := range namesByDir {
		sort.Strings(names)
		content := ""
		if len(names) > 0 {
			content = strings.Join(names, "\n") + "\n"
		}
		if err := os.WriteFile(filepath.Join(dir, renderNodeManifestName), []byte(content), 0o644); err != nil {
			return fmt.Errorf("write manifest %s: %w", dir, err)
		}
	}
	return nil
}

// diffInstalled compares the rendered plan against the installed files
// and returns the drift lines — "path: missing" (nothing installed),
// "path: changed" (bytes differ) and "path: stale" (a manifest-tracked
// file the render no longer produces). An in-sync install yields an
// empty slice.
func diffInstalled(plan []port.RenderNodeFile, pruned []string) []string {
	drift := make([]string, 0, len(plan)+len(pruned))
	for _, file := range plan {
		installed, err := os.ReadFile(file.Path)
		switch {
		case os.IsNotExist(err):
			drift = append(drift, file.Path+": missing")
		case err != nil:
			drift = append(drift, file.Path+": unreadable")
		case string(installed) != file.Content:
			drift = append(drift, file.Path+": changed")
		}
	}
	for _, path := range pruned {
		drift = append(drift, path+": stale")
	}
	sort.Strings(drift)
	return drift
}

// installPlan writes each file into the destination's staging
// directory, renames it into place (same directory: atomic), then
// applies the exact permission AFTER write — umask can never soften
// the discipline. The staging directory is cleared first (a crash
// mid-install may have left one) and removed after.
func installPlan(plan []port.RenderNodeFile) error {
	byDir := make(map[string][]port.RenderNodeFile)
	for _, file := range plan {
		dir := filepath.Dir(file.Path)
		byDir[dir] = append(byDir[dir], file)
	}
	for dir, files := range byDir {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
		staging := filepath.Join(dir, renderNodeStagingDirname)
		// Clear any crash leftover first so staged names never alias a
		// half-installed predecessor.
		if err := os.RemoveAll(staging); err != nil {
			return fmt.Errorf("clear staging %s: %w", staging, err)
		}
		if err := os.MkdirAll(staging, 0o700); err != nil {
			return fmt.Errorf("mkdir staging %s: %w", staging, err)
		}
		for _, file := range files {
			stagedPath := filepath.Join(staging, filepath.Base(file.Path))
			if err := os.WriteFile(stagedPath, []byte(file.Content), file.Mode); err != nil {
				return fmt.Errorf("stage %s: %w", stagedPath, err)
			}
			if err := os.Rename(stagedPath, file.Path); err != nil {
				return fmt.Errorf("install %s: %w", file.Path, err)
			}
			// The write above is subject to umask; the chmod here pins
			// the exact mode the plan promises.
			if err := os.Chmod(file.Path, file.Mode); err != nil {
				return fmt.Errorf("chmod %s: %w", file.Path, err)
			}
		}
		if err := os.Remove(staging); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove staging %s: %w", staging, err)
		}
	}
	return nil
}

// chownPlan chowns every installed file to the owner account. Name
// lookup falls back to numeric ("uid[:gid]", the -user flag's escape
// hatch for distros with odd NSS wiring). A default-owner lookup that
// fails is best-effort (a developer render on a laptop without the
// clickhouse account must still succeed — cloud-init runs as root and
// there the chown is the entire point); an explicit -user that cannot
// be resolved is a hard error, because the operator asked for it.
func chownPlan(plan []port.RenderNodeFile, ownerUser string) error {
	explicit := ownerUser != ""
	if ownerUser == "" {
		ownerUser = defaultRenderNodeOwner
	}
	uid, gid, err := lookupOwnerIDs(ownerUser)
	if err != nil {
		if explicit {
			return err
		}
		return nil
	}
	for _, file := range plan {
		if err := os.Chown(file.Path, uid, gid); err != nil {
			// EPERM as non-root is expected (no CAP_CHOWN): the
			// permission and content discipline still holds, and
			// root-run installs — the production path — chown fine.
			if errors.Is(err, os.ErrPermission) {
				continue
			}
			return fmt.Errorf("chown %s to %s: %w", file.Path, ownerUser, err)
		}
	}
	return nil
}

// defaultRenderNodeOwner is the account rendered configuration belongs
// to on every supported distro.
const defaultRenderNodeOwner = "clickhouse"

// lookupOwnerIDs resolves the owner account to uid/gid: bare numeric
// uid, numeric uid:gid, name, or name:group.
func lookupOwnerIDs(ownerUser string) (int, int, error) {
	name, gidPart, hasGid := strings.Cut(ownerUser, ":")
	uid, err := lookupUserID(name)
	if err != nil {
		return 0, 0, err
	}
	if !hasGid {
		// Name form: the account's own primary group (also for a
		// numeric uid that resolves; a uid that does not falls back to
		// gid = uid — the single-account container pattern).
		if u, err := osuser.Lookup(name); err == nil {
			gid, _ := strconv.Atoi(u.Gid)
			return uid, gid, nil
		}
		if u, err := osuser.LookupId(name); err == nil {
			gid, _ := strconv.Atoi(u.Gid)
			return uid, gid, nil
		}
		return uid, uid, nil
	}
	gid, err := resolveGroupID(gidPart)
	if err != nil {
		return 0, 0, err
	}
	return uid, gid, nil
}

// lookupUserID resolves a user by name or by numeric id.
func lookupUserID(name string) (int, error) {
	if uid, err := strconv.Atoi(name); err == nil {
		return uid, nil
	}
	u, err := osuser.Lookup(name)
	if err != nil {
		return 0, fmt.Errorf("user %q not found", name)
	}
	uid, _ := strconv.Atoi(u.Uid)
	return uid, nil
}

// resolveGroupID resolves a group by name or by numeric id.
func resolveGroupID(name string) (int, error) {
	if gid, err := strconv.Atoi(name); err == nil {
		return gid, nil
	}
	g, err := osuser.LookupGroup(name)
	if err != nil {
		return 0, fmt.Errorf("group %q not found", name)
	}
	gid, _ := strconv.Atoi(g.Gid)
	return gid, nil
}
