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
	"encoding/xml"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chiConfig "github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
	"github.com/altinity/clickhouse-operator/pkg/util"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// ConfigRenderer implements port.ConfigRenderer over the operator's config
// file generators — the same code that produces ConfigMaps on Kubernetes.
type ConfigRenderer struct{}

// NewConfigRenderer builds the operator-backed renderer.
func NewConfigRenderer() *ConfigRenderer {
	return &ConfigRenderer{}
}

// RenderCHI writes the common, users and (when a host is selected) host
// config files for a normalized CHI.
//
// Why the generator (not hand-written templates): the operator's normalizer
// defaults, section naming, and user hardening apply identically
// off-cluster, and any generator improvement is inherited automatically.
//
// Without it: every deployment hand-maintains its own XML templates, which
// drift from ClickHouse LTS behavior and from each other.
func (r *ConfigRenderer) RenderCHI(ctx context.Context, request port.RenderCHIRequest) ([]port.RenderedFile, error) {
	bootstrapChop()

	cr, ok := request.NormalizedCR.(*v1.ClickHouseInstallation)
	if !ok || cr == nil {
		return nil, fmt.Errorf("normalized CR is not a ClickHouseInstallation")
	}
	if request.OutputDir == "" {
		return nil, fmt.Errorf("output dir is empty")
	}

	// CHI generator built manually (not via managers.NewConfigFilesGenerator)
	// to inject the cloud-wrapped namer when a cloud inventory is given:
	// remote_servers <host> entries and interserver_http_host resolve via
	// NameInstanceHostname and macros <replica> via NamePodHostname — the
	// operator's defaults render both as Kubernetes pod patterns, which
	// are unresolvable (the former) and identity-breaking (the latter)
	// off-cluster. Without an inventory the plain operator namer is used,
	// keeping output identical to the pre-inventory render.
	var namer interfaces.INameManager = managers.NewNameManager(managers.NameManagerTypeClickHouse)
	if addresses := hostInventory(request.Hosts); len(addresses) > 0 || len(request.Replicas) > 0 {
		namer = newCloudNamer(addresses).withReplicas(request.Replicas)
	}
	genOpts := chiGeneratorOptions(cr)
	// The normalizer injects the operator's machinery user
	// (chop.Config().ClickHouse.Access.Username) into the users tree.
	// On-cluster that is a real credential with a profile definition
	// the operator itself ships (config/chi/users.d/01-clickhouse-
	// operator-profile.xml, mounted by the users ConfigMap);
	// bootstrapChop() pins those runtime scan paths to nonexistent, so
	// off-cluster the profile never materializes and ClickHouse
	// refuses the whole users.d load (Code 347: Profile was not
	// found). The spec's users — the deployment's own accounts — are
	// the contract; the machinery user is operator-only and must not
	// render. Drop it from the settings tree BEFORE emission — the
	// same pre-generation discipline as injectUserPasswords — so the
	// operator's own emitter produces the file and no XML string
	// surgery (regex/re-parse, with their escaping and tag-mismatch
	// failure classes) exists anywhere in the chain.
	genOpts.Users = withoutMachineryUser(genOpts.Users)
	if len(request.UserPasswords) > 0 {
		users, err := injectUserPasswords(genOpts.Users, request.UserPasswords)
		if err != nil {
			return nil, err
		}
		genOpts.Users = users
	}
	gen := chiConfig.NewGenerator(cr, namer, genOpts)
	genDomain := chiConfig.NewFilesGeneratorDomain(gen)
	filesGenerator := chiConfig.NewFilesGenerator(gen, chop.Config().ClickHouse.Config.File.Runtime, genDomain)

	rendered := make([]port.RenderedFile, 0, 16)

	commonFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupCommon, chiConfig.NewFilesGeneratorOptions())
	rendered = append(rendered, toRenderedFiles("common", commonFiles)...)

	// Per-replica distributed-query credentials are a chctl-only
	// concern: the operator's generator has no <user>/<password> support
	// (its cluster-level <secret> element is rejected by ClickHouse
	// 26.8), so the pair is spliced into the already-rendered
	// remote_servers.xml instead of threading a new option through the
	// shared generator.
	if request.RemoteUser != "" || request.RemotePassword != "" {
		if request.RemoteUser == "" {
			return nil, fmt.Errorf("RemotePassword set without RemoteUser")
		}
		if err := injectReplicaCredentials(rendered, request.RemoteUser, request.RemotePassword); err != nil {
			return nil, err
		}
	}

	usersFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupUsers)
	rendered = append(rendered, toRenderedFiles("users", usersFiles)...)

	if request.HostName != "" {
		host, err := findHost(cr, request.HostName)
		if err != nil {
			return nil, err
		}
		hostOptions := chiConfig.NewFilesGeneratorOptions().SetHost(host)
		hostFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupHost, hostOptions)
		rendered = append(rendered, toRenderedFiles("host", hostFiles)...)
	}

	if !request.DryRun {
		if err := writeFiles(rendered, request.OutputDir, request.UsersDir); err != nil {
			return nil, err
		}
	}

	return rendered, nil
}

// RenderCHK writes the common and host config files for a normalized CHK.
//
// Why: keeper nodes need identical config discipline (raft, TLS,
// prometheus endpoint); reusing the keeper generator keeps keeper configs
// on the same single-source footing.
//
// Single-file mode: when request.SingleFilePath is set, no per-group
// files are written to OutputDir — instead the common and host sections
// merge into ONE complete <clickhouse>-rooted document at the given
// path. Cloud stacks run clickhouse-keeper with an explicit
// --config=keeper_config.xml, which disables the config.d auto-include;
// those nodes need the whole configuration in a single file.
func (r *ConfigRenderer) RenderCHK(ctx context.Context, request port.RenderCHKRequest) ([]port.RenderedFile, error) {
	bootstrapChop()

	cr, ok := request.NormalizedCR.(*chk.ClickHouseKeeperInstallation)
	if !ok || cr == nil {
		return nil, fmt.Errorf("normalized CR is not a ClickHouseKeeperInstallation")
	}
	if request.OutputDir == "" {
		return nil, fmt.Errorf("output dir is empty")
	}

	// Keeper generator built manually (not via managers.NewConfigFilesGenerator)
	// to inject the cloud-wrapped namer: raft server hostnames come from
	// NameInstanceHostname, which the default namer renders in the k8s
	// service pattern — unresolvable off-cluster. The wrapped namer
	// translates them to the cloud DNS names from KeeperAddresses.
	keeperNamer := newKeeperCloudNamer(mergeHostAddresses(hostInventory(request.Hosts), request.KeeperAddresses))
	if warning := keeperListenHostWarning(cr); warning != "" {
		// Lint, not a gate: the render stays valid output; the warning
		// tells the operator the config will not listen as intended
		// before the fleet finds out empirically.
		fmt.Fprintln(os.Stderr, warning)
	}
	gen := chkConfig.NewGenerator(cr, keeperNamer, chkGeneratorOptions(cr))
	genDomain := chkConfig.NewFilesGeneratorDomain(gen)
	filesGenerator := chkConfig.NewFilesGenerator(gen, chop.Config().Keeper.Config.File.Runtime, genDomain)

	rendered := make([]port.RenderedFile, 0, 8)

	commonFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupCommon, chkConfig.NewFilesGeneratorOptions())
	rendered = append(rendered, toRenderedFiles("common", commonFiles)...)

	var host *v1.Host
	if request.HostName != "" {
		var err error
		host, err = findKeeperHost(cr, request.HostName)
		if err != nil {
			return nil, err
		}
		hostOptions := chkConfig.NewFilesGeneratorOptions().SetHost(host)
		hostFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupHost, hostOptions)
		rendered = append(rendered, toRenderedFiles("host", hostFiles)...)
	}

	// Identity overrides (cloud state, authoritative across re-images):
	// the raft-id map rewrites the raft_configuration <server><id> values
	// — an existing off-cluster ensemble persists ONE-based ids in its
	// raft logs, and zero-based CR indices would present a different
	// ensemble to surviving members, breaking Raft recovery. Applies to
	// both output paths (per-group -out and single-file).
	//
	// The consistency guard runs before any rewrite: -server-id and
	// -raft-id must tell the same story about the selected host.
	if err := checkServerIDRaftIDConsistency(request.HostName, request.ServerID, request.RaftIDs); err != nil {
		return nil, err
	}
	if len(request.RaftIDs) > 0 {
		if err := applyRaftIDOverrides(rendered, request.RaftIDs, keeperHostNames(cr)); err != nil {
			return nil, err
		}
	}

	// Single-file mode: merge every rendered section into one complete
	// document and write it to SingleFilePath. OutputDir is deliberately
	// skipped.
	if request.SingleFilePath != "" {
		if host == nil {
			return nil, fmt.Errorf("-single-file requires a keeper host: pass -host (name or name=address)")
		}
		if request.ServerID > 0 {
			applyServerIDOverride(rendered, request.ServerID)
		}
		merged, err := mergeRenderedSections(rendered)
		if err != nil {
			return nil, err
		}
		if !request.DryRun {
			if err := writeSingleFile(request.SingleFilePath, merged); err != nil {
				return nil, err
			}
		}
		// The returned list keeps the per-group files (dry-run parity for
		// callers that inspect them) plus the merged artifact, so
		// consumers can see both.
		return append(rendered, port.RenderedFile{
			Group:   "single",
			Path:    request.SingleFilePath,
			Content: merged,
		}), nil
	}

	if !request.DryRun {
		if err := writeFiles(rendered, request.OutputDir, ""); err != nil {
			return nil, err
		}
	}

	return rendered, nil
}

// keeperListenHostSetting is the settings key the live AWS stack put
// under keeper_server — the exact key keeper IGNORES.
const keeperListenHostSetting = "keeper_server/listen_host"

// keeperListenHostWarning reports the keeper_server/listen_host gotcha:
// clickhouse-keeper ignores the setting, so a spec carrying it renders
// a config whose client port stays on 127.0.0.1 — no fleet node can
// reach mntr or replication until listen_host moves to a ROOT-level
// files entry (<clickhouse><listen_host>).
//
// Why a render-time warning (not a gate): the render is still valid
// output and the CR must never be modified in place. The check is
// surgical — the normalized settings trees are queried for exactly
// this key: the CR-level configuration settings (where the CHK
// generator reads them, so every host inherits the misplacement) and
// each host's own settings (a host-scoped spec puts the key there
// instead). The named hosts make the warning actionable; a bare
// "check your spec" would force the operator to re-derive which host
// they belong to.
//
// Without it: nothing surfaces the misconfiguration until every keeper
// in the fleet is unreachable and the error has to be re-derived by
// hand (the live finding: no mntr, no replication, from one nesting
// mistake).
func keeperListenHostWarning(cr *chk.ClickHouseKeeperInstallation) string {
	var offenders []string
	cr.WalkHosts(func(host *v1.Host) error {
		if host.GetSettings().Has(keeperListenHostSetting) {
			offenders = append(offenders, host.Runtime.Address.HostName)
			return nil
		}
		// The CR-level settings apply cluster-wide: every host inherits
		// a misplaced key there, so each host is named once.
		if cr.GetSpecT().GetConfiguration().GetSettings().Has(keeperListenHostSetting) {
			offenders = append(offenders, host.Runtime.Address.HostName)
		}
		return nil
	})
	if len(offenders) == 0 {
		return ""
	}
	sort.Strings(offenders)
	return fmt.Sprintf(
		"WARNING: keeper_server/listen_host is IGNORED by clickhouse-keeper (it is a ROOT-level setting; "+
			"put <listen_host> in a root-level files entry in the spec) — the rendered config will not listen as intended (hosts: %s)",
		strings.Join(offenders, ", "))
}

// applyServerIDOverride replaces the rendered host server-id section's
// <server_id> value, deterministically and without touching the CR.
//
// Why a section replacement, not a regex on the merged document: the
// generator emits exactly one <server_id> and it lives in exactly one
// host-group file (chop-generated-server-id.xml); regenerating that
// section through the same Settings.ClickHouseConfig() machinery keeps
// the output byte-identical in shape to the generator's own render.
func applyServerIDOverride(rendered []port.RenderedFile, serverID int) {
	const serverIDFilename = "chop-generated-server-id.xml"
	replacement := v1.NewSettings().
		Set("keeper_server/server_id", v1.MustNewSettingScalarFromAny(serverID)).
		ClickHouseConfig()
	for i, file := range rendered {
		if file.Group != "host" || file.Path != serverIDFilename {
			continue
		}
		rendered[i].Content = replacement
		return
	}
	// No server-id section found: unreachable in single-file mode (a
	// host is required and its group always carries the section); kept
	// defensive so a future call path cannot silently drop the
	// override.
}

// checkServerIDRaftIDConsistency guards against a node being given two
// different identities in one render: -server-id N (the local
// <server_id>) and -raft-id <selectedHost>=N' (the ensemble's view of
// this node) must agree when both are given. A mismatch means the
// operator thinks this node is member N locally but member N' to the
// ensemble — ClickHouse Keeper requires server_id to match the node's
// own raft_configuration entry, and a mismatched pair fails at boot or,
// worse, silently partitions the ensemble.
//
// Only the SELECTED host's pair is checked: -raft-id legitimately maps
// other hosts (the whole ensemble's ids), while -server-id is always
// about this node.
func checkServerIDRaftIDConsistency(hostName string, serverID int, raftIDs map[string]int) error {
	if serverID <= 0 || len(raftIDs) == 0 {
		return nil
	}
	raftID, mapped := raftIDs[hostName]
	if !mapped {
		return nil
	}
	if raftID != serverID {
		return fmt.Errorf(
			"-server-id %d disagrees with -raft-id %s=%d for the selected host: a keeper node must have one identity (server_id must equal its raft_configuration id)",
			serverID, hostName, raftID)
	}
	return nil
}

// applyRaftIDOverrides rewrites the raft_configuration <server><id>
// values in the rendered raft section, deterministically and without
// touching the CR.
//
// Why a section rebuild, not a regex on the rendered bytes: the ids
// live in a machine-generated XML section the renderer already parses
// (parseSectionChildren); a parse-rebuild round trip preserves the
// CR's host order and every <server> child (hostname, port, secure)
// exactly, where a regex would re-derive fragile assumptions about
// indentation and nesting.
//
// Matching is POSITIONAL: the -raft-id flag keys by CR host name
// ("0-0"), which never appears in the rendered section (only the
// hostname does). The generator emits <server> entries in cr.WalkHosts
// order with no exclusions (the render passes no raft selector), so
// the i-th CR host corresponds to the i-th entry — the same order this
// function re-derives from the CR.
//
// Unmapped hosts keep their CR-derived ids.
func applyRaftIDOverrides(rendered []port.RenderedFile, raftIDs map[string]int, crHostNames []string) error {
	const raftFilename = "chop-generated-raft.xml"
	sectionIdx := -1
	for i, file := range rendered {
		if file.Group == "common" && file.Path == raftFilename {
			sectionIdx = i
			break
		}
	}
	if sectionIdx < 0 {
		// No raft section rendered (a CR without hosts is degenerate but
		// not invalid): nothing to rewrite. The mapping is then inert.
		return nil
	}

	servers, err := parseRaftServers(rendered[sectionIdx].Content)
	if err != nil {
		return fmt.Errorf("parse %s: %w", raftFilename, err)
	}
	if len(servers) != len(crHostNames) {
		return fmt.Errorf("raft section has %d <server> entries but the CR walks %d hosts — refusing to rewrite ids positionally",
			len(servers), len(crHostNames))
	}

	// Rebuild the section through the same Settings machinery the
	// generator's getRaftConfig uses, preserving entry order and every
	// non-id child (hostname, port, secure) verbatim.
	body := &bytes.Buffer{}
	// Indent 12 = 3-rd level (clickhouse/keeper_server/raft_configuration)
	// by 4 spaces — matches Generator.getRaftConfig's convention so the
	// replaced section is byte-identical in shape to the original.
	for i, server := range servers {
		id := server.id
		if override, mapped := raftIDs[crHostNames[i]]; mapped {
			id = override
		}
		util.Iline(body, 12, "<server>")
		util.Iline(body, 12, "    <id>%d</id>", id)
		util.Iline(body, 12, "    <hostname>%s</hostname>", server.hostname)
		util.Iline(body, 12, "    <port>%d</port>", server.port)
		if server.secure {
			util.Iline(body, 12, "    <secure>1</secure>")
		}
		util.Iline(body, 12, "</server>")
	}
	replacement := v1.NewSettings().
		Set("keeper_server/raft_configuration", v1.MustNewSettingScalarFromAny(body).SetEmbed()).
		ClickHouseConfig()
	rendered[sectionIdx].Content = replacement
	return nil
}

// keeperHostNames walks the CR's hosts in generator order and returns
// their CR names — the identity -raft-id keys by. Must stay in the same
// walk order as Generator.getRaftConfig (plain cr.WalkHosts, no
// exclusions) for the positional match in applyRaftIDOverrides.
func keeperHostNames(cr *chk.ClickHouseKeeperInstallation) []string {
	var names []string
	_ = cr.WalkHosts(func(host *v1.Host) error {
		names = append(names, host.Runtime.Address.HostName)
		return nil
	})
	return names
}

// raftServer is one parsed raft_configuration <server> entry.
type raftServer struct {
	// id is the rendered <id> (CR-derived replica index).
	id int
	// hostname is the rendered <hostname> (cloud DNS name when an
	// inventory was given).
	hostname string
	// port is the rendered raft <port>.
	port int
	// secure mirrors the <secure>1</secure> flag.
	secure bool
}

// parseRaftServers parses the rendered raft section and extracts the
// <server> entries in document order.
func parseRaftServers(content string) ([]raftServer, error) {
	children, err := parseSectionChildren(content)
	if err != nil {
		return nil, err
	}
	// Walk clickhouse → keeper_server → raft_configuration → server.
	var servers []raftServer
	var walk func(node *configXMLNode)
	walk = func(node *configXMLNode) {
		if node.Name == "raft_configuration" {
			for _, child := range node.Children {
				if child.Name != "server" {
					continue
				}
				server := raftServer{}
				for _, field := range child.Children {
					switch field.Name {
					case "id":
						server.id = parseXMLInt(field.Text)
					case "hostname":
						server.hostname = strings.TrimSpace(field.Text)
					case "port":
						server.port = parseXMLInt(field.Text)
					case "secure":
						server.secure = strings.TrimSpace(field.Text) == "1"
					}
				}
				servers = append(servers, server)
			}
			return
		}
		for _, child := range node.Children {
			walk(child)
		}
	}
	for _, child := range children {
		walk(child)
	}
	return servers, nil
}

// parseXMLInt parses element text as an int; unparsable text yields 0
// (the rebuild keeps the original bytes for non-id fields regardless).
func parseXMLInt(text string) int {
	n, err := strconv.Atoi(strings.TrimSpace(text))
	if err != nil {
		return 0
	}
	return n
}

// mergeRenderedSections merges every rendered section into ONE complete
// <clickhouse>-rooted document.
//
// Merge order mirrors clickhouse-keeper's on-cluster ConfigMap assembly:
// the per-host files (conf.d) merge first, the common files
// (keeper_config.d) after, so common wins any conflict — the merged
// single file behaves exactly like the two directories did together.
//
// Merge semantics mirror ClickHouse's own ConfigProcessor.mergeRecursive
// exactly: elements match by name and merge-control-free attributes;
// one merge pass per section; a matched pair merges recursively while a
// removal directive deletes its match — see mergeXMLChildren.
//
// Why parsing (not string surgery): the sections are generated XML rooted
// at <clickhouse>; an encoding/xml parse-extract-append round trip keeps
// the merge deterministic for any section shape the generator emits,
// where regex-scraping would re-derive fragile assumptions about
// whitespace and nesting.
func mergeRenderedSections(rendered []port.RenderedFile) (string, error) {
	root := &configXMLNode{Name: xmlTagClickHouse}
	// Host group first, common after: common wins conflicts (see doc
	// comment). Within a group, keep the caller's deterministic order.
	ordered := make([]port.RenderedFile, 0, len(rendered))
	for _, group := range []string{"host", "common"} {
		for _, file := range rendered {
			if file.Group == group {
				ordered = append(ordered, file)
			}
		}
	}
	for _, file := range ordered {
		nodes, err := parseSectionChildren(file.Content)
		if err != nil {
			return "", fmt.Errorf("merge section %s/%s: %w", file.Group, file.Path, err)
		}
		// One merge pass per section, exactly as ClickHouse runs one
		// pass per config.d file — sibling repetition inside a single
		// section (e.g. vector-valued settings such as listen_host)
		// appends rather than folding into the first entry.
		mergeXMLChildren(root, nodes)
	}
	return renderXMLDocument(root), nil
}

// xmlTagClickHouse is the root tag ClickHouse 26.8 keeper configuration
// uses (the operator's generator emits the same tag — see
// Settings.ClickHouseConfig). The legacy <yandex> tag remains an accepted
// equivalent in ClickHouse itself, but new documents are <clickhouse>.
const xmlTagClickHouse = "clickhouse"

// configXMLNode is a minimal DOM node for section merging.
type configXMLNode struct {
	// Name is the element tag.
	Name string
	// Attrs holds the element's attributes (name → value).
	Attrs []xml.Attr
	// Text is the element's character data.
	Text string
	// Children are the nested elements.
	Children []*configXMLNode
}

// parseSectionChildren parses one rendered section (a <clickhouse>- or
// <yandex>-rooted fragment, per the generator's Settings.ClickHouseConfig)
// and returns the root's child elements.
func parseSectionChildren(content string) ([]*configXMLNode, error) {
	if strings.TrimSpace(content) == "" {
		return nil, nil
	}
	decoder := xml.NewDecoder(strings.NewReader(content))
	var root *configXMLNode
	var stack []*configXMLNode
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		switch tok := token.(type) {
		case xml.StartElement:
			node := &configXMLNode{Name: tok.Name.Local, Attrs: copyAttrs(tok.Attr)}
			if len(stack) == 0 {
				// First element is the section root (<clickhouse> or
				// <yandex>); its own attributes stay on the root and its
				// children are what merges.
				root = node
				stack = append(stack, node)
				continue
			}
			parent := stack[len(stack)-1]
			parent.Children = append(parent.Children, node)
			stack = append(stack, node)
		case xml.EndElement:
			if len(stack) == 0 {
				return nil, fmt.Errorf("unbalanced XML: unexpected </%s>", tok.Name.Local)
			}
			stack = stack[:len(stack)-1]
		case xml.CharData:
			if len(stack) > 0 {
				// Append character data to the innermost open element;
				// the section root's own text is ignored (never rendered).
				stack[len(stack)-1].Text += string(tok)
			}
		case xml.Comment, xml.Directive, xml.ProcInst:
			// Comments and directives do not affect configuration
			// semantics; they are dropped (the merged document is
			// generated output, and deterministic bytes matter more
			// than provenance comments).
		}
	}
	if len(stack) != 0 {
		return nil, fmt.Errorf("unbalanced XML: unclosed <%s>", stack[len(stack)-1].Name)
	}
	if root == nil {
		return nil, nil
	}
	if root.Name != xmlTagClickHouse && root.Name != xmlTagYandex {
		return nil, fmt.Errorf("section root is <%s>, expected <%s> or <%s>", root.Name, xmlTagClickHouse, xmlTagYandex)
	}
	return root.Children, nil
}

// copyAttrs snapshots decoder-provided attributes: xml.Decoder reuses the
// Attr slice between tokens, so storing the slice directly would alias
// later elements' attributes.
func copyAttrs(attrs []xml.Attr) []xml.Attr {
	copied := make([]xml.Attr, len(attrs))
	copy(copied, attrs)
	return copied
}

// xmlTagYandex is the legacy root tag ClickHouse still accepts as an
// equivalent of <clickhouse>.
const xmlTagYandex = "yandex"

// mergeXMLChildren merges one incoming batch into target's children with
// ClickHouse's exact merge rules (ConfigProcessor::mergeRecursive):
//
//   - The candidate map snapshots the target's children BEFORE the pass;
//     nodes appended during the pass are not matchable (parity with the
//     upstream multimap built once per merge).
//   - An incoming element matches the first not-yet-consumed candidate
//     with the same merge-control-free identity (name + attributes).
//   - remove="1" on the incoming node deletes the matched target; a
//     removal with no match is dropped (never appended).
//   - replace="1" swaps the matched target for the incoming node.
//   - A matched pair merges: attributes copy over, and children recurse
//     (a fresh pass over the matched target's children).
//   - An unmatched incoming element appends.
//
// One batch == one section: repeated siblings in the SAME batch append
// (each finds no remaining candidate — the raft <server> list grows);
// the same element contributed by a LATER section merges into the
// earlier one, so common (merged after host) wins conflicts — the
// on-cluster keeper_config.d-over-conf.d precedence, kept intact in the
// single file.
func mergeXMLChildren(target *configXMLNode, incoming []*configXMLNode) {
	// Snapshot the candidates: name+attrs → queue of child indices, in
	// document order. Consumption erases from the queue head.
	candidates := make(map[string][]int)
	for i, child := range target.Children {
		id := elementIdentity(child)
		candidates[id] = append(candidates[id], i)
	}

	for _, node := range incoming {
		if node.Name == "" {
			continue
		}
		id := elementIdentity(node)
		queue := candidates[id]
		if isRemovedNode(node) {
			// Removal directive: delete the matched target; nothing to
			// add when there is no match (ConfigProcessor skips a remove
			// node that finds no pair).
			if len(queue) > 0 {
				idx := queue[0]
				candidates[id] = queue[1:]
				target.Children = append(target.Children[:idx], target.Children[idx+1:]...)
			}
			continue
		}
		if len(queue) > 0 {
			idx := queue[0]
			candidates[id] = queue[1:]
			if isReplaced(node) {
				target.Children[idx] = node
				continue
			}
			mergeInto(target.Children[idx], node)
			continue
		}
		target.Children = append(target.Children, node)
	}
}

// elementIdentity builds the merge match key: element name plus sorted
// merge-control-free attributes — ConfigProcessor's getElementIdentifier.
// Appended nodes are NOT added to any index (they are only matchable in
// a later merge pass), which callers get by rebuilding candidates per
// batch.
func elementIdentity(node *configXMLNode) string {
	var b strings.Builder
	b.WriteString(node.Name)
	attrs := mergeControlFreeAttrs(node.Attrs)
	// Decoder order is document order; identifiers must be stable across
	// documents, so sort (getElementIdentifier sorts attribute pairs).
	sort.Slice(attrs, func(i, j int) bool {
		return attrs[i].Name.Local < attrs[j].Name.Local
	})
	for _, attr := range attrs {
		b.WriteString("\x00" + attr.Name.Local + "\x01" + attr.Value)
	}
	return b.String()
}

// mergeInto merges a matched pair: the incoming node's attributes copy
// over the target's, non-empty incoming text replaces the target's text,
// and children merge recursively.
func mergeInto(target, incoming *configXMLNode) {
	for _, attr := range incoming.Attrs {
		replaced := false
		for i, existing := range target.Attrs {
			if existing.Name.Local == attr.Name.Local {
				target.Attrs[i].Value = attr.Value
				replaced = true
				break
			}
		}
		if !replaced {
			target.Attrs = append(target.Attrs, attr)
		}
	}
	if strings.TrimSpace(incoming.Text) != "" {
		target.Text = incoming.Text
	}
	mergeXMLChildren(target, incoming.Children)
}

// isRemovedNode reports whether the node carries remove="1" — the
// preprocessor directive ClickHouse uses to delete a previously-defined
// element (e.g. the plaintext <tcp_port> removal overlay).
func isRemovedNode(node *configXMLNode) bool {
	return mergeDirective(node, "remove")
}

// isReplaced reports whether the node carries replace="1".
func isReplaced(node *configXMLNode) bool {
	return mergeDirective(node, "replace")
}

// mergeDirective reports whether the node carries attr="1".
func mergeDirective(node *configXMLNode, directive string) bool {
	for _, attr := range node.Attrs {
		if attr.Name.Local == directive {
			return attr.Value == "1" || strings.EqualFold(attr.Value, "true")
		}
	}
	return false
}

// mergeControlFreeAttrs strips remove/replace attributes — the merge
// control directives ClickHouse's getElementIdentifier excludes so a
// removal directive can match the plain element it deletes.
func mergeControlFreeAttrs(attrs []xml.Attr) []xml.Attr {
	filtered := make([]xml.Attr, 0, len(attrs))
	for _, attr := range attrs {
		switch attr.Name.Local {
		case "remove", "replace":
		default:
			filtered = append(filtered, attr)
		}
	}
	return filtered
}

// renderXMLDocument renders the merged DOM back to a stable, readable XML
// document rooted at <clickhouse>, with the same 4-space indent the
// operator's own generator emits.
func renderXMLDocument(root *configXMLNode) string {
	var b strings.Builder
	b.WriteString("<?xml version=\"1.0\"?>\n")
	writeXMLNode(&b, root, 0)
	return b.String()
}

// writeXMLNode writes one node (and its subtree) at the given indent
// level. Empty element text and childless leaves render compactly.
func writeXMLNode(b *strings.Builder, node *configXMLNode, indent int) {
	pad := strings.Repeat("    ", indent)
	b.WriteString(pad)
	b.WriteString("<" + node.Name)
	for _, attr := range node.Attrs {
		b.WriteString(" " + attr.Name.Local + "=\"" + xmlEscapeAttr(attr.Value) + "\"")
	}
	if len(node.Children) == 0 && strings.TrimSpace(node.Text) == "" {
		b.WriteString("/>\n")
		return
	}
	b.WriteString(">")
	if len(node.Children) == 0 {
		b.WriteString(xmlEscapeText(strings.TrimSpace(node.Text)))
		b.WriteString("</" + node.Name + ">\n")
		return
	}
	if node.Text != "" && strings.TrimSpace(node.Text) != "" {
		b.WriteString(xmlEscapeText(strings.TrimSpace(node.Text)))
	}
	b.WriteString("\n")
	for _, child := range node.Children {
		writeXMLNode(b, child, indent+1)
	}
	b.WriteString(pad + "</" + node.Name + ">\n")
}

// xmlEscapeText escapes character data for element text.
func xmlEscapeText(text string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(text)
}

// xmlEscapeAttr escapes a value for a double-quoted attribute.
func xmlEscapeAttr(value string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;").Replace(value)
}

// writeSingleFile writes the merged single-file keeper configuration.
func writeSingleFile(path, content string) error {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", dir, err)
		}
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// chiGeneratorOptions mirrors the CHI controller's configGeneratorOptions:
// users/profiles/quotas/settings/files flow from the CR's configuration
// section into the generator.
//
// Why: skipping any field silently drops whole spec sections from the
// rendered files; the controller's own wiring is the reference.
func chiGeneratorOptions(cr *v1.ClickHouseInstallation) *chiConfig.GeneratorOptions {
	return &chiConfig.GeneratorOptions{
		Users:          cr.GetSpecT().GetConfiguration().GetUsers(),
		Profiles:       cr.GetSpecT().GetConfiguration().GetProfiles(),
		Quotas:         cr.GetSpecT().GetConfiguration().GetQuotas(),
		Settings:       cr.GetSpecT().GetConfiguration().GetSettings(),
		Files:          cr.GetSpecT().GetConfiguration().GetFiles(),
		DistributedDDL: cr.GetSpecT().GetDefaults().GetDistributedDDL(),
	}
}

// chkGeneratorOptions mirrors the CHK controller's configGeneratorOptions.
func chkGeneratorOptions(cr *chk.ClickHouseKeeperInstallation) *chkConfig.GeneratorOptions {
	return &chkConfig.GeneratorOptions{
		Settings: cr.GetSpecT().GetConfiguration().GetSettings(),
		Files:    cr.GetSpecT().GetConfiguration().GetFiles(),
	}
}

// withoutMachineryUser deep-copies the users settings and drops the
// operator's machinery user (chop.Config().ClickHouse.Access.Username)
// injected by the normalizer.
//
// Why drop pre-emission (not post-process the rendered XML): the
// generator owns the users XML — ordering, nesting, and value escaping.
// Editing the settings before generation means the file is emitted by
// the operator's own emitter, byte-identical to an on-cluster render of
// a spec without the machinery user; a post-process pass (regex or
// re-parse) re-implements XML editing with new escaping and tag-mismatch
// failure classes. The deep copy keeps the shared CR untouched — the
// normalizer-added machinery user stays in the tree for any later
// render that wants the operator-native output.
//
// Without it: the machinery user renders into users.d off-cluster, where
// its predefined profile (operator-shipped on-cluster only) does not
// exist, and ClickHouse refuses the whole users.d load — Code 347
// "Profile clickhouse_operator was not found", the live boot crash.
func withoutMachineryUser(users *v1.Settings) *v1.Settings {
	machinery := chop.Config().ClickHouse.Access.Username
	if users == nil {
		return nil
	}
	if !userSettingsPrefixExists(users, machinery) {
		return users
	}
	emitted := users.DeepCopy()
	// SettingsUser.Delete routes through the "username/" storage-key
	// prefix converter, so each Delete removes exactly that user's
	// field — the same discipline injectUserPasswords applies.
	fields := v1.NewSettingsUser(emitted, machinery)
	for _, name := range fields.Names() {
		fields.Delete(name)
	}
	return emitted
}

// injectUserPasswords deep-copies the users settings and injects one
// password_sha256_hex per requested user, stripping every competing
// password specification for that user.
//
// Why a settings copy (not post-processing the rendered XML): the
// generator already owns the users XML — ordering, nesting, and value
// escaping. Editing the settings before generation means the injected
// hash is emitted by the operator's own emitter, byte-identical to how
// an on-cluster spec-declared hash would be; a post-process pass would
// re-implement XML editing (regex or re-parse) with new escaping and
// tag-mismatch failure classes (the live cluster's SAXParseException).
// The deep copy keeps the shared CR untouched — the same CR renders
// again in the same process (common+users+host groups, -dry-run), and
// in-place mutation would leak the injected hash into later renders.
//
// Without it: the spec would have to carry the password hash itself —
// a runtime secret checked into the spec file, the exact thing the
// cloud secret store exists to prevent; or plaintext `password` from a
// hand-migrated spec would silently win and land in the rendered XML.
func injectUserPasswords(users *v1.Settings, userPasswords map[string]string) (*v1.Settings, error) {
	if users == nil {
		users = v1.NewSettings()
	}
	// Typo guard: every requested user must be declared by the spec.
	// A silent no-op would render a users.xml still carrying the spec's
	// password while the operator believes the secret-file password is
	// live — the account would run with a credential nobody set.
	for _, name := range sortedKeys(userPasswords) {
		if !userSettingsPrefixExists(users, name) {
			return nil, fmt.Errorf(
				"user %q from -user-password is not declared by the spec (spec users: %v)",
				name, users.Prefixes(true))
		}
	}

	// Deep copy: the CR renders again in this process (host group, later
	// renders) and must never observe the injected hashes.
	emitted := users.DeepCopy()
	if emitted == nil {
		emitted = v1.NewSettings()
	}
	for _, name := range sortedKeys(userPasswords) {
		// SettingsUser keys fields by the "name/" storage-key prefix.
		fields := v1.NewSettingsUser(emitted, name)
		// Strip every competing password spec so the injected hash is
		// the only credential ClickHouse can pick: a spec-declared
		// password_double_sha1_hex would otherwise out-rank
		// password_sha256_hex in ClickHouse's own priority order.
		// (k8s_secret_* fields are already impossible here — the
		// SpecLoader rejects that syntax at load time.)
		fields.Delete("password")
		fields.Delete("password_double_sha1_hex")
		// Operator parity with the normalizer's removePlainPassword:
		// "default" is the one user stock users.xml carries a <password>
		// element for, so a hash must blank it via remove="1" or the
		// users.d merge leaves two auth specs on the account. Custom
		// users don't exist in stock users.xml — no stub, matching the
		// on-cluster ConfigMap byte-for-byte.
		if name == "default" {
			fields.Set("password", v1.NewSettingScalar("").SetAttribute("remove", "1"))
		}
		fields.Set("password_sha256_hex", v1.NewSettingScalar(userPasswords[name]))
	}
	return emitted, nil
}

// userSettingsPrefixExists reports whether the users settings carry any
// field for the named user (storage key prefix "name/").
func userSettingsPrefixExists(users *v1.Settings, name string) bool {
	if users == nil {
		return false
	}
	prefix := name + "/"
	found := false
	users.WalkKeys(func(key string, _ *v1.Setting) {
		if strings.HasPrefix(key, prefix) {
			found = true
		}
	})
	return found
}

// sortedKeys returns the map's keys sorted (deterministic error order).
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// hostInventory converts the port-level host list to the CR-name →
// cloud-address map the cloud namer keys by. Duplicate names keep the
// last value (the schema/replica commands' inventory semantics).
func hostInventory(hosts []domain.HostIdentity) map[string]string {
	if len(hosts) == 0 {
		return nil
	}
	addresses := make(map[string]string, len(hosts))
	for _, h := range hosts {
		if h.Name == "" || h.Address == "" {
			continue
		}
		addresses[h.Name] = h.Address
	}
	return addresses
}

// mergeHostAddresses overlays the secondary map over the primary,
// returning the merged CR-name → cloud-address map (nil when both are
// empty, so the plain namer fast path stays).
func mergeHostAddresses(primary, secondary map[string]string) map[string]string {
	if len(primary) == 0 {
		return secondary
	}
	merged := make(map[string]string, len(primary)+len(secondary))
	for name, address := range primary {
		merged[name] = address
	}
	for name, address := range secondary {
		merged[name] = address
	}
	return merged
}

// findHost locates a host by name within a CHI.
//
// WalkHosts appends the closure's return value per host, so a clean walk
// still yields a []error of nils; filter them before treating the slice as
// failures.
func findHost(cr *v1.ClickHouseInstallation, name string) (*v1.Host, error) {
	var found *v1.Host
	errs := cr.WalkHosts(func(host *v1.Host) error {
		if host.Runtime.Address.HostName == name {
			found = host
		}
		return nil
	})
	if err := firstErr(errs); err != nil {
		return nil, fmt.Errorf("walk hosts: %v", err)
	}
	if found == nil {
		return nil, fmt.Errorf("host %q not found in CHI %q", name, cr.Name)
	}
	return found, nil
}

// findKeeperHost locates a keeper host by name within a CHK. Keeper CRs
// reuse the shared v1.Host type.
func findKeeperHost(cr *chk.ClickHouseKeeperInstallation, name string) (*v1.Host, error) {
	var found *v1.Host
	errs := cr.WalkHosts(func(host *v1.Host) error {
		if host.Runtime.Address.HostName == name {
			found = host
		}
		return nil
	})
	if err := firstErr(errs); err != nil {
		return nil, fmt.Errorf("walk keeper hosts: %v", err)
	}
	if found == nil {
		return nil, fmt.Errorf("keeper host %q not found in CHK %q", name, cr.Name)
	}
	return found, nil
}

// firstErr returns the first non-nil error, or nil.
func firstErr(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// toRenderedFiles maps generator output (filename → content) to
// RenderedFile, deterministically ordered so output stays stable across
// map iterations.
func toRenderedFiles(group string, files map[string]string) []port.RenderedFile {
	rendered := make([]port.RenderedFile, 0, len(files))
	for name, content := range files {
		rendered = append(rendered, port.RenderedFile{
			Group:   group,
			Path:    name,
			Content: content,
		})
	}
	sort.Slice(rendered, func(i, j int) bool {
		return rendered[i].Path < rendered[j].Path
	})
	return rendered
}

// injectReplicaCredentials splices <user>/<password> children into
// every <replica> entry of the remote_servers.xml content carried by
// rendered.
//
// Why string splicing over the GeneratorOptions: the operator's
// generator has no per-replica credential support — its cluster-level
// <secret> element is rejected by ClickHouse 26.8 — and injecting through
// the shared generator options would fork generator behavior for every
// downstream consumer. The generator emits each <replica> through one
// deterministic function, so inserting two elements before each
// </replica> close-tag produces byte-clean XML while leaving pkg/model
// untouched.
//
// Without it: off-cluster peers reject distributed queries as
// passwordless "default" (PASSWORD_REQUIRED) — the AWS stack worked
// around this by hand-writing cluster.xml, which drifts from the spec
// the moment topology changes.
func injectReplicaCredentials(rendered []port.RenderedFile, user, password string) error {
	var escapedUser, escapedPassword bytes.Buffer
	if err := xml.EscapeText(&escapedUser, []byte(user)); err != nil {
		return fmt.Errorf("XML-escape remote user: %w", err)
	}
	if err := xml.EscapeText(&escapedPassword, []byte(password)); err != nil {
		// The error must not carry the value — secrets hygiene.
		return fmt.Errorf("XML-escape remote password: %w", err)
	}

	for i, file := range rendered {
		// Exact generated name (createConfigSectionFilename's output):
		// a spec `files:` entry that merely ends in remote_servers.xml
		// must not be mistaken for the generator's own cluster file.
		if filepath.Base(file.Path) != "chop-generated-remote_servers.xml" {
			continue
		}
		// The generator writes every </replica> as a newline plus 16
		// spaces (util.Iline(b, 16, "</replica>")), and replica
		// children carry 4 more (util.Iline(b, 16, "    <host>...")).
		// Matching those exact forms keeps the splice byte-identical
		// to generator style without a full XML re-serialization
		// that would reformat the whole file.
		const replicaClose = "\n                </replica>"
		if !strings.Contains(file.Content, replicaClose) {
			return fmt.Errorf("no <replica> entries found in %s", file.Path)
		}
		rendered[i].Content = strings.ReplaceAll(file.Content, replicaClose,
			"\n                    <user>"+escapedUser.String()+"</user>\n"+
				"                    <password>"+escapedPassword.String()+"</password>\n"+
				"                </replica>")
	}
	return nil
}

// writeFiles writes rendered files to disk. users-group files go to
// UsersDir (default OutputDir) so callers can keep config.d and users.d
// apart. Files are 0640 (config and users alike): users XML carries
// password_sha256_hex and remote_servers.xml may embed distributed-query
// credentials, so rendered config must stay group-readable at most,
// never world-readable. The service user reads via the clickhouse group;
// the cloud-init step re-owns the tree anyway.
func writeFiles(rendered []port.RenderedFile, outputDir, usersDir string) error {
	if usersDir == "" {
		usersDir = outputDir
	}
	for _, file := range rendered {
		dir := outputDir
		if file.Group == "users" {
			dir = usersDir
		}
		path := filepath.Join(dir, filepath.Base(file.Path))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(file.Content), 0o640); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return nil
}
