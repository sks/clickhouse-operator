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
	"strings"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// cloudNamer wraps the operator's name manager, translating pod-pattern
// output into cloud values per host.
//
// Why: every operator code path resolves hosts through pattern-based
// naming with no per-host cloud override. Off-cluster the Kubernetes
// patterns are either unresolvable (schemer connection targets,
// remote_servers <host>, keeper raft <hostname>, interserver_http_host)
// or actively harmful (macros <replica> — re-identifying a node on an
// existing cluster breaks Keeper replica paths and per-replica S3
// prefixes). The fix is the same for all of them: intercept the name
// form at the name-manager boundary and return the cloud value mapped
// from the host's CR name.
//
// Translated forms:
//   - NameFQDNs / NameFQDN (schemer exec targets) → cloud address
//   - NameInstanceHostname (remote_servers <host>, keeper raft
//     <hostname>, interserver_http_host) → cloud address
//   - NamePodHostname (macros <replica>) → the mapped cloud short
//     hostname, e.g. "writer-0" — the value replica identity must carry
//     off-cluster
//
// All other name forms (replica names, service names, stateful-set
// names) pass through untouched — only the network-address and identity
// forms above are cloud-shaped.
type cloudNamer struct {
	// inner is the operator's name manager (CHI or Keeper flavor).
	inner interfaces.INameManager
	// addresses maps CR host names (e.g. "0-0") to cloud addresses.
	addresses map[string]string
	// replicas maps CR host names to the short cloud hostname
	// ClickHouse replica identity must use (e.g. "0-0" → "writer-0").
	// Empty keeps the operator's native identity — backwards compatible.
	replicas map[string]string
}

// newCloudNamer builds a wrapped CHI namer over the operator's default.
func newCloudNamer(addresses map[string]string) *cloudNamer {
	return &cloudNamer{
		inner:     managers.NewNameManager(managers.NameManagerTypeClickHouse),
		addresses: addresses,
	}
}

// newKeeperCloudNamer builds a wrapped Keeper namer: the CHK generator
// uses NameInstanceHostname for raft server hostnames, which the default
// namer renders in the Kubernetes service pattern — unresolvable
// off-cluster. The Keeper flavor translates that form to cloud DNS.
func newKeeperCloudNamer(addresses map[string]string) *cloudNamer {
	return &cloudNamer{
		inner:     managers.NewNameManager(managers.NameManagerTypeKeeper),
		addresses: addresses,
	}
}

// withReplicas attaches the replica-identity mapping and returns the
// same namer (builder-style).
func (n *cloudNamer) withReplicas(replicas map[string]string) *cloudNamer {
	n.replicas = replicas
	return n
}

// Names implements interfaces.INameManager; FQDN lists are translated,
// everything else delegates.
func (n *cloudNamer) Names(what interfaces.NameType, params ...any) []string {
	names := n.inner.Names(what, params...)
	if what != interfaces.NameFQDNs {
		return names
	}
	translated := make([]string, 0, len(names))
	for _, name := range names {
		translated = append(translated, n.translate(name, params...))
	}
	return translated
}

// Name implements interfaces.INameManager. Address forms (NameFQDN,
// NameInstanceHostname) translate to the cloud address; NamePodHostname
// — the macros <replica> identity — becomes the mapped cloud short
// hostname exactly (Keeper replica paths and per-replica S3 prefixes
// encode this identity). Everything else delegates operator-native.
func (n *cloudNamer) Name(what interfaces.NameType, params ...any) string {
	switch what {
	case interfaces.NameFQDN, interfaces.NameInstanceHostname:
		return n.translate(n.inner.Name(what, params...), params...)
	case interfaces.NamePodHostname:
		if name, ok := n.replicaName(params...); ok {
			return name
		}
	}
	return n.inner.Name(what, params...)
}

// translate maps one pod-pattern name to its cloud address when the
// call's host param carries a CR name we know. Unknown hosts pass
// through (a host outside the inventory keeps the operator name —
// callers rely on the error downstream).
// translate maps one pod-pattern name to its cloud value. Matching:
//  1. the call's Host params whose NATIVE name equals the output name
//     (NameFQDN / NamePodHostname / NameInstanceHostname) — an output
//     name belongs to exactly one of the hosts in the params;
//  2. the CR ordinal parsed from the pod-pattern name itself
//     (chi-{CR}-{cluster}-{shard}-{replica}...) — Names() calls that
//     build lists from cluster/shard scope objects pass NO host param
//     for the listed hosts;
//  3. legacy first-Host fallback for single-target calls (ExecHost).
//
// Picking the first Host param unconditionally collapses every name in a
// multi-host Names(NameFQDNs, host, scope, excludeSelf) list onto the
// first host's address — the schema-sync peer list (excludeSelf=true
// passes the SELF host first) degenerated to [self] and QueryAny
// queried an empty fresh replica as the only "peer" (the live reader-0
// finding: zero schema propagated).
func (n *cloudNamer) translate(name string, params ...any) string {
	for _, param := range params {
		host, ok := param.(*v1.Host)
		if !ok {
			continue
		}
		if n.inner.Name(interfaces.NameFQDN, host) != name && n.inner.Name(interfaces.NamePodHostname, host) != name && n.inner.Name(interfaces.NameInstanceHostname, host) != name {
			// Not the host whose native name this is — keep looking.
			continue
		}
		if address, found := n.addresses[host.Runtime.Address.HostName]; found {
			return address
		}
		return name
	}
	// Scope-object lists carry no Host param per output name: parse the
	// CR ordinal from the pod-pattern name (chi-...-{shard}-{replica}).
	if crHost := crHostFromPodPattern(name); crHost != "" {
		if address, found := n.addresses[crHost]; found {
			return address
		}
	}
	// Single-target calls (ExecHost): the first Host param IS the target.
	for _, param := range params {
		host, ok := param.(*v1.Host)
		if !ok {
			continue
		}
		if address, found := n.addresses[host.Runtime.Address.HostName]; found {
			return address
		}
		break
	}
	return name
}

// crHostFromPodPattern extracts the CR host ordinal (shard-replica,
// e.g. "0-1") from a pod-pattern name. The pod hostname pattern is
// chi-{CR}-{cluster}-{shard}-{replica} and the FQDN appends the
// namespace domain; the trailing "-{shard}-{replica}" of the first
// label is the CR host name the addresses map is keyed by. Non-matching
// shapes (custom patterns) yield "" and translation falls through.
func crHostFromPodPattern(name string) string {
	label := strings.SplitN(name, ".", 2)[0]
	parts := strings.Split(label, "-")
	if len(parts) < 2 {
		return ""
	}
	shard, replica := parts[len(parts)-2], parts[len(parts)-1]
	if !isOrdinal(shard) || !isOrdinal(replica) {
		return ""
	}
	return shard + "-" + replica
}

// isOrdinal reports whether s is a non-negative integer (the shard and
// replica components of a CR host name are numeric).
func isOrdinal(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// replicaName returns the mapped cloud short hostname for the call's
// host when one exists — the CRITICAL identity invariant: the rendered
// macros <replica> must be exactly the cloud name (writer-0, reader-3),
// never an operator pod pattern. Unmapped hosts report false and keep
// the operator name.
func (n *cloudNamer) replicaName(params ...any) (string, bool) {
	for _, param := range params {
		host, ok := param.(*v1.Host)
		if !ok {
			continue
		}
		if name, found := n.replicas[host.Runtime.Address.HostName]; found {
			return name, true
		}
	}
	return "", false
}
