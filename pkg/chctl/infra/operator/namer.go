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
func (n *cloudNamer) translate(fqdn string, params ...any) string {
	for _, param := range params {
		host, ok := param.(*v1.Host)
		if !ok {
			continue
		}
		if address, found := n.addresses[host.Runtime.Address.HostName]; found {
			return address
		}
	}
	return fqdn
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
