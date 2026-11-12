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

// cloudNamer wraps the operator's CHI name manager, translating pod-FQDN
// output into cloud addresses.
//
// Why: every schemer execution path (HostCreateTables, HostSyncTables,
// HostDropReplica) resolves the exec host via
// namer.Names(interfaces.NameFQDNs, host, ...), whose output is the
// Kubernetes pod-FQDN pattern chi-<chi>-<cluster>-<s>-<r>.<ns>.svc... —
// unresolvable off-cluster. The operator's naming is pattern-based with
// no per-host cloud override, so the fix is to intercept the FQDN form
// at the name-manager boundary and return the cloud address mapped from
// the host's CR name. All other name forms (replica names, service
// names, stateful-set names) pass through untouched — replica naming
// must stay operator-native so replication paths match.
type cloudNamer struct {
	// inner is the operator's CHI name manager.
	inner interfaces.INameManager
	// addresses maps CR host names (e.g. "0-0") to cloud addresses.
	addresses map[string]string
}

// newCloudNamer builds a wrapped namer over the operator's default.
func newCloudNamer(addresses map[string]string) *cloudNamer {
	return &cloudNamer{
		inner:     managers.NewNameManager(managers.NameManagerTypeClickHouse),
		addresses: addresses,
	}
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

// Name implements interfaces.INameManager.
func (n *cloudNamer) Name(what interfaces.NameType, params ...any) string {
	name := n.inner.Name(what, params...)
	if what != interfaces.NameFQDNs {
		return name
	}
	return n.translate(name, params...)
}

// translate maps one pod-FQDN to its cloud address when the call's host
// param carries a CR name we know. Unknown hosts pass through (a host
// outside the inventory keeps the operator name — callers rely on the
// error downstream).
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
