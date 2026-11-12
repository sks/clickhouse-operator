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
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain"
	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"

	"github.com/stretchr/testify/require"
)

// chiSpecPath points at the example CHI spec carried in the repo for
// smoke-testing the render path.
const chiSpecPath = "../../../../docs/chctl-example-chi.yaml"

// loadCHIFromFile runs the SpecLoader over the example spec.
func loadCHIFromFile(t *testing.T, name, namespace string) *v1.ClickHouseInstallation {
	t.Helper()
	data, err := os.ReadFile(chiSpecPath)
	require.NoError(t, err)

	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: data, Name: name, Namespace: namespace},
	})
	require.NoError(t, err)
	cr, ok := crAny.(*v1.ClickHouseInstallation)
	require.True(t, ok, "loader returned non-CHI")
	return cr
}

func TestSpecLoaderLoadCHINormalizes(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	require.Equal(t, "ch-int-dev", cr.Name)

	// Normalization must produce at least one host with a non-empty
	// runtime address — the config generator panics without one.
	hosts := 0
	_ = cr.WalkHosts(func(host *v1.Host) error {
		hosts++
		require.NotEmpty(t, host.Runtime.Address.HostName)
		require.NotEmpty(t, host.Runtime.Address.ClusterName)
		require.NotNil(t, host.GetCR())
		return nil
	})
	require.NotZero(t, hosts)
}

func TestSpecLoaderLoadCHIRejectsRemovedSecretRefSyntax(t *testing.T) {
	// The 0.27.4 RemovedSecretRefSyntax hardening must hold off-cluster
	// too: a spec using the removed k8s_secret_ syntax is refused.
	bad := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: bad
spec:
  configuration:
    users:
      admin/k8s_secret_password: "ns/secret/key"
`)
	loader := NewSpecLoader()
	_, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: bad},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "spec rejected")
}

func TestSpecLoaderParseSpecYAMLToJSON(t *testing.T) {
	// Settings has only UnmarshalJSON; direct yaml.Unmarshal silently
	// drops users/profiles — this pins the yaml→JSON conversion.
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: settings-check
spec:
  configuration:
    profiles:
      default/async_insert: "1"
`)
	raw := &v1.ClickHouseInstallation{}
	require.NoError(t, parseSpec(spec, raw))

	profiles := raw.GetSpecT().GetConfiguration().GetProfiles()
	require.NotNil(t, profiles)
	require.True(t, profiles.Has("default/async_insert"),
		"profiles must survive YAML parsing")
}

func TestConfigRendererRenderCHI(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	hostName := ""
	_ = cr.WalkHosts(func(h *v1.Host) error {
		hostName = h.Runtime.Address.HostName
		return nil
	})
	require.NotEmpty(t, hostName)

	renderer := NewConfigRenderer()
	rendered, err := renderer.RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR: cr,
		HostName:     hostName,
		OutputDir:    t.TempDir(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, rendered)

	groups := map[string]int{}
	for _, file := range rendered {
		groups[file.Group]++
		require.NotEmpty(t, file.Content)
	}
	require.NotZero(t, groups["host"], "host-scoped files must render")
	require.NotZero(t, groups["common"], "common files must render")
	require.NotZero(t, groups["users"], "users files must render")
}

func TestConfigRendererRenderCHIUnknownHost(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	renderer := NewConfigRenderer()
	_, err := renderer.RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR: cr,
		HostName:     "no-such-host",
		OutputDir:    t.TempDir(),
		DryRun:       true,
	})
	require.ErrorContains(t, err, "not found")
}

// newGCRunner builds a SchemaRunner over the example spec with an
// inventory, for detection-SQL and cluster-gate tests.
func newGCRunner(t *testing.T) *SchemaRunner {
	t.Helper()
	cr := loadCHIFromFile(t, "ch-int-dev", "")
	factory := NewSchemaClientFactory()
	runnerAny, err := factory.NewClient(context.Background(), port.NewSchemaClientRequest{
		ClusterName:  "logexplorer",
		Credentials:  domain.Credentials{Username: "default"},
		Hosts:        []domain.HostIdentity{{Name: "0-0", Address: "writer-0.example.internal"}},
		HTTPPort:     8123,
		NormalizedCR: cr,
	})
	require.NoError(t, err)
	runner, ok := runnerAny.(*SchemaRunner)
	require.True(t, ok, "factory returned non-SchemaRunner")
	return runner
}

func TestSchemaRunnerStaleReplicaDetectionSQL(t *testing.T) {
	runner := newGCRunner(t)
	sql := runner.sqlFindStaleReplicas()

	// The query shape the bash version pinned: NOT IN over the
	// clusterAllReplicas fan-out, with skip_unavailable_shards so down
	// peers don't fail the sweep.
	require.Contains(t, sql, "SELECT DISTINCT host_name")
	require.Contains(t, sql, "FROM system.replicas")
	require.Contains(t, sql, "NOT IN")
	require.Contains(t, sql, fmt.Sprintf("clusterAllReplicas('%s', system.replicas)", runner.clusterName))
	require.Contains(t, sql, "skip_unavailable_shards = 1")
	// The original bash's LIKE gate must NOT be in the SQL — the pattern
	// is applied in Go (service layer) so the SQL stays simple.
	require.NotContains(t, sql, "LIKE")
}

func TestSchemaRunnerRejectsUndeclaredClusterName(t *testing.T) {
	// The detection SQL interpolates the cluster name; only a cluster the
	// loaded CR declares may ever reach the SQL.
	cr := loadCHIFromFile(t, "ch-int-dev", "")
	factory := NewSchemaClientFactory()
	_, err := factory.NewClient(context.Background(), port.NewSchemaClientRequest{
		ClusterName:  "evil'; DROP TABLE system.replicas; --",
		Credentials:  domain.Credentials{Username: "default"},
		Hosts:        []domain.HostIdentity{{Name: "0-0", Address: "writer-0.example.internal"}},
		NormalizedCR: cr,
	})
	require.ErrorContains(t, err, "not declared by the CR")
}

// splitAddr breaks an httptest URL into host and integer port — the
// inventory carries plain hosts; the driver appends the HTTP port
// itself, so an address with a port suffix produces a double-port DSN.
func splitAddr(t *testing.T, serverURL string) (string, int) {
	t.Helper()
	parsed, err := url.Parse(serverURL)
	require.NoError(t, err)
	p, err := strconv.Atoi(parsed.Port())
	require.NoError(t, err)
	return parsed.Hostname(), p
}

func TestSchemaRunnerFindStaleReplicasOverWire(t *testing.T) {
	// End-to-end detection over the driver's real HTTP path: the query
	// the server received must be the whitelisted fan-out SQL, and the
	// TSV rows must unzip into the stale host list.
	var captured string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "text/tab-separated-values; charset=UTF-8")
		// The driver pings the pool with "SELECT 1" before the first
		// query; answer every non-detection query with the scalar 1.
		if !strings.Contains(string(query), "system.replicas") {
			_, _ = w.Write([]byte("1\n"))
			return
		}
		captured = string(query)
		_, _ = w.Write([]byte("host_name\nString\nreader-3\nreader-9\n"))
	}))
	t.Cleanup(server.Close)
	host, httpPort := splitAddr(t, server.URL)

	cr := loadCHIFromFile(t, "ch-int-dev", "")
	runnerAny, err := NewSchemaClientFactory().NewClient(context.Background(), port.NewSchemaClientRequest{
		ClusterName:  "logexplorer",
		Credentials:  domain.Credentials{Username: "default"},
		Hosts:        []domain.HostIdentity{{Name: "0-0", Address: host}},
		HTTPPort:     httpPort,
		NormalizedCR: cr,
	})
	require.NoError(t, err)

	hosts, err := runnerAny.FindStaleReplicas(context.Background(), domain.ReplicaGcRequest{RunOn: "0-0"})
	require.NoError(t, err)
	require.Equal(t, []string{"reader-3", "reader-9"}, hosts)

	// The SQL the server actually received: the whitelisted cluster name
	// in clusterAllReplicas and the skip_unavailable_shards tolerance.
	require.Contains(t, captured, "clusterAllReplicas('logexplorer', system.replicas)")
	require.Contains(t, captured, "skip_unavailable_shards = 1")
	require.Contains(t, captured, "SELECT DISTINCT host_name")
	require.NotContains(t, captured, "LIKE")
}

func TestSchemaRunnerFindStaleReplicasEmptyResult(t *testing.T) {
	// A fresh cluster: only the header row, zero stale hosts, no error.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "text/tab-separated-values; charset=UTF-8")
		if !strings.Contains(string(query), "system.replicas") {
			_, _ = w.Write([]byte("1\n"))
			return
		}
		_, _ = w.Write([]byte("host_name\nString\n"))
	}))
	t.Cleanup(server.Close)
	host, httpPort := splitAddr(t, server.URL)

	cr := loadCHIFromFile(t, "ch-int-dev", "")
	runnerAny, err := NewSchemaClientFactory().NewClient(context.Background(), port.NewSchemaClientRequest{
		ClusterName:  "logexplorer",
		Credentials:  domain.Credentials{Username: "default"},
		Hosts:        []domain.HostIdentity{{Name: "0-0", Address: host}},
		HTTPPort:     httpPort,
		NormalizedCR: cr,
	})
	require.NoError(t, err)

	hosts, err := runnerAny.FindStaleReplicas(context.Background(), domain.ReplicaGcRequest{RunOn: "0-0"})
	require.NoError(t, err)
	require.Empty(t, hosts)
}

func TestSchemaRunnerFindStaleReplicasUnknownPeer(t *testing.T) {
	runner := newGCRunner(t)
	_, err := runner.FindStaleReplicas(context.Background(), domain.ReplicaGcRequest{RunOn: "no-such-peer"})
	require.ErrorContains(t, err, "not in inventory")
}

func TestConfigRendererWritesGroupedDirs(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	outDir := t.TempDir()
	usersDir := t.TempDir()

	renderer := NewConfigRenderer()
	rendered, err := renderer.RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR: cr,
		OutputDir:    outDir,
		UsersDir:     usersDir,
	})
	require.NoError(t, err)

	for _, file := range rendered {
		dir := outDir
		if file.Group == "users" {
			dir = usersDir
		}
		_, err := os.Stat(filepath.Join(dir, filepath.Base(file.Path)))
		require.NoError(t, err)
	}
}

// renderCHIFiles renders the example CHI with the given inventory and
// replica mapping and returns the files keyed by file name.
func renderCHIFiles(t *testing.T, request port.RenderCHIRequest) map[string]string {
	t.Helper()
	request.DryRun = true
	if request.OutputDir == "" {
		request.OutputDir = t.TempDir() // rendered content only; dry-run never writes
	}
	rendered, err := NewConfigRenderer().RenderCHI(context.Background(), request)
	require.NoError(t, err)
	files := make(map[string]string, len(rendered))
	for _, file := range rendered {
		files[filepath.Base(file.Path)] = file.Content
	}
	return files
}

// TestConfigRendererCloudInventory asserts the cloud-inventory render:
// every remote_servers <host> entry and the interserver_http_host carry
// the cloud address instead of the unresolvable pod pattern.
func TestConfigRendererCloudInventory(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	files := renderCHIFiles(t, port.RenderCHIRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		Hosts: []domain.HostIdentity{
			{Name: "0-0", Address: "writer-0.ch-int-dev.clickhouse.internal"},
		},
	})

	remote := files["chop-generated-remote_servers.xml"]
	require.Contains(t, remote, "<host>writer-0.ch-int-dev.clickhouse.internal</host>",
		"remote_servers host entries must resolve to the cloud address")
	require.NotContains(t, remote, "chi-ch-int-dev-logexplorer-0-0",
		"no pod-pattern names may leak into remote_servers")

	ports := files["chop-generated-hostname-ports.xml"]
	require.Contains(t, ports,
		"<interserver_http_host>writer-0.ch-int-dev.clickhouse.internal</interserver_http_host>",
		"interserver_http_host must resolve to the cloud address")
}

// TestConfigRendererReplicaIdentity asserts the CRITICAL invariant: with
// a -replica mapping the macros <replica> is exactly the cloud short
// hostname — Keeper replica paths and per-replica S3 prefixes on
// existing clusters encode this identity.
func TestConfigRendererReplicaIdentity(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	files := renderCHIFiles(t, port.RenderCHIRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		Hosts: []domain.HostIdentity{
			{Name: "0-0", Address: "writer-0.ch-int-dev.clickhouse.internal"},
		},
		Replicas: map[string]string{"0-0": "writer-0"},
	})

	macros := files["chop-generated-macros.xml"]
	require.Contains(t, macros, "<replica>writer-0</replica>",
		"macros replica must be the mapped cloud short hostname")
	require.NotContains(t, macros, "chi-ch-int-dev-logexplorer-0-0",
		"the operator pod pattern must never leak into macros")
}

// TestConfigRendererNoInventoryUnchanged asserts backwards compatibility:
// without a mapping the render keeps the operator-native output
// byte-for-byte (macros replica and remote_servers host keep the pod
// pattern — the pre-inventory behavior).
func TestConfigRendererNoInventoryUnchanged(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	files := renderCHIFiles(t, port.RenderCHIRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
	})

	macros := files["chop-generated-macros.xml"]
	require.Contains(t, macros, "<replica>chi-ch-int-dev-logexplorer-0-0</replica>",
		"no mapping: operator-native replica identity must be kept")
	remote := files["chop-generated-remote_servers.xml"]
	require.Contains(t, remote, "<host>chi-ch-int-dev-logexplorer-0-0</host>",
		"no inventory: operator-native host names must be kept")
}

func TestConfigRendererRenderCHK(t *testing.T) {
	chkSpec := []byte(`
apiVersion: clickhouse-keeper.altinity.com/v1
kind: ClickHouseKeeperInstallation
metadata:
  name: keeper-dev
spec:
  replicas: 1
  configuration:
    settings:
      keeper_server/tcp_port: "9181"
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHK(context.Background(), port.LoadCHKRequest{
		Spec: domain.SpecReference{Data: chkSpec},
	})
	require.NoError(t, err)

	renderer := NewConfigRenderer()
	rendered, err := renderer.RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR: crAny,
		OutputDir:    t.TempDir(),
		DryRun:       true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, rendered)
}

func TestCloudNamerTranslatesFQDNs(t *testing.T) {
	// The FQDN form must translate to cloud addresses; other forms pass
	// through operator-native (replica naming must never be cloud-mangled).
	n := newCloudNamer(map[string]string{
		"0-0": "writer-0.ch-int-dev.clickhouse.internal",
	})
	// Pass a host-shaped param; translate only reacts to *v1.Host.
	fqdn := n.translate("chi-ch-int-dev-ch-int-dev-0-0.default.svc.cluster.local", nil)
	require.Equal(t, "chi-ch-int-dev-ch-int-dev-0-0.default.svc.cluster.local", fqdn, "no host param: pass-through")
}

// fakeSecretsResolver stubs the secrets resolver for users tests.
type fakeSecretsResolver struct {
	values map[string]string
}

func (f fakeSecretsResolver) ResolveSecret(ctx context.Context, request port.SecretRequest) (string, error) {
	if v, ok := f.values[request.Name]; ok {
		return v, nil
	}
	return "", fmt.Errorf("secret %q not found", request.Name)
}

func TestUsersRendererRendersHashedPasswords(t *testing.T) {
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: users-test
spec:
  configuration:
    users:
      admin/passwordFrom: "test-admin-password"
      admin/profile: default
      admin/access_management: "1"
      writer/passwordFrom: "test-writer-password"
      writer/profile: ingest
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	renderer := NewUsersRenderer(fakeSecretsResolver{values: map[string]string{
		"test-admin-password":  "admin-secret",
		"test-writer-password": "writer-secret",
	}})
	rendered, err := renderer.RenderUsers(context.Background(), port.RenderUsersRequest{
		NormalizedCR: crAny,
		OutputDir:    t.TempDir(),
		Networks:     map[string][]string{"admin": {"10.0.1.0/24"}},
	})
	require.NoError(t, err)
	require.Len(t, rendered, 1)

	xml := rendered[0].Content
	// Hashes, never plaintext
	require.NotContains(t, xml, "admin-secret")
	require.NotContains(t, xml, "writer-secret")
	require.Contains(t, xml, "password_sha256_hex")
	// Hash correctness
	expected := sha256.Sum256([]byte("admin-secret"))
	require.Contains(t, xml, hex.EncodeToString(expected[:]))
	// Networks: cloud CIDR + localhost appended
	require.Contains(t, xml, "10.0.1.0/24")
	require.Contains(t, xml, "127.0.0.1")
	// Other user fields pass through
	require.Contains(t, xml, "access_management")
	require.Contains(t, xml, "ingest")
}

func TestUsersRendererFailsOnMissingSecret(t *testing.T) {
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: users-missing
spec:
  configuration:
    users:
      admin/passwordFrom: "no-such-secret"
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	_, err = NewUsersRenderer(fakeSecretsResolver{}).RenderUsers(context.Background(), port.RenderUsersRequest{
		NormalizedCR: crAny,
		OutputDir:    t.TempDir(),
	})
	require.ErrorContains(t, err, "no-such-secret")
}

func TestUsersRendererEmitsWellFormedXML(t *testing.T) {
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: xml-shape
spec:
  configuration:
    users:
      admin/passwordFrom: "x-secret"
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)
	rendered, err := NewUsersRenderer(fakeSecretsResolver{values: map[string]string{"x-secret": "v"}}).RenderUsers(
		context.Background(), port.RenderUsersRequest{NormalizedCR: crAny, OutputDir: t.TempDir()})
	require.NoError(t, err)

	content := rendered[0].Content
	// Exactly one root open/close, users wrapper present.
	require.Equal(t, 1, strings.Count(content, "<clickhouse>"))
	require.Equal(t, 1, strings.Count(content, "</clickhouse>"))
	require.Contains(t, content, "<users>")
	// Well-formedness: a real XML parse (encoding/xml) — the exact class
	// of failure the live cluster hit (SAXParseException: Tag mismatch
	// from the double-wrapped root).
	decoder := xml.NewDecoder(strings.NewReader(content))
	for {
		if _, err := decoder.Token(); err == io.EOF {
			break
		} else {
			require.NoError(t, err, "malformed users XML: %s", content)
		}
	}
}

func TestRenderCHKCloudRaftEntries(t *testing.T) {
	// The keeper generator's raft entries must carry the CLOUD DNS names
	// when KeeperAddresses is set — the default namer renders the k8s
	// service pattern, unresolvable off-cluster.
	spec := []byte(`
apiVersion: clickhouse-keeper.altinity.com/v1
kind: ClickHouseKeeperInstallation
metadata:
  name: keeper-cloud
spec:
  replicas: 1
  configuration:
    settings:
      keeper_server/tcp_port: "9181"
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHK(context.Background(), port.LoadCHKRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	renderer := NewConfigRenderer()
	rendered, err := renderer.RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR: crAny,
		OutputDir:    t.TempDir(),
		DryRun:       true,
		KeeperAddresses: map[string]string{
			"0-0": "keeper-0.ch-int-dev.clickhouse.internal",
		},
	})
	require.NoError(t, err)

	var raft string
	for _, f := range rendered {
		if strings.Contains(f.Path, "raft") {
			raft += f.Content
		}
	}
	require.Contains(t, raft, "keeper-0.ch-int-dev.clickhouse.internal",
		"raft entries must use the cloud DNS name, not the k8s service pattern")
	require.NotContains(t, raft, "svc.cluster.local")
	// Well-formed XML (the users.xml live finding class).
	decoder := xml.NewDecoder(strings.NewReader(raft))
	for {
		if _, err := decoder.Token(); err == io.EOF {
			break
		} else {
			require.NoError(t, err, "malformed raft XML: %s", raft)
		}
	}
}

func TestRenderCHKHostInventoryRaftEntries(t *testing.T) {
	// The same raft translation must flow from the shared -host inventory
	// (the flag schema/replica commands use) — keepers join the cluster
	// inventory rather than carrying a keeper-only address list.
	spec := []byte(`
apiVersion: clickhouse-keeper.altinity.com/v1
kind: ClickHouseKeeperInstallation
metadata:
  name: keeper-inventory
spec:
  replicas: 1
  configuration:
    settings:
      keeper_server/tcp_port: "9181"
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHK(context.Background(), port.LoadCHKRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	rendered, err := NewConfigRenderer().RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR: crAny,
		OutputDir:    t.TempDir(),
		DryRun:       true,
		Hosts:        []domain.HostIdentity{{Name: "0-0", Address: "keeper-0.aws.internal"}},
	})
	require.NoError(t, err)

	var raft string
	for _, f := range rendered {
		if strings.Contains(f.Path, "raft") {
			raft += f.Content
		}
	}
	require.Contains(t, raft, "<hostname>keeper-0.aws.internal</hostname>",
		"-host inventory must resolve raft hostnames")
}

func TestSchemaRunnerHostResolvesCloudName(t *testing.T) {
	// The real stack's inventory keys are CR ordinals (0-2) with cloud
	// DNS addresses (reader-3.aws.internal); the GC detects system.replicas
	// host_name values (reader-3). The first FQDN label must resolve the
	// cloud name onto the CR host — otherwise the drop path can never
	// address a stale replica.
	cr := loadCHIFromFile(t, "ch-int-dev", "")
	runnerAny, err := NewSchemaClientFactory().NewClient(context.Background(), port.NewSchemaClientRequest{
		ClusterName:  "logexplorer",
		Credentials:  domain.Credentials{Username: "default"},
		Hosts:        []domain.HostIdentity{{Name: "0-0", Address: "writer-0.example.internal"}},
		NormalizedCR: cr,
	})
	require.NoError(t, err)
	runner := runnerAny.(*SchemaRunner)

	host, err := runner.host("writer-0")
	require.NoError(t, err, "cloud name must resolve via the FQDN first-label fallback")
	require.Equal(t, "writer-0.example.internal", host.Runtime.Address.FQDN)

	_, err = runner.host("reader-9")
	require.ErrorContains(t, err, "not in inventory")
}

func TestSchemaRunnerDropReplicaUnknownHost(t *testing.T) {
	// The AWS stack's GC detects cloud host_name values (reader-9);
	// DropReplica must refuse a name the inventory does not know before
	// any SQL runs — the gate that keeps a GC from dropping arbitrary
	// registrations.
	runner := newGCRunner(t)
	err := runner.DropReplica(context.Background(), domain.ReplicaDropRequest{
		HostName: "reader-9",
		RunOn:    "writer-0",
	})
	require.ErrorContains(t, err, "not in inventory")
}
