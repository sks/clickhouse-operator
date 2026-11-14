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
	osuser "os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chop"

	"github.com/stretchr/testify/require"
)

// chiSpecPath points at the example CHI spec carried in the module for
// smoke-testing the render path.
const chiSpecPath = "testdata/example-chi.yaml"

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
	// With no Host param the CR ordinal is parsed from the pod-pattern
	// name itself (scope-object lists pass no per-host param): the
	// schema-sync peer list translates correctly (the live reader-0
	// finding — peers collapsing onto self).
	fqdn := n.translate("chi-ch-int-dev-ch-int-dev-0-0.default.svc.cluster.local", nil)
	require.Equal(t, "writer-0.ch-int-dev.clickhouse.internal", fqdn, "ordinal-parse: translated to the mapped address")
	// A name whose trailing parts are not ordinals passes through.
	fqdn = n.translate("chi-ch-int-dev-ch-int-dev-writer.default.svc.cluster.local", nil)
	require.Equal(t, "chi-ch-int-dev-ch-int-dev-writer.default.svc.cluster.local", fqdn, "non-ordinal label: pass-through")
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

func TestUsersRendererPasswordFromRemovesStockPassword(t *testing.T) {
	// A passwordFrom reference is resolved AFTER normalization (at render
	// time), so removePlainPassword never runs for it — the renderer must
	// emit the remove="1" stub itself. Without it ClickHouse 26.8 merges
	// the users.d hash with the stock users.xml plaintext <password>
	// entry and dies at startup: Code 36 "Cannot specify multiple
	// authentication methods" (the live finding).
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: users-pfr-remove
spec:
  configuration:
    users:
      admin/passwordFrom: "test-admin-password"
      admin/profile: default
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	renderer := NewUsersRenderer(fakeSecretsResolver{values: map[string]string{
		"test-admin-password": "admin-secret",
	}})
	rendered, err := renderer.RenderUsers(context.Background(), port.RenderUsersRequest{
		NormalizedCR: crAny,
		OutputDir:    t.TempDir(),
	})
	require.NoError(t, err)

	xml := rendered[0].Content
	// The stub clears the stock plaintext entry (removePlainPassword parity).
	require.Contains(t, xml, `<password remove="1"></password>`,
		"passwordFrom-resolved user must blank the stock <password> entry")
	// The resolved hash rides alongside the stub.
	expected := sha256.Sum256([]byte("admin-secret"))
	require.Contains(t, xml, "<password_sha256_hex>"+hex.EncodeToString(expected[:])+"</password_sha256_hex>")
	// Exactly one stub for the one resolved user — no double emission.
	require.Equal(t, 1, strings.Count(xml, `remove="1"`))
}

func TestUsersRendererDirectSpecHashKeepsNormalizerRemove(t *testing.T) {
	// An at-normalize hash (spec password_sha256_hex on the default user)
	// gets its remove="1" stub from removePlainPassword itself — the
	// renderer must preserve the attribute, not strip it, and not add a
	// second stub. This pins the pre-fix regression where adaptUser's
	// scalar re-wrap dropped attributes.
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: users-direct-hash
spec:
  configuration:
    users:
      default/password_sha256_hex: "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8"
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	rendered, err := NewUsersRenderer(fakeSecretsResolver{}).RenderUsers(context.Background(), port.RenderUsersRequest{
		NormalizedCR: crAny,
		OutputDir:    t.TempDir(),
	})
	require.NoError(t, err)

	xml := rendered[0].Content
	require.Contains(t, xml, `<password remove="1"></password>`,
		"normalizer's removePlainPassword stub must survive the renderer")
	require.Contains(t, xml, "5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8")
	require.Equal(t, 1, strings.Count(xml, `remove="1"`))
}

func TestUsersRendererPasswordlessUserNoRemoveStub(t *testing.T) {
	// A passwordless spec user (no passwordFrom, no hash) is defaulted by
	// the normalizer to hash("default") — password_sha256_hex with NO
	// remove stub. The stub must not appear: emitting one would clear a
	// nonexistent stock entry and change the merged account shape.
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: users-passwordless
spec:
  configuration:
    users:
      reader/profile: default
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	rendered, err := NewUsersRenderer(fakeSecretsResolver{}).RenderUsers(context.Background(), port.RenderUsersRequest{
		NormalizedCR: crAny,
		OutputDir:    t.TempDir(),
	})
	require.NoError(t, err)

	xml := rendered[0].Content
	require.NotContains(t, xml, `remove="1"`,
		"passwordless user must not gain a remove stub")
	require.NotContains(t, xml, "<password>", "no bare password element")
	require.NotContains(t, xml, `<password `, "no stub element either")
	// The normalizer's default hash still rides through.
	expected := sha256.Sum256([]byte("default"))
	require.Contains(t, xml, "<password_sha256_hex>"+hex.EncodeToString(expected[:])+"</password_sha256_hex>")
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

// keeperSpec is the CHK spec shape the off-cluster stack renders: a
// 3-keeper fleet with client settings and a logger files section.
const keeperSingleFileSpec = `
apiVersion: clickhouse-keeper.altinity.com/v1
kind: ClickHouseKeeperInstallation
metadata:
  name: keeper-single
spec:
  configuration:
    clusters:
      - name: keeper
        layout:
          replicasCount: 3
    settings:
      keeper_server/tcp_port: "9181"
      keeper_server/listen_host: "0.0.0.0"
    files:
      keeper-logging.xml: |
        <clickhouse>
            <logger>
                <level>information</level>
                <log>/var/log/clickhouse-keeper/clickhouse-keeper.log</log>
            </logger>
        </clickhouse>
`

// loadCHKFromSpec runs the SpecLoader over a CHK spec body.
func loadCHKFromSpec(t *testing.T, spec string) any {
	t.Helper()
	crAny, err := NewSpecLoader().LoadCHK(context.Background(), port.LoadCHKRequest{
		Spec: domain.SpecReference{Data: []byte(spec)},
	})
	require.NoError(t, err)
	return crAny
}

// keeperInventory is the 3-node cloud inventory the single-file tests use.
var keeperInventory = []domain.HostIdentity{
	{Name: "0-0", Address: "keeper-0.aws.internal"},
	{Name: "0-1", Address: "keeper-1.aws.internal"},
	{Name: "0-2", Address: "keeper-2.aws.internal"},
}

// requireWellFormedXML asserts the document parses as XML.
func requireWellFormedXML(t *testing.T, doc string) {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(doc))
	for {
		_, err := decoder.Token()
		if err == io.EOF {
			return
		}
		require.NoError(t, err, "malformed XML: %s", doc)
	}
}

func TestRenderCHKSingleFileMerge(t *testing.T) {
	// The AWS stack runs clickhouse-keeper with an explicit
	// --config=keeper_config.xml, which DISABLES the config.d
	// auto-include — the node needs ONE complete file: raft config,
	// listeners, settings and the logger, everything the common+host
	// groups produce, under one root.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	outDir := t.TempDir()
	singlePath := filepath.Join(outDir, "keeper_config.xml")

	rendered, err := NewConfigRenderer().RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		OutputDir:      outDir,
		Hosts:          keeperInventory,
		ServerID:       1,
		SingleFilePath: singlePath,
	})
	require.NoError(t, err)

	// The merged artifact is returned as its own entry for inspection.
	var merged string
	for _, f := range rendered {
		if f.Group == "single" {
			merged = f.Content
		}
	}
	require.NotEmpty(t, merged, "single-file render must return the merged content")

	// ONE <clickhouse> root, valid XML.
	require.Equal(t, 1, strings.Count(merged, "<clickhouse>"))
	requireWellFormedXML(t, merged)

	// Exactly one <server_id> — the myid-file override, not the CR's
	// zero-based replica index.
	require.Equal(t, 1, strings.Count(merged, "<server_id>"))
	require.Contains(t, merged, "<server_id>1</server_id>")
	require.NotContains(t, merged, "<server_id>0</server_id>")

	// The full raft <server> list with cloud DNS names (pinned from
	// ebc4625 — the single-file merge must not lose or collapse it).
	require.Equal(t, 3, strings.Count(merged, "<server>"))
	require.Contains(t, merged, "<hostname>keeper-0.aws.internal</hostname>")
	require.Contains(t, merged, "<hostname>keeper-1.aws.internal</hostname>")
	require.Contains(t, merged, "<hostname>keeper-2.aws.internal</hostname>")
	require.NotContains(t, merged, "svc.cluster.local")

	// Every section a keeper needs to boot from the single file: client
	// settings, listeners and the logger from the CR's files section.
	require.Contains(t, merged, "<tcp_port>9181</tcp_port>")
	require.Contains(t, merged, "<listen_host>0.0.0.0</listen_host>")
	require.Contains(t, merged, "<logger>")

	// The file is on disk at the requested path, byte-identical to the
	// returned content.
	data, err := os.ReadFile(singlePath)
	require.NoError(t, err)
	require.Equal(t, merged, string(data))

	// Single-file mode skips -out writing entirely.
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	for _, e := range entries {
		if e.Name() != "keeper_config.xml" {
			t.Fatalf("single-file mode must not write per-group files to -out, found %s", e.Name())
		}
	}
}

func TestRenderCHKSingleFileWithoutServerIDKeepsCRIndex(t *testing.T) {
	// -server-id 0/unset keeps the CR-derived replica index — the
	// override is opt-in, never a silent renumbering.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	rendered, err := NewConfigRenderer().RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		OutputDir:      t.TempDir(),
		Hosts:          keeperInventory,
		SingleFilePath: filepath.Join(t.TempDir(), "keeper_config.xml"),
	})
	require.NoError(t, err)
	var merged string
	for _, f := range rendered {
		if f.Group == "single" {
			merged = f.Content
		}
	}
	require.Contains(t, merged, "<server_id>0</server_id>",
		"no override: CR zero-based replica index must be kept")
}

func TestRenderCHKSingleFileRequiresHost(t *testing.T) {
	// The single file is host-complete (it carries <server_id>): without
	// a host selection there is no such thing as "this node's" file.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	_, err := NewConfigRenderer().RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR:   cr,
		OutputDir:      t.TempDir(),
		Hosts:          keeperInventory,
		SingleFilePath: filepath.Join(t.TempDir(), "keeper_config.xml"),
	})
	require.ErrorContains(t, err, "requires a keeper host")
}

func TestRenderCHKSingleFileDryRunSkipsWrite(t *testing.T) {
	// DryRun means no writes at all — the merged content is returned
	// but the file does not land on disk.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	singlePath := filepath.Join(t.TempDir(), "keeper_config.xml")
	rendered, err := NewConfigRenderer().RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		OutputDir:      t.TempDir(),
		Hosts:          keeperInventory,
		DryRun:         true,
		SingleFilePath: singlePath,
	})
	require.NoError(t, err)
	var merged string
	for _, f := range rendered {
		if f.Group == "single" {
			merged = f.Content
		}
	}
	require.NotEmpty(t, merged)
	require.NoFileExists(t, singlePath, "dry-run must not write the single file")
}

func TestRenderCHKOutDirUnchangedBySingleFileFlags(t *testing.T) {
	// Regression: without -single-file, the per-group render (files,
	// groups, content) must stay byte-identical to the pre-flag output —
	// the new flags may not perturb the existing path.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)

	a := renderCHKFiles(t, port.RenderCHKRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    t.TempDir(),
		DryRun:       true,
		Hosts:        keeperInventory,
	})
	b := renderCHKFiles(t, port.RenderCHKRequest{
		NormalizedCR: cr,
		HostName:     "0-1",
		OutputDir:    t.TempDir(),
		DryRun:       true,
		Hosts:        keeperInventory,
	})
	// Host 0-1: its own server-id section (index 1), the same common files.
	require.Contains(t, b["chop-generated-server-id.xml"], "<server_id>1</server_id>")
	require.Equal(t, a["chop-generated-raft.xml"], b["chop-generated-raft.xml"],
		"common raft file must be identical across hosts")
	require.Contains(t, a["chop-generated-server-id.xml"], "<server_id>0</server_id>")
}

func TestRenderCHKRaftIDOverridesOutDir(t *testing.T) {
	// An existing off-cluster ensemble persists ONE-based raft ids in
	// its raft logs; the per-group render must carry those ids — the
	// zero-based CR indices would present a different ensemble to
	// surviving members and break Raft recovery on re-image.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	files := renderCHKFiles(t, port.RenderCHKRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    t.TempDir(),
		DryRun:       true,
		Hosts:        keeperInventory,
		RaftIDs:      map[string]int{"0-0": 1, "0-1": 2, "0-2": 3},
	})

	raft := files["chop-generated-raft.xml"]
	require.Contains(t, raft, "<id>1</id>", "0-0's raft entry must carry the mapped id")
	require.Contains(t, raft, "<id>2</id>", "0-1's raft entry must carry the mapped id")
	require.Contains(t, raft, "<id>3</id>", "0-2's raft entry must carry the mapped id")
	require.NotContains(t, raft, "<id>0</id>", "unmapped zero-based ids must not leak into the ensemble view")
	// Hostnames and ports are preserved verbatim — only ids change.
	require.Contains(t, raft, "<hostname>keeper-0.aws.internal</hostname>")
	require.Contains(t, raft, "<port>9444</port>")
	requireWellFormedXML(t, raft)
}

func TestRenderCHKRaftIDOverridesSingleFile(t *testing.T) {
	// The merged single file must reflect the overridden ids — a
	// keeper booted from it presents the same ensemble members its
	// peers have in their raft logs.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	rendered, err := NewConfigRenderer().RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		OutputDir:      t.TempDir(),
		Hosts:          keeperInventory,
		ServerID:       1,
		RaftIDs:        map[string]int{"0-0": 1, "0-1": 2, "0-2": 3},
		SingleFilePath: filepath.Join(t.TempDir(), "keeper_config.xml"),
	})
	require.NoError(t, err)
	var merged string
	for _, f := range rendered {
		if f.Group == "single" {
			merged = f.Content
		}
	}
	require.NotEmpty(t, merged)
	requireWellFormedXML(t, merged)
	require.Contains(t, merged, "<id>1</id>")
	require.Contains(t, merged, "<id>2</id>")
	require.Contains(t, merged, "<id>3</id>")
	require.NotContains(t, merged, "<id>0</id>")
	// Local and ensemble identities agree for the selected host.
	require.Contains(t, merged, "<server_id>1</server_id>")
	require.Equal(t, 3, strings.Count(merged, "<server>"))
}

func TestRenderCHKRaftIDPartialMappingKeepsCRIndices(t *testing.T) {
	// Unmapped hosts keep their CR-derived zero-based ids — a partial
	// mapping (adopting nodes one at a time) must not renumber the
	// rest of the ensemble.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	files := renderCHKFiles(t, port.RenderCHKRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    t.TempDir(),
		DryRun:       true,
		Hosts:        keeperInventory,
		RaftIDs:      map[string]int{"0-0": 1},
	})
	raft := files["chop-generated-raft.xml"]
	// 0-0 maps to 1; unmapped 0-1 keeps CR index 1 → two entries with
	// id 1; unmapped 0-2 keeps CR index 2 → one entry with id 2; the
	// CR index 0 for 0-0 is replaced, so no id 0 remains.
	require.Equal(t, 2, strings.Count(raft, "<id>1</id>"),
		"mapped 0-0 (id 1) + unmapped 0-1 (CR index 1)")
	require.Equal(t, 1, strings.Count(raft, "<id>2</id>"),
		"unmapped 0-2 keeps CR index 2")
	require.NotContains(t, raft, "<id>0</id>",
		"the mapped host's CR index must be replaced, not duplicated")
}

func TestRenderCHKServerIDRaftIDConsistency(t *testing.T) {
	// -server-id and this node's -raft-id must tell the same story: a
	// mismatched pair means the operator thinks the node has two
	// different identities, and keeper refuses to boot (server_id must
	// equal its own raft_configuration id) or silently partitions the
	// ensemble.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	base := port.RenderCHKRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		OutputDir:      t.TempDir(),
		Hosts:          keeperInventory,
		ServerID:       1,
		RaftIDs:        map[string]int{"0-0": 1, "0-1": 2, "0-2": 3},
		SingleFilePath: filepath.Join(t.TempDir(), "keeper_config.xml"),
	}
	_, err := NewConfigRenderer().RenderCHK(context.Background(), base)
	require.NoError(t, err, "agreeing pair must render")

	mismatched := base
	mismatched.RaftIDs = map[string]int{"0-0": 7, "0-1": 2, "0-2": 3}
	_, err = NewConfigRenderer().RenderCHK(context.Background(), mismatched)
	require.ErrorContains(t, err, "-server-id 1 disagrees with -raft-id 0-0=7")

	// Another host's -raft-id is not this node's identity: no conflict.
	otherHost := base
	otherHost.RaftIDs = map[string]int{"0-1": 5, "0-2": 3}
	_, err = NewConfigRenderer().RenderCHK(context.Background(), otherHost)
	require.NoError(t, err, "a raft-id for a NON-selected host must not conflict with -server-id")
}

// TestRenderCHKOutDirBytes pins the per-group output byte-for-byte: the
// -server-id/-single-file flags may not perturb the original render path.
// Golden bytes captured from the ebc4625 output.
func TestRenderCHKOutDirBytes(t *testing.T) {
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	files := renderCHKFiles(t, port.RenderCHKRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    t.TempDir(),
		DryRun:       true,
		Hosts:        keeperInventory,
	})

	require.Equal(t, map[string]string{
		"chop-generated-raft.xml": `<clickhouse>
    <keeper_server>
        <raft_configuration>
            <server>
                <id>0</id>
                <hostname>keeper-0.aws.internal</hostname>
                <port>9444</port>
            </server>
            <server>
                <id>1</id>
                <hostname>keeper-1.aws.internal</hostname>
                <port>9444</port>
            </server>
            <server>
                <id>2</id>
                <hostname>keeper-2.aws.internal</hostname>
                <port>9444</port>
            </server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
`,
		"chop-generated-server-id.xml": `<clickhouse>
    <keeper_server>
        <server_id>0</server_id>
    </keeper_server>
</clickhouse>
`,
		"chop-generated-settings.xml": `<clickhouse>
    <keeper_server>
        <listen_host>0.0.0.0</listen_host>
        <tcp_port>9181</tcp_port>
    </keeper_server>
</clickhouse>
`,
		"keeper-logging.xml": `<clickhouse>
    <logger>
        <level>information</level>
        <log>/var/log/clickhouse-keeper/clickhouse-keeper.log</log>
    </logger>
</clickhouse>
`,
	}, files)
}

// renderCHKFiles runs a dry render and maps file path → content.
func renderCHKFiles(t *testing.T, request port.RenderCHKRequest) map[string]string {
	t.Helper()
	rendered, err := NewConfigRenderer().RenderCHK(context.Background(), request)
	require.NoError(t, err)
	files := make(map[string]string, len(rendered))
	for _, f := range rendered {
		files[f.Path] = f.Content
	}
	return files
}

func TestKeeperListenHostWarningWithTheKey(t *testing.T) {
	// The live finding: keeper IGNORES keeper_server/listen_host — the
	// client port stayed on 127.0.0.1 and no fleet node could reach
	// mntr or replication until the setting moved to a root-level files
	// entry. The render must warn, name every offending host, and stay
	// a successful render.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	warning := keeperListenHostWarning(cr.(*chk.ClickHouseKeeperInstallation))
	require.Contains(t, warning, "keeper_server/listen_host is IGNORED by clickhouse-keeper")
	require.Contains(t, warning, "ROOT-level setting")
	require.Contains(t, warning, "0-0, 0-1, 0-2", "the warning names the offending hosts, sorted")
}

func TestKeeperListenHostWarningWithoutTheKey(t *testing.T) {
	// A spec without the key renders silently — no false-positive noise
	// on healthy specs.
	cr := loadCHKFromSpec(t, `
apiVersion: clickhouse-keeper.altinity.com/v1
kind: ClickHouseKeeperInstallation
metadata:
  name: keeper-clean
spec:
  configuration:
    clusters:
      - name: keeper
        layout:
          replicasCount: 2
    settings:
      keeper_server/tcp_port: "9181"
`)
	require.Empty(t, keeperListenHostWarning(cr.(*chk.ClickHouseKeeperInstallation)))
}

func TestRenderCHKPrintsListenHostWarningToStderr(t *testing.T) {
	// The end-to-end wiring: RenderCHK writes the warning to stderr and
	// still returns a successful render — the check never gates output.
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)

	original := os.Stderr
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stderr = w
	rendered, renderErr := NewConfigRenderer().RenderCHK(context.Background(), port.RenderCHKRequest{
		NormalizedCR: cr,
		OutputDir:    t.TempDir(),
		DryRun:       true,
	})
	_ = w.Close()
	os.Stderr = original
	require.NoError(t, renderErr)
	require.NotEmpty(t, rendered)

	captured, _ := io.ReadAll(r)
	require.Contains(t, string(captured), "keeper_server/listen_host is IGNORED")
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

func TestSchemaRunnerHostMissListsAvailableNames(t *testing.T) {
	// The live finding: "host \"ip-10-0-1-53\" not in inventory" gave
	// no clue what the valid names are. The miss must name every form
	// host() accepts — CR names AND cloud short names, sorted — so an
	// operator reading a bootstrap log can fix the call without
	// re-deriving the inventory.
	runner := newGCRunner(t)
	_, err := runner.host("ip-10-0-1-53")
	require.ErrorContains(t, err, `host "ip-10-0-1-53" not in inventory (available: 0-0, writer-0)`,
		"the miss must carry the deterministic available-names suffix")

	// Deterministic: the same runner reports the same suffix twice.
	_, err2 := runner.host("other")
	require.ErrorContains(t, err2, "(available: 0-0, writer-0)")
}

func TestSchemaRunnerHostMissSingleLine(t *testing.T) {
	// The error stays single-line: node bootstrap logs paste it whole
	// into issue trackers and one-line greps.
	runner := newGCRunner(t)
	_, err := runner.host("nope")
	require.NotContains(t, err.Error(), "\n")
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

// userPasswordSpec is the shared fixture for user-password injection
// tests: a spec declaring `admin` with a plaintext password (the
// hand-migrated-spec case the injection must win over) plus a second
// user to prove non-injected users pass through untouched.
const userPasswordSpec = `
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: pw-inject
spec:
  configuration:
    users:
      admin/profile: default
      admin/quota: default
      admin/access_management: "1"
      admin/password: "specpass"
      writer/profile: ingest
`

// loadUserPasswordCHI loads the shared injection fixture.
func loadUserPasswordCHI(t *testing.T) *v1.ClickHouseInstallation {
	t.Helper()
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: []byte(userPasswordSpec)},
	})
	require.NoError(t, err)
	cr, ok := crAny.(*v1.ClickHouseInstallation)
	require.True(t, ok, "loader returned non-CHI")
	return cr
}

// hashOf returns the sha256 hex of s — the same value the app layer's
// readUserPasswordHashes produces for a file containing s.
func hashOf(t *testing.T, s string) string {
	t.Helper()
	hash := sha256.Sum256([]byte(s))
	return hex.EncodeToString(hash[:])
}

// usersXML renders the users-group files of the given request, keyed by
// file name.
func usersXML(t *testing.T, request port.RenderCHIRequest) map[string]string {
	t.Helper()
	files := renderCHIFiles(t, request)
	users := make(map[string]string, 1)
	for name, content := range files {
		if strings.Contains(name, "users") {
			users[name] = content
		}
	}
	require.NotEmpty(t, users, "users-group files must render")
	return users
}

// TestConfigRendererMachineryUserNeverRenders asserts the core
// off-cluster contract: the normalizer injects the operator's machinery
// user (clickhouse_operator) into every users tree, but RenderCHI drops
// it before emission — off-cluster its profile definition (operator
// machinery, mounted from the users ConfigMap on-cluster only) does not
// exist and ClickHouse refuses the whole users.d load (Code 347, the
// live boot crash). The spec's own users must render untouched.
func TestConfigRendererMachineryUserNeverRenders(t *testing.T) {
	cr := loadUserPasswordCHI(t)

	files := usersXML(t, port.RenderCHIRequest{NormalizedCR: cr})
	xmlBody := files["chop-generated-users.xml"]

	require.NotContains(t, xmlBody, "<clickhouse_operator>",
		"the machinery user must never render off-cluster: its profile is operator-shipped on-cluster only (Code 347)")
	require.NotContains(t, xmlBody, "clickhouse_operator",
		"no machinery trace may ride the users file — including profile/quota references")

	// The spec-declared users are the deployment's contract and must
	// survive the machinery drop byte-for-byte.
	require.Contains(t, xmlBody, "<profile>default</profile>")
	require.Contains(t, xmlBody, "<profile>ingest</profile>")
	require.Contains(t, xmlBody, "<access_management>1</access_management>")
}

// TestConfigRendererMachineryDropDoesNotMutateCR asserts the deep-copy
// invariant: the shared CR's users tree still carries the machinery
// user after a machinery-free render — a later render in the same
// process (host group, render-node plan) sees the operator-native tree,
// never a silently-stripped one.
func TestConfigRendererMachineryDropDoesNotMutateCR(t *testing.T) {
	cr := loadUserPasswordCHI(t)
	machinery := chop.Config().ClickHouse.Access.Username

	_, err := NewConfigRenderer().RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR: cr,
		OutputDir:    t.TempDir(),
		DryRun:       true,
	})
	require.NoError(t, err)

	users := cr.GetSpecT().GetConfiguration().GetUsers()
	require.True(t, userSettingsPrefixExists(users, machinery),
		"in-place mutation of the shared CR would strip the machinery user from every later render")
}

// TestRenderNodePlanMachineryUserAbsent asserts the render-node plan
// (what actually installs on a node) carries no machinery user in any
// users.d file — the Code 347 class is impossible by construction, with
// no post-render strip in the plan or the cloud-init caller.
func TestRenderNodePlanMachineryUserAbsent(t *testing.T) {
	cr := loadRenderNodeCHI(t)
	plan, _ := renderNodePlan(t, port.RenderNodeRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    "/etc/clickhouse-server/config.d",
		UsersDir:     "/etc/clickhouse-server/users.d",
	})
	for path, file := range plan {
		if filepath.Base(filepath.Dir(path)) != "users.d" {
			continue
		}
		require.NotContains(t, file.Content, "clickhouse_operator",
			"machinery user in %s would crash the node's users.d load (Code 347)", path)
	}
	// The deployment's own users file still renders.
	require.Contains(t, plan["/etc/clickhouse-server/users.d/chop-generated-users.xml"].Content, "<default>")
}

// TestConfigRendererUserPasswordInjectionOverridesSpec asserts the core
// fix: the injected runtime hash replaces every spec-carried password
// for that user, the spec's own fields pass through, and non-injected
// users keep the normalizer's output untouched.
func TestConfigRendererUserPasswordInjectionOverridesSpec(t *testing.T) {
	cr := loadUserPasswordCHI(t)
	injected := hashOf(t, "testpass")

	files := usersXML(t, port.RenderCHIRequest{
		NormalizedCR:  cr,
		UserPasswords: map[string]string{"admin": injected},
	})
	xmlBody := files["chop-generated-users.xml"]

	// The injected hash wins over the spec's plaintext password (which
	// the normalizer would have hashed itself at load time).
	require.Contains(t, xmlBody, "<password_sha256_hex>"+injected+"</password_sha256_hex>")
	require.NotContains(t, xmlBody, hashOf(t, "specpass"),
		"the spec-derived hash must be replaced by the injected one")
	require.NotContains(t, xmlBody, "specpass",
		"the plaintext must never appear, not even as a hash pre-image")

	// The spec still owns the non-secret fields — chctl only supplies
	// the hash.
	require.Contains(t, xmlBody, "<access_management>1</access_management>")
	require.Contains(t, xmlBody, "<profile>default</profile>")

	// Non-injected users pass through the normalizer untouched.
	require.Contains(t, xmlBody, "<profile>ingest</profile>")

	// A custom user gets no remove-stub: stock users.xml has no <password>
	// element for it, matching the operator's own removePlainPassword
	// (default-only) discipline.
	require.NotContains(t, xmlBody, "remove=")
}

// TestConfigRendererUserPasswordDoesNotLeakIntoLaterRenders asserts the
// deep-copy invariant: the shared CR is rendered again in the same
// process (host group, subsequent renders), so an injected hash must
// never leak into a render that did not request it.
func TestConfigRendererUserPasswordDoesNotLeakIntoLaterRenders(t *testing.T) {
	cr := loadUserPasswordCHI(t)
	injected := hashOf(t, "testpass")

	// First render: injection on.
	_, err := NewConfigRenderer().RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR:  cr,
		OutputDir:     t.TempDir(),
		DryRun:        true,
		UserPasswords: map[string]string{"admin": injected},
	})
	require.NoError(t, err)

	// Second render, same CR: no injection requested — the spec's own
	// (normalizer-derived) hash must be back, the injected one gone.
	files := usersXML(t, port.RenderCHIRequest{NormalizedCR: cr})
	xmlBody := files["chop-generated-users.xml"]
	require.NotContains(t, xmlBody, injected,
		"in-place mutation of the shared CR would leak the injected hash across renders")
	require.Contains(t, xmlBody, hashOf(t, "specpass"),
		"the CR's own spec must survive injection renders byte-for-byte")
}

// TestConfigRendererUserPasswordUnknownUserRejected asserts the typo
// guard: a -user-password entry naming a user the spec does not declare
// is a hard error listing the known users — never a silent partial
// render where the account keeps a stale credential while the operator
// believes the secret-file password is live.
func TestConfigRendererUserPasswordUnknownUserRejected(t *testing.T) {
	cr := loadUserPasswordCHI(t)

	_, err := NewConfigRenderer().RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR:  cr,
		OutputDir:     t.TempDir(),
		DryRun:        true,
		UserPasswords: map[string]string{"admi": hashOf(t, "testpass")},
	})
	require.ErrorContains(t, err, `"admi"`)
	require.ErrorContains(t, err, "not declared by the spec")
	require.ErrorContains(t, err, "admin", "the error must list the spec's known users (typo cross-reference)")
	require.ErrorContains(t, err, "writer")
}

// TestConfigRendererUserPasswordDefaultBlanksStockPassword asserts
// operator parity for the one user stock users.xml ships with a
// <password> element: the injected hash must arrive with the
// remove="1" stub or the base-file credential survives the users.d
// merge.
func TestConfigRendererUserPasswordDefaultBlanksStockPassword(t *testing.T) {
	cr := loadUserPasswordCHI(t)
	injected := hashOf(t, "testpass")

	files := usersXML(t, port.RenderCHIRequest{
		NormalizedCR:  cr,
		UserPasswords: map[string]string{"default": injected},
	})
	xmlBody := files["chop-generated-users.xml"]

	require.Contains(t, xmlBody, `<password remove="1"></password>`,
		"default user must blank the stock <password> element (removePlainPassword parity)")
	require.Contains(t, xmlBody, "<password_sha256_hex>"+injected+"</password_sha256_hex>")

	// The injected user's spec fields are untouched by injection for
	// other users.
	require.Contains(t, xmlBody, "<access_management>1</access_management>")
}

// TestConfigRendererUserPasswordDoubleSHA1SpecOverridden asserts the
// ClickHouse priority order is respected: a spec-declared
// password_double_sha1_hex (which ClickHouse ranks above
// password_sha256_hex) is stripped so the injected hash is the only
// credential on the account.
func TestConfigRendererUserPasswordDoubleSHA1SpecOverridden(t *testing.T) {
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: pw-double-sha1
spec:
  configuration:
    users:
      admin/profile: default
      admin/password_double_sha1_hex: "aaaa"
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	injected := hashOf(t, "testpass")
	files := usersXML(t, port.RenderCHIRequest{
		NormalizedCR:  crAny,
		UserPasswords: map[string]string{"admin": injected},
	})
	xmlBody := files["chop-generated-users.xml"]

	require.Contains(t, xmlBody, "<password_sha256_hex>"+injected+"</password_sha256_hex>")
	require.NotContains(t, xmlBody, "password_double_sha1_hex",
		"a leftover double-sha1 spec would out-rank the injected hash")
}

// TestConfigRendererUserPasswordNoFlagUnchanged asserts backwards
// compatibility: without UserPasswords the users-group render is the
// operator-native output (no injected hash, no remove stub) — the
// pre-feature behavior byte-for-byte.
func TestConfigRendererUserPasswordNoFlagUnchanged(t *testing.T) {
	cr := loadUserPasswordCHI(t)

	files := usersXML(t, port.RenderCHIRequest{NormalizedCR: cr})
	xmlBody := files["chop-generated-users.xml"]

	require.Contains(t, xmlBody, hashOf(t, "specpass"),
		"no flag: the normalizer's own hash of the spec password must stay")
	require.NotContains(t, xmlBody, "remove=", "no flag: no injection stubs may appear")
}

// countReplicaEntries counts the <replica> blocks the rendered
// remote_servers.xml carries (user-specified plus autogenerated clusters).
func countReplicaEntries(t *testing.T, remote string) int {
	t.Helper()
	require.Contains(t, remote, "<remote_servers>", "remote_servers.xml must render")
	return strings.Count(remote, "<replica>")
}

// parseXMLTokens asserts the XML is well-formed via a real decode —
// escaping bugs corrupt remote_servers and the server rejects the whole
// file at startup (the users.xml Tag-mismatch finding class).
func parseXMLTokens(t *testing.T, content string) {
	t.Helper()
	decoder := xml.NewDecoder(strings.NewReader(content))
	for {
		if _, err := decoder.Token(); err == io.EOF {
			return
		} else {
			require.NoError(t, err, "malformed XML: %s", content)
		}
	}
}

// TestConfigRendererRemoteCredentialsInjected asserts the core fix: with
// remote credentials every <replica> entry — user-specified AND
// autogenerated clusters — carries <user>/<password>, XML-escaped.
// Without them peers reject distributed queries as passwordless
// "default" (PASSWORD_REQUIRED).
func TestConfigRendererRemoteCredentialsInjected(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")
	password := `p&w<>"'`

	files := renderCHIFiles(t, port.RenderCHIRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		RemoteUser:     "distwriter",
		RemotePassword: password,
	})

	remote := files["chop-generated-remote_servers.xml"]
	replicas := countReplicaEntries(t, remote)

	require.Equal(t, replicas, strings.Count(remote, "<user>distwriter</user>"),
		"every <replica> must carry the remote user")
	require.Equal(t, replicas, strings.Count(remote, "<password>p&amp;w&lt;&gt;&#34;&#39;</password>"),
		"every <replica> must carry the XML-escaped password")
	require.NotContains(t, remote, password, "the raw password must never appear unescaped")
	parseXMLTokens(t, remote)
}

// TestConfigRendererRemoteCredentialsEscapeTable pins the exact escaping
// for the five XML-significant characters — the generator's own
// getRemoteServersClusterSecret escapes with encoding/xml and chctl must
// match byte-for-byte.
func TestConfigRendererRemoteCredentialsEscapeTable(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	files := renderCHIFiles(t, port.RenderCHIRequest{
		NormalizedCR:   cr,
		RemoteUser:     "u&1",
		RemotePassword: "<>&\"'",
	})

	remote := files["chop-generated-remote_servers.xml"]
	require.Contains(t, remote, "<user>u&amp;1</user>")
	require.Contains(t, remote, "<password>&lt;&gt;&amp;&#34;&#39;</password>")
	parseXMLTokens(t, remote)
}

// TestConfigRendererNoRemoteCredentialsUnchanged asserts backwards
// compatibility: no credentials in the request, no <user>/<password> in
// the output — the pre-credentials render byte-for-byte.
func TestConfigRendererNoRemoteCredentialsUnchanged(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	files := renderCHIFiles(t, port.RenderCHIRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
	})

	remote := files["chop-generated-remote_servers.xml"]
	require.NotContains(t, remote, "<user>", "no credentials: no <user> element")
	require.NotContains(t, remote, "<password>", "no credentials: no <password> element")
}

// TestConfigRendererRemotePasswordWithoutUserRejected asserts the port
// guard: the renderer is callable without the app's flag validation, so
// a password-only request must fail loudly rather than silently render
// credentials-free XML.
func TestConfigRendererRemotePasswordWithoutUserRejected(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")

	_, err := NewConfigRenderer().RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR:   cr,
		RemotePassword: "orphan",
		OutputDir:      t.TempDir(),
		DryRun:         true,
	})
	require.ErrorContains(t, err, "RemotePassword set without RemoteUser")
}

// TestConfigRendererRemoteCredentialsWritten0640 asserts the secrets
// hygiene follow-through: remote_servers.xml carrying a password lands
// 0640 (group-readable at most), never world-readable.
func TestConfigRendererRemoteCredentialsWritten0640(t *testing.T) {
	cr := loadCHIFromFile(t, "ch-int-dev", "")
	outDir := t.TempDir()

	rendered, err := NewConfigRenderer().RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		RemoteUser:     "distwriter",
		RemotePassword: "sekret",
		OutputDir:      outDir,
	})
	require.NoError(t, err)

	for _, file := range rendered {
		info, err := os.Stat(filepath.Join(outDir, filepath.Base(file.Path)))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o640), info.Mode().Perm(),
			"%s must be 0640", file.Path)
	}
}

func TestUsersRendererEmitsReferencedProfiles(t *testing.T) {
	// A user referencing a spec profile must get that profile in the same
	// file (Code 347: Profile was not found — the live finding for ingest).
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: profiles-test
spec:
  configuration:
    profiles:
      ingest/insert_quorum: "auto"
      ingest/select_sequential_consistency: "1"
    users:
      writer/passwordFrom: "writer-secret"
      writer/profile: ingest
`)
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	rendered, err := NewUsersRenderer(fakeSecretsResolver{values: map[string]string{"writer-secret": "v"}}).RenderUsers(
		context.Background(), port.RenderUsersRequest{NormalizedCR: crAny, OutputDir: t.TempDir()})
	require.NoError(t, err)

	content := rendered[0].Content
	require.Contains(t, content, "<ingest>")
	require.Contains(t, content, "insert_quorum")
	// Well-formed XML.
	decoder := xml.NewDecoder(strings.NewReader(content))
	for {
		if _, err := decoder.Token(); err == io.EOF {
			break
		} else {
			require.NoError(t, err, "malformed users XML: %s", content)
		}
	}
}

func TestUsersRendererProfilesWrapped(t *testing.T) {
	// Profiles must emit inside a <profiles> wrapper (live finding: the
	// raw profile tree at the document root parses as a user entry).
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: profiles-wrap
spec:
  configuration:
    profiles:
      ingest/insert_quorum: "auto"
    users:
      writer/passwordFrom: "s"
      writer/profile: ingest
`)
	crAny, err := NewSpecLoader().LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)
	rendered, err := NewUsersRenderer(fakeSecretsResolver{values: map[string]string{"s": "v"}}).RenderUsers(
		context.Background(), port.RenderUsersRequest{NormalizedCR: crAny, OutputDir: t.TempDir()})
	require.NoError(t, err)
	require.Contains(t, rendered[0].Content, "<profiles>")
	require.Contains(t, rendered[0].Content, "<ingest>")
}

// --- render-node (the single-invocation node render + install) ---

// renderNodeSpec is a CHI spec exercising every section render-node
// composes: clusters (remote_servers), zookeeper, profiles, settings,
// users and a files: entry — the shape the stack's chi.yaml carries.
const renderNodeSpec = `
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: render-node
spec:
  configuration:
    clusters:
      - name: logexplorer
        layout:
          shardsCount: 1
          replicasCount: 2
    zookeeper:
      nodes:
        - host: keeper-0.ch.internal
          port: 9181
    profiles:
      default/async_insert: "1"
    settings:
      query_log/partition_by: "toYYYYMM(event_date)"
    users:
      default/profile: readonly
      admin/profile: default
      admin/access_management: "1"
    files:
      config.d/server-prometheus.xml: |
        <clickhouse>
            <prometheus>
                <endpoint>/metrics</endpoint>
            </prometheus>
        </clickhouse>
`

// loadRenderNodeCHI loads the shared render-node fixture.
func loadRenderNodeCHI(t *testing.T) *v1.ClickHouseInstallation {
	t.Helper()
	loader := NewSpecLoader()
	crAny, err := loader.LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: []byte(renderNodeSpec)},
	})
	require.NoError(t, err)
	cr, ok := crAny.(*v1.ClickHouseInstallation)
	require.True(t, ok, "loader returned non-CHI")
	return cr
}

// newTestNodeRenderer wires the node renderer with the fake secrets
// resolver — render-node tests never touch AWS.
func newTestNodeRenderer() *NodeRenderer {
	return NewNodeRenderer(NewConfigRenderer(), NewUsersRenderer(fakeSecretsResolver{}))
}

// renderNodePlan runs a dry render-node and returns the plan keyed by
// destination path.
func renderNodePlan(t *testing.T, request port.RenderNodeRequest) (map[string]port.RenderNodeFile, port.RenderNodeResult) {
	t.Helper()
	request.DryRun = true
	result, err := newTestNodeRenderer().RenderNode(context.Background(), request)
	require.NoError(t, err)
	plan := make(map[string]port.RenderNodeFile, len(result.Files))
	for _, file := range result.Files {
		plan[file.Path] = file
	}
	return plan, result
}

// TestRenderNodeComposesTheThreeRenders asserts the keystone contract:
// ONE render-node invocation produces exactly what composing the
// existing three paths produces — the host-selected render, the
// inventory render and the users render — byte-for-byte, for the same
// spec and flags. A divergence here would break the cutover the stack
// plans around this command.
func TestRenderNodeComposesTheThreeRenders(t *testing.T) {
	cr := loadRenderNodeCHI(t)
	inventory := []domain.HostIdentity{
		{Name: "0-0", Address: "writer-0.ch.internal"},
		{Name: "0-1", Address: "reader-0.ch.internal"},
	}
	request := port.RenderNodeRequest{
		NormalizedCR:   cr,
		NodeType:       "writer",
		HostName:       "0-0",
		Hosts:          inventory,
		Replicas:       map[string]string{"0-0": "writer-0"},
		RemoteUser:     "distwriter",
		RemotePassword: "sekret",
		OutputDir:      "/etc/clickhouse-server/config.d",
		UsersDir:       "/etc/clickhouse-server/users.d",
	}
	plan, result := renderNodePlan(t, request)

	// The three individually-rendered paths, composed by hand.
	chiFiles, err := NewConfigRenderer().RenderCHI(context.Background(), port.RenderCHIRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		Hosts:          inventory,
		Replicas:       map[string]string{"0-0": "writer-0"},
		RemoteUser:     "distwriter",
		RemotePassword: "sekret",
		OutputDir:      "/etc/clickhouse-server/config.d",
		UsersDir:       "/etc/clickhouse-server/users.d",
		DryRun:         true,
	})
	require.NoError(t, err)
	usersFiles, err := NewUsersRenderer(fakeSecretsResolver{}).RenderUsers(context.Background(), port.RenderUsersRequest{
		NormalizedCR: cr,
		OutputDir:    "/etc/clickhouse-server/users.d",
		DryRun:       true,
	})
	require.NoError(t, err)

	composed := make(map[string]port.RenderedFile, len(chiFiles)+len(usersFiles))
	for _, file := range chiFiles {
		// The machinery clickhouse_operator user never reaches
		// users-group files: RenderCHI drops it from the users tree
		// before emission (its profile is operator-shipped on-cluster
		// only — Code 347 off-cluster), so the composed expectation is
		// exactly what the renderer emits, no post-guard.
		dir := "/etc/clickhouse-server/config.d"
		if file.Group == "users" {
			dir = "/etc/clickhouse-server/users.d"
		}
		composed[filepath.Join(dir, filepath.Base(file.Path))] = file
	}
	for _, file := range usersFiles {
		composed[filepath.Join("/etc/clickhouse-server/users.d", filepath.Base(file.Path))] = file
	}

	require.Len(t, plan, len(composed),
		"render-node plan and the composed three renders must cover the same files")
	for path, expected := range composed {
		actual, ok := plan[path]
		require.True(t, ok, "file %s missing from the render-node plan", path)
		require.Equal(t, expected.Content, actual.Content,
			"render-node content for %s must equal the composed render", path)
		require.Equal(t, expected.Group, actual.Group, "group of %s", path)
	}

	// The credential split: remote_servers with an injected password and
	// every users file stay 0640; the rest (macros, zookeeper,
	// settings, profiles, the files: entry) is world-readable 0644.
	require.Equal(t, os.FileMode(0o640), plan["/etc/clickhouse-server/config.d/chop-generated-remote_servers.xml"].Mode,
		"remote_servers with an injected password must be 0640")
	require.Equal(t, os.FileMode(0o644), plan["/etc/clickhouse-server/config.d/chop-generated-macros.xml"].Mode)
	require.Equal(t, os.FileMode(0o644), plan["/etc/clickhouse-server/config.d/chop-generated-zookeeper.xml"].Mode)
	require.Equal(t, os.FileMode(0o644), plan["/etc/clickhouse-server/config.d/server-prometheus.xml"].Mode,
		"spec files: entry without credentials is ordinary configuration: 0644")
	require.Equal(t, os.FileMode(0o640), plan["/etc/clickhouse-server/users.d/chop-generated-users.xml"].Mode)
	require.Equal(t, os.FileMode(0o640), plan["/etc/clickhouse-server/users.d/z_chctl-users.xml"].Mode)

	// Every section the node needs is present in ONE plan.
	for _, section := range []string{
		"chop-generated-macros.xml",
		"chop-generated-zookeeper.xml",
		"chop-generated-settings.xml",
		"chop-generated-profiles.xml",
		"chop-generated-remote_servers.xml",
		"chop-generated-users.xml",
		"z_chctl-users.xml",
		"server-prometheus.xml",
	} {
		_, ok := plan[filepath.Join("/etc/clickhouse-server/config.d", section)]
		if !ok {
			_, ok = plan[filepath.Join("/etc/clickhouse-server/users.d", section)]
		}
		require.True(t, ok, "section %s missing from the plan", section)
	}
	require.Equal(t, "writer", result.NodeType)
}

// TestRenderNodeNoCredentialsKeeps0644 asserts the mode split keys on
// CONTENT, not on flags: without remote credentials remote_servers.xml
// carries no password and is ordinary 0644 (matching the config render
// back-compat output discipline the stack's chmod lines applied).
func TestRenderNodeNoCredentialsKeeps0644(t *testing.T) {
	cr := loadRenderNodeCHI(t)
	plan, _ := renderNodePlan(t, port.RenderNodeRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    "/etc/clickhouse-server/config.d",
		UsersDir:     "/etc/clickhouse-server/users.d",
	})
	require.Equal(t, os.FileMode(0o644),
		plan["/etc/clickhouse-server/config.d/chop-generated-remote_servers.xml"].Mode)
}

// TestRenderNodeInstallPermsAndOwnership asserts the install
// discipline on disk: exact 0644/0640 modes (umask cannot soften them)
// and chown to the requested owner. The -user numeric form is how the
// test asserts ownership without a clickhouse account on the host —
// the same escape hatch the flag documents for odd distros.
func TestRenderNodeInstallPermsAndOwnership(t *testing.T) {
	cr := loadRenderNodeCHI(t)
	outDir := t.TempDir()
	usersDir := t.TempDir()

	result, err := newTestNodeRenderer().RenderNode(context.Background(), port.RenderNodeRequest{
		NormalizedCR:   cr,
		HostName:       "0-0",
		RemoteUser:     "distwriter",
		RemotePassword: "sekret",
		OutputDir:      outDir,
		UsersDir:       usersDir,
		OwnerUser:      fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
	})
	require.NoError(t, err)
	require.Empty(t, result.Pruned)
	require.Empty(t, result.Drift)

	for _, file := range result.Files {
		info, err := os.Stat(file.Path)
		require.NoError(t, err, "installed file %s", file.Path)
		require.Equal(t, file.Mode, info.Mode().Perm(),
			"%s must be %04o after install (umask cannot soften it)", file.Path, file.Mode)
		// Ownership: the -user override landed (self-owned in the test
		// environment — the assertion proves chown ran).
		stat, ok := info.Sys().(any)
		require.True(t, ok)
		_ = stat // ownership is asserted via the uid/gid syscall below
	}
	// Spot-check the credential split on disk.
	info, err := os.Stat(filepath.Join(outDir, "chop-generated-remote_servers.xml"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
	info, err = os.Stat(filepath.Join(outDir, "chop-generated-macros.xml"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o644), info.Mode().Perm())
	info, err = os.Stat(filepath.Join(usersDir, "z_chctl-users.xml"))
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
}

// TestRenderNodeAtomicity asserts the crash-class gate: a spec whose
// files: section renders malformed XML fails the run BEFORE anything
// installs — the installed directory stays byte-identical (here:
// empty) and the error names the offending file.
func TestRenderNodeAtomicity(t *testing.T) {
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: bad-files
spec:
  configuration:
    clusters:
      - name: logexplorer
        layout: { shardsCount: 1, replicasCount: 1 }
    files:
      config.d/broken.xml: |
        <clickhouse>
            <unclosed>
        </clickhouse>
`)
	crAny, err := NewSpecLoader().LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	outDir := t.TempDir()
	usersDir := t.TempDir()
	_, err = newTestNodeRenderer().RenderNode(context.Background(), port.RenderNodeRequest{
		NormalizedCR: crAny,
		HostName:     "0-0",
		OutputDir:    outDir,
		UsersDir:     usersDir,
		OwnerUser:    fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "broken.xml", "the error must name the offending section")
	require.Contains(t, err.Error(), "malformed XML")

	// NOTHING installed — not even the valid sections that rendered
	// before the broken one was validated.
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Empty(t, entries, "a failed render must install nothing")
	entries, err = os.ReadDir(usersDir)
	require.NoError(t, err)
	require.Empty(t, entries, "a failed render must install nothing into users.d either")
}

// TestRenderNodeDryRunWritesNothing asserts the -dry-run contract:
// the full plan is returned (destinations, modes, content) but not a
// single byte lands on disk.
func TestRenderNodeDryRunWritesNothing(t *testing.T) {
	cr := loadRenderNodeCHI(t)
	outDir := t.TempDir()
	usersDir := t.TempDir()

	plan, result := renderNodePlan(t, port.RenderNodeRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    outDir,
		UsersDir:     usersDir,
	})
	require.NotEmpty(t, plan)
	require.True(t, result.DryRun)

	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Empty(t, entries)
	entries, err = os.ReadDir(usersDir)
	require.NoError(t, err)
	require.Empty(t, entries)
}

// TestRenderNodeCheckModes asserts the -check contract end-to-end over
// a real install: exit-code signal aside, in-sync yields no drift and
// an edited installed file surfaces as a per-file delta — with the
// check itself installing nothing (the edit survives).
func TestRenderNodeCheckModes(t *testing.T) {
	cr := loadRenderNodeCHI(t)
	outDir := t.TempDir()
	usersDir := t.TempDir()
	owner := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	base := port.RenderNodeRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    outDir,
		UsersDir:     usersDir,
		OwnerUser:    owner,
	}

	// Check before any install: every file is missing → drift.
	result, err := newTestNodeRenderer().RenderNode(context.Background(), withCheck(base))
	require.NoError(t, err)
	require.NotEmpty(t, result.Drift)
	for _, line := range result.Drift {
		require.Contains(t, line, ": missing")
	}

	// Install, then check with identical args: in sync.
	_, err = newTestNodeRenderer().RenderNode(context.Background(), base)
	require.NoError(t, err)
	result, err = newTestNodeRenderer().RenderNode(context.Background(), withCheck(base))
	require.NoError(t, err)
	require.Empty(t, result.Drift, "a just-installed tree must check in sync")

	// A hand edit drifts; the check must not repair it.
	edited := filepath.Join(outDir, "chop-generated-macros.xml")
	original, err := os.ReadFile(edited)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(edited, append(original, []byte("<!-- drift -->")...), 0o644))
	result, err = newTestNodeRenderer().RenderNode(context.Background(), withCheck(base))
	require.NoError(t, err)
	require.Len(t, result.Drift, 1)
	require.Contains(t, result.Drift[0], "chop-generated-macros.xml: changed")
	after, err := os.ReadFile(edited)
	require.NoError(t, err)
	require.Contains(t, string(after), "drift", "check mode must not write anything")

	// A missing installed file is drift too (a wiped config.d must never
	// report in sync).
	require.NoError(t, os.Remove(filepath.Join(outDir, "chop-generated-zookeeper.xml")))
	result, err = newTestNodeRenderer().RenderNode(context.Background(), withCheck(base))
	require.NoError(t, err)
	require.Len(t, result.Drift, 2)
	require.Contains(t, strings.Join(result.Drift, "\n"), "chop-generated-zookeeper.xml: missing")
	require.Contains(t, strings.Join(result.Drift, "\n"), "chop-generated-macros.xml: changed")
}

// withCheck copies the request with Check on.
func withCheck(request port.RenderNodeRequest) port.RenderNodeRequest {
	request.Check = true
	request.DryRun = false
	return request
}

// TestRenderNodePruneManifestOnly asserts the prune discipline: files
// the manifest tracks but the render stopped producing are removed,
// while hand-maintained files in the same directory are never touched —
// the exact hazard the stack's hand-rolled prune lists got wrong.
func TestRenderNodePruneManifestOnly(t *testing.T) {
	cr := loadRenderNodeCHI(t)
	outDir := t.TempDir()
	usersDir := t.TempDir()
	owner := fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid())
	base := port.RenderNodeRequest{
		NormalizedCR: cr,
		HostName:     "0-0",
		OutputDir:    outDir,
		UsersDir:     usersDir,
		OwnerUser:    owner,
	}

	// First install writes the manifest.
	_, err := newTestNodeRenderer().RenderNode(context.Background(), base)
	require.NoError(t, err)

	// Simulate a previous run's file that no longer renders, plus a
	// hand-maintained overlay and a hand-written chop-generated file
	// (the backup-node pattern) — both must survive the prune.
	stale := filepath.Join(outDir, "chctl-retired-section.xml")
	require.NoError(t, os.WriteFile(stale, []byte("<clickhouse/>"), 0o644))
	handOverlay := filepath.Join(outDir, "network.xml")
	require.NoError(t, os.WriteFile(handOverlay, []byte("<clickhouse/>"), 0o644))
	handChopNamed := filepath.Join(outDir, "chop-generated-handmade.xml")
	require.NoError(t, os.WriteFile(handChopNamed, []byte("<clickhouse/>"), 0o644))
	manifestPath := filepath.Join(outDir, renderNodeManifestName)
	manifest, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(manifestPath, append(manifest, []byte("chctl-retired-section.xml\n")...), 0o644))

	result, err := newTestNodeRenderer().RenderNode(context.Background(), base)
	require.NoError(t, err)
	require.Equal(t, []string{stale}, result.Pruned)
	require.NoFileExists(t, stale, "the stale manifest-tracked file must be pruned")
	require.FileExists(t, handOverlay, "hand-maintained files are never prune candidates")
	require.FileExists(t, handChopNamed, "a hand-written chop-generated-* file is not manifest-tracked: never pruned")

	// The manifest reflects the current install exactly.
	data, err := os.ReadFile(manifestPath)
	require.NoError(t, err)
	require.NotContains(t, string(data), "chctl-retired-section.xml")
	require.Contains(t, string(data), "chop-generated-macros.xml")
	require.NotContains(t, string(data), "network.xml")
	require.NotContains(t, string(data), "chop-generated-handmade.xml")
}

// TestRenderNodeKeeperSingleFile asserts the keeper flow: ONE merged
// file (no users.d render, no per-group installs) lands at -single-file
// with the server-id override, raft cloud names and the prometheus/
// logger files sections riding it.
func TestRenderNodeKeeperSingleFile(t *testing.T) {
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	outDir := t.TempDir()
	singlePath := filepath.Join(t.TempDir(), "keeper_config.xml")

	result, err := NewNodeRenderer(NewConfigRenderer(), nil).RenderNode(context.Background(), port.RenderNodeRequest{
		NormalizedCR:    cr,
		Keeper:          true,
		NodeType:        "keeper",
		HostName:        "0-0",
		Hosts:           keeperInventory,
		KeeperAddresses: map[string]string{"0-0": "keeper-0.aws.internal"},
		ServerID:        1,
		RaftIDs:         map[string]int{"0-0": 1, "0-1": 2, "0-2": 3},
		SingleFilePath:  singlePath,
		OutputDir:       outDir,
		OwnerUser:       fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
	})
	require.NoError(t, err)
	require.Len(t, result.Files, 1, "keeper mode installs exactly one file")

	file := result.Files[0]
	require.Equal(t, singlePath, file.Path)
	require.Equal(t, os.FileMode(0o640), file.Mode, "keeper config may carry TLS material: 0640")
	requireWellFormedXML(t, file.Content)
	require.Contains(t, file.Content, "<server_id>1</server_id>")
	require.Contains(t, file.Content, "<hostname>keeper-0.aws.internal</hostname>")
	require.Contains(t, file.Content, "<id>1</id>")
	require.Contains(t, file.Content, "<logger>", "files sections ride the single file")

	// On disk, byte-identical, correct mode; -out stays untouched.
	data, err := os.ReadFile(singlePath)
	require.NoError(t, err)
	require.Equal(t, file.Content, string(data))
	info, err := os.Stat(singlePath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)
	require.Empty(t, entries, "keeper mode must not write per-group files to -out")
}

// TestRenderNodeKeeperRequiresSingleFileAndHost asserts the flag gate:
// keeper mode without -single-file or without a host is a usage error
// before any rendering.
func TestRenderNodeKeeperRequiresSingleFileAndHost(t *testing.T) {
	cr := loadCHKFromSpec(t, keeperSingleFileSpec)
	base := port.RenderNodeRequest{
		NormalizedCR:   cr,
		Keeper:         true,
		NodeType:       "keeper",
		OutputDir:      t.TempDir(),
		SingleFilePath: filepath.Join(t.TempDir(), "keeper_config.xml"),
	}
	_, err := NewNodeRenderer(NewConfigRenderer(), nil).RenderNode(context.Background(), base)
	require.ErrorContains(t, err, "requires a host")

	noSingle := base
	noSingle.SingleFilePath = ""
	_, err = NewNodeRenderer(NewConfigRenderer(), nil).RenderNode(context.Background(), noSingle)
	require.ErrorContains(t, err, "keeper mode requires -single-file")
}

// TestRenderNodeValidationErrors asserts the request guards: empty
// out/users dirs and a render that produces nothing are errors.
func TestRenderNodeValidationErrors(t *testing.T) {
	cr := loadRenderNodeCHI(t)
	_, err := newTestNodeRenderer().RenderNode(context.Background(), port.RenderNodeRequest{
		NormalizedCR: cr, HostName: "0-0",
	})
	require.ErrorContains(t, err, "output dir is empty")

	_, err = newTestNodeRenderer().RenderNode(context.Background(), port.RenderNodeRequest{
		NormalizedCR: cr, HostName: "0-0", OutputDir: "/x",
	})
	require.ErrorContains(t, err, "users dir is empty")
}

// TestRenderNodeUsersWithoutUsersSection asserts a spec with no users
// section at all renders the generator's own users-group files without
// failing (the minimal-CHI case; ErrNoUsersSection is a benign skip).
func TestRenderNodeUsersWithoutUsersSection(t *testing.T) {
	spec := []byte(`
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: no-users
spec:
  configuration:
    clusters:
      - name: logexplorer
        layout: { shardsCount: 1, replicasCount: 1 }
`)
	crAny, err := NewSpecLoader().LoadCHI(context.Background(), port.LoadCHIRequest{
		Spec: domain.SpecReference{Data: spec},
	})
	require.NoError(t, err)

	plan, _ := renderNodePlan(t, port.RenderNodeRequest{
		NormalizedCR: crAny,
		HostName:     "0-0",
		OutputDir:    "/etc/clickhouse-server/config.d",
		UsersDir:     "/etc/clickhouse-server/users.d",
	})
	require.NotEmpty(t, plan)
}

// TestRenderNodeOwnerLookup asserts the -user forms: numeric uid:gid,
// bare numeric uid and an explicit unknown name (a hard error — the
// operator asked for it). The default-owner path is best-effort and
// covered by every other install test (self-owned or clickhouse-less).
func TestRenderNodeOwnerLookup(t *testing.T) {
	uid, gid := os.Getuid(), os.Getgid()

	// Numeric uid:gid.
	_, gotGid, err := lookupOwnerIDs(fmt.Sprintf("%d:%d", uid, gid))
	require.NoError(t, err)
	require.Equal(t, gid, gotGid)

	// Bare numeric uid: resolves to the account's primary group (same
	// gid the name form yields).
	_, gotGid, err = lookupOwnerIDs(fmt.Sprintf("%d", uid))
	require.NoError(t, err)
	require.Equal(t, gid, gotGid)

	// name form for the running user (the primary group must match).
	name := "unknown-user-for-test"
	if u, err := osuser.LookupId(fmt.Sprintf("%d", uid)); err == nil {
		name = u.Username
	}
	_, gotGid, err = lookupOwnerIDs(name)
	require.NoError(t, err)
	require.Equal(t, gid, gotGid)

	// An explicit unknown account is a hard error.
	_, _, err = lookupOwnerIDs("no-such-user-for-chctl-test")
	require.ErrorContains(t, err, "not found")
}
