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
	"os"
	"path/filepath"
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
