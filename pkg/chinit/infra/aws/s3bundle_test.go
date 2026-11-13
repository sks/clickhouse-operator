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

package aws

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// fakeS3 answers GetObject from a canned key map. The fakes package
// cannot catch layout drift: these keys are the REAL Terraform bundle
// layout (configs/*.xml under the prefix, specs at the root), which is
// exactly the contract FetchBundle must honor.
type fakeS3 struct {
	keys map[string]string
}

func (f *fakeS3) GetObject(_ context.Context, in *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	body, ok := f.keys[*in.Key]
	if !ok {
		return nil, &missingKey{key: *in.Key}
	}
	return &s3.GetObjectOutput{Body: io.NopCloser(strings.NewReader(body))}, nil
}

type missingKey struct{ key string }

func (m *missingKey) Error() string { return "NoSuchKey: " + m.key }

// TestFetchBundle_RealBundleLayout: the XMLs must be fetched from
// <prefix>/configs/<name>.xml — the layout the stack uploads and the
// refresh path consumes. Fetching <prefix>/<name>.xml 404s on a real
// bucket (the live chinit-v0.1.0 boot failed exactly this way).
func TestFetchBundle_RealBundleLayout(t *testing.T) {
	ctx := context.Context(context.Background())
	keys := map[string]string{
		"bootstrap/configs/network.xml":                 "<network/>",
		"bootstrap/configs/ssl.xml":                     "<ssl/>",
		"bootstrap/configs/storage.xml":                 "<storage/>",
		"bootstrap/configs/backup-replica-settings.xml": "<backup-replica/>",
		"bootstrap/chi.yaml":                            "chi: spec",
	}
	src := NewS3BundleSource("us-west-2").WithClient(&fakeS3{keys: keys})
	dest := t.TempDir()

	written, err := src.FetchBundle(ctx, model.ConfigBundle{
		Bucket: "data-bucket",
		Prefix: "bootstrap",
	}, dest)
	require.NoError(t, err)
	require.Len(t, written, 5)
	for _, name := range []string{"network.xml", "ssl.xml", "storage.xml", "backup-replica-settings.xml", "chi.yaml"} {
		require.FileExists(t, dest+"/"+name)
	}
	// keeper.yaml NoSuchKey is benign (single-role stacks skip it)
	require.NoFileExists(t, dest+"/keeper.yaml")
}

// TestFetchBundle_MissingXMLIsFatal: a 404 on a static XML aborts the
// fetch — the node must never boot against a partial bundle.
func TestFetchBundle_MissingXMLIsFatal(t *testing.T) {
	ctx := context.Context(context.Background())
	src := NewS3BundleSource("us-west-2").WithClient(&fakeS3{keys: map[string]string{
		"bootstrap/chi.yaml": "chi: spec",
	}})
	_, err := src.FetchBundle(ctx, model.ConfigBundle{
		Bucket: "data-bucket",
		Prefix: "bootstrap",
	}, t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "configs/")
}

// TestFetchBundle_ShaGate: with a pinned sha256 the fetch recomputes the
// member digest and refuses a mismatched bundle.
func TestFetchBundle_ShaGate(t *testing.T) {
	ctx := context.Context(context.Background())
	src := NewS3BundleSource("us-west-2").WithClient(&fakeS3{keys: map[string]string{
		"bootstrap/configs/network.xml":                 "<network/>",
		"bootstrap/configs/ssl.xml":                     "<ssl/>",
		"bootstrap/configs/storage.xml":                 "<storage/>",
		"bootstrap/configs/backup-replica-settings.xml": "<backup-replica/>",
		"bootstrap/chi.yaml":                            "chi: spec",
	}})
	_, err := src.FetchBundle(ctx, model.ConfigBundle{
		Bucket: "data-bucket",
		Prefix: "bootstrap",
		SHA256: "deadbeef",
	}, t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "SHA256 mismatch")
}

// TestFetchBundle_ShaGateMatchesTerraform: the gate digest MUST equal
// Terraform's config_files_hash — sha256(jsonencode(merge(
// config_files_all, {"chi.yaml": spec}))): sorted key→content pairs,
// JSON-escaped values. A digest in any other shape fails EVERY boot
// (the spec's sha256 is Terraform's value). Computed here
// independently of chinit's code path.
func TestFetchBundle_ShaGateMatchesTerraform(t *testing.T) {
	ctx := context.Context(context.Background())
	net := "<network>0.0.0.0</network>"
	ssl := "<ssl>\n<cert>/etc/clickhouse-server/ssl/server.crt</cert>\n</ssl>"
	stor := "<storage><disk>s3</disk></storage>"
	chi := "apiVersion: clickhouse.altinity.com/v1\nkind: ClickHouseInstallation\n"

	// Terraform-side computation (the expected value):
	merge := map[string]string{
		"configs/network.xml":                net,
		"configs/ssl.xml":                    ssl,
		"configs/storage.xml":                stor,
		"configs/backup-replica-settings.xml": "<backup-replica/>",
		"chi.yaml":                           chi,
	}
	enc, _ := json.Marshal(merge) // jsonencode: sorted pairs, escaped values
	want := sha256.Sum256(enc)
	wantHex := hex.EncodeToString(want[:])

	keys := map[string]string{
		"bootstrap/configs/network.xml":                 net,
		"bootstrap/configs/ssl.xml":                     ssl,
		"bootstrap/configs/storage.xml":                 stor,
		"bootstrap/configs/backup-replica-settings.xml": "<backup-replica/>",
		"bootstrap/chi.yaml":                            chi,
	}
	src := NewS3BundleSource("us-west-2").WithClient(&fakeS3{keys: keys})
	written, err := src.FetchBundle(ctx, model.ConfigBundle{
		Bucket: "data-bucket",
		Prefix: "bootstrap",
		SHA256: wantHex,
	}, t.TempDir())
	require.NoError(t, err, "the gate MUST accept Terraform's own digest; got: %v", err)
	require.Len(t, written, 5)

	// And a one-bit content change must be refused.
	keys["bootstrap/configs/storage.xml"] = stor + "\n<!-- edited -->"
	src2 := NewS3BundleSource("us-west-2").WithClient(&fakeS3{keys: keys})
	_, err = src2.FetchBundle(ctx, model.ConfigBundle{
		Bucket: "data-bucket",
		Prefix: "bootstrap",
		SHA256: wantHex,
	}, t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "SHA256 mismatch")
}

// TestFetchBundle_ShaGate_IgnoresKeeperYaml: keeper.yaml is fetched
// when present but excluded from the digest (Terraform's hash does not
// cover it), so a writer stack and a keeper stack with the same
// config_files_hash both pass the gate.
func TestFetchBundle_ShaGate_IgnoresKeeperYaml(t *testing.T) {
	ctx := context.Context(context.Background())
	merge := map[string]string{
		"configs/network.xml":                 "<network/>",
		"configs/ssl.xml":                     "<ssl/>",
		"configs/storage.xml":                 "<storage/>",
		"configs/backup-replica-settings.xml": "<backup-replica/>",
		"chi.yaml":                            "chi: spec",
	}
	enc, _ := json.Marshal(merge)
	want := hex.EncodeToString(func() []byte { s := sha256.Sum256(enc); return s[:] }())

	keys := map[string]string{
		"bootstrap/configs/network.xml":                 "<network/>",
		"bootstrap/configs/ssl.xml":                     "<ssl/>",
		"bootstrap/configs/storage.xml":                 "<storage/>",
		"bootstrap/configs/backup-replica-settings.xml": "<backup-replica/>",
		"bootstrap/chi.yaml":                            "chi: spec",
		"bootstrap/keeper.yaml":                         "keeper: spec",
	}
	src := NewS3BundleSource("us-west-2").WithClient(&fakeS3{keys: keys})
	_, err := src.FetchBundle(ctx, model.ConfigBundle{
		Bucket: "data-bucket",
		Prefix: "bootstrap",
		SHA256: want,
	}, t.TempDir())
	require.NoError(t, err, "keeper.yaml presence must not affect the gate")
}
