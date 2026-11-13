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
		"bootstrap/configs/network.xml": "<network/>",
		"bootstrap/configs/ssl.xml":     "<ssl/>",
		"bootstrap/configs/storage.xml": "<storage/>",
		"bootstrap/chi.yaml":            "chi: spec",
	}
	src := NewS3BundleSource("us-west-2").WithClient(&fakeS3{keys: keys})
	dest := t.TempDir()

	written, err := src.FetchBundle(ctx, model.ConfigBundle{
		Bucket: "data-bucket",
		Prefix: "bootstrap",
	}, dest)
	require.NoError(t, err)
	require.Len(t, written, 4)
	for _, name := range []string{"network.xml", "ssl.xml", "storage.xml", "chi.yaml"} {
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
		"bootstrap/configs/network.xml": "<network/>",
		"bootstrap/configs/ssl.xml":     "<ssl/>",
		"bootstrap/configs/storage.xml": "<storage/>",
		"bootstrap/chi.yaml":            "chi: spec",
	}})
	_, err := src.FetchBundle(ctx, model.ConfigBundle{
		Bucket: "data-bucket",
		Prefix: "bootstrap",
		SHA256: "deadbeef",
	}, t.TempDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "SHA256 mismatch")
}
