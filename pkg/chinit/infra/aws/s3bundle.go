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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// bundleFiles are the known members of the config bundle. The list is
// closed (the Terraform bundle layout is fixed): chinit.yaml is NOT in
// the bundle — it arrives via user-data and names the bundle.
var bundleFiles = []string{
	"network.xml",
	"ssl.xml",
	"storage.xml",
	"chi.yaml",
	"keeper.yaml",
}

// S3API is the S3 surface the bundle fetcher needs (s3.Client satisfies
// it; tests provide a fake).
type S3API interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput, opts ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// S3BundleSource fetches the config bundle from the data bucket.
type S3BundleSource struct {
	region string
	client S3API
}

// NewS3BundleSource builds the fetcher; the lazy client materializes on
// first use (the boot may never fetch when the bundle is local, e.g.
// packer builds staging files).
func NewS3BundleSource(region string) *S3BundleSource {
	return &S3BundleSource{region: region}
}

// WithClient injects a concrete API client (tests).
func (s *S3BundleSource) WithClient(c S3API) *S3BundleSource {
	s.client = c
	return s
}

// clientOrInit returns the client, loading the SDK config on first use.
func (s *S3BundleSource) clientOrInit(ctx context.Context) (S3API, error) {
	if s.client != nil {
		return s.client, nil
	}
	cfg, err := awsConfig(ctx, s.region)
	if err != nil {
		return nil, err
	}
	s.client = s3.NewFromConfig(cfg)
	return s.client, nil
}

// FetchBundle downloads every bundle member into destDir. The SHA256
// gate (when the spec pins one) is the aggregate over member contents —
// a mismatch is Fatal (never retried): a drifted bundle means the spec
// hash in tfvars is stale, and retrying would install files the release
// does not vouch for.
func (s *S3BundleSource) FetchBundle(ctx context.Context, bundle model.ConfigBundle, destDir string) ([]string, error) {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return nil, chinitsvc.Fatal(fmt.Errorf("create %s: %w", destDir, err))
	}
	client, err := s.clientOrInit(ctx)
	if err != nil {
		return nil, err
	}

	var written []string
	hasher := sha256.New()
	for _, name := range bundleFiles {
		key := strings.TrimPrefix(bundle.Prefix+"/", "/") + name
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: awsString(bundle.Bucket),
			Key:    awsString(key),
		})
		if err != nil {
			// NoSuchKey for chi.yaml/keeper.yaml on single-role stacks
			// is benign (writers skip keeper.yaml); missing static
			// XMLs are fatal. The error text distinguishes them.
			if strings.Contains(err.Error(), "NoSuchKey") && (name == "chi.yaml" || name == "keeper.yaml") {
				continue
			}
			return nil, fmt.Errorf("fetch s3://%s/%s: %w", bundle.Bucket, key, err)
		}
		body, err := io.ReadAll(out.Body)
		_ = out.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("read s3://%s/%s: %w", bundle.Bucket, key, err)
		}
		if len(body) == 0 {
			return nil, fmt.Errorf("s3://%s/%s is empty", bundle.Bucket, key)
		}
		dest := filepath.Join(destDir, name)
		if err := os.WriteFile(dest, body, 0644); err != nil {
			return nil, chinitsvc.Fatal(fmt.Errorf("write %s: %w", dest, err))
		}
		written = append(written, dest)
		_, _ = hasher.Write(body)
	}

	if bundle.SHA256 != "" {
		got := hex.EncodeToString(hasher.Sum(nil))
		if got != bundle.SHA256 {
			return written, chinitsvc.Fatal(fmt.Errorf(
				"config bundle SHA256 mismatch: want %s got %s — the tfvars hash is stale or the bucket was overwritten; refusing to install",
				bundle.SHA256, got))
		}
	}
	return written, nil
}

// awsString is a local *string helper (keeps the aws import surface
// small in files that only need pointers).
func awsString(s string) *string { return &s }
