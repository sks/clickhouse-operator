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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// bundleFiles are the known members of the config bundle. The list is
// closed (the Terraform bundle layout is fixed): chinit.yaml is NOT in
// the bundle — it arrives via user-data and names the bundle. The
// static XMLs live under configs/ in the bundle prefix (the same keys
// the refresh path and the pre-chinit cloud-init consumed); the specs
// sit at the prefix root.
//
// The SHA256 gate contract — the digest MUST equal Terraform's
// config_files_hash, i.e. sha256(jsonencode(merge(config_files_all,
// {"chi.yaml": spec}))): jsonencode of a map renders SORTED key→content
// pairs wrapped in braces, so chinit recomputes exactly that over the
// five role-independent members it fetches (json.Marshal of the same
// Go map is byte-identical to Terraform's jsonencode). Two shape
// constraints fall out:
//   - keeper.yaml is fetched when present (keepers need it; writers
//     skip it via NoSuchKey) but EXCLUDED from the digest: Terraform's
//     hash does not include it (it is a separate spec upload), and
//     including it would make the digest role-dependent.
//   - backup-replica-settings.xml participates in the digest AND is
//     fetched by every role (one ~1KB file): Terraform's hash covers
//     it inside config_files_all, so a role-skipped fetch would make
//     the digests diverge. The static INSTALLER gates the overlay to
//     the backup role only — fetching everywhere, installing on one.
var bundleFiles = []bundleFile{
	{key: "configs/network.xml", name: "network.xml", hashed: true},
	{key: "configs/ssl.xml", name: "ssl.xml", hashed: true},
	{key: "configs/storage.xml", name: "storage.xml", hashed: true},
	{key: "configs/backup-replica-settings.xml", name: "backup-replica-settings.xml", hashed: true},
	{key: "chi.yaml", name: "chi.yaml", hashed: true},
	{key: "keeper.yaml", name: "keeper.yaml", hashed: false},
}

type bundleFile struct {
	key    string // S3 key suffix under the bundle prefix
	name   string // on-disk name in the staging dir
	hashed bool   // participates in the SHA256 gate digest
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
	// hashedContents maps bundle key → content for the digest (only
	// hashed members). Filled here, digested below in Terraform's
	// jsonencode shape.
	hashedContents := map[string]string{}
	for _, bf := range bundleFiles {
		key := strings.TrimPrefix(bundle.Prefix+"/", "/") + bf.key
		out, err := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: awsString(bundle.Bucket),
			Key:    awsString(key),
		})
		if err != nil {
			// NoSuchKey for chi.yaml/keeper.yaml on single-role stacks
			// is benign (writers skip keeper.yaml); missing static
			// XMLs are fatal. The error text distinguishes them.
			if strings.Contains(err.Error(), "NoSuchKey") && (bf.name == "chi.yaml" || bf.name == "keeper.yaml") {
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
		dest := filepath.Join(destDir, bf.name)
		if err := os.WriteFile(dest, body, 0644); err != nil {
			return nil, chinitsvc.Fatal(fmt.Errorf("write %s: %w", dest, err))
		}
		written = append(written, dest)
		if bf.hashed {
			// The digest key is the SHORT member name — Terraform's hash
			// merges {"chi.yaml": spec} at the TOP level (no prefix),
			// and the XMLs' keys there keep the configs/ prefix they
			// carry in config_files_all.
			hashedContents[bf.key] = string(body)
		}
	}

	if bundle.SHA256 != "" {
		got := hex.EncodeToString(terraformPairsDigest(hashedContents))
		if got != bundle.SHA256 {
			return written, chinitsvc.Fatal(fmt.Errorf(
				"config bundle SHA256 mismatch: want %s got %s — the tfvars hash is stale or the bucket was overwritten; refusing to install",
				bundle.SHA256, got))
		}
	}
	return written, nil
}

// terraformPairsDigest computes sha256 over the fetch's hashed members
// in EXACTLY Terraform's config_files_hash shape:
// sha256(jsonencode(merge(config_files_all, {"chi.yaml": spec}))).
// jsonencode of a map emits the SORTED key→content pairs, comma-joined,
// wrapped in braces, with the values HTML-escaped (<,>,& → \u003c,\u003e,
// \u0026) — verified byte-identical against Go's json.Marshal by a real
// `tofu apply` probing digest, and pinned by the parity test.
func terraformPairsDigest(contents map[string]string) []byte {
	enc, err := json.Marshal(contents)
	if err != nil {
		// Unreachable for string maps (encoding is total); a failure
		// must fail the gate rather than silently pass.
		panic("terraformPairsDigest: " + err.Error())
	}
	h := sha256.New()
	h.Write(enc)
	return h.Sum(nil)
}

// awsString is a local *string helper (keeps the aws import surface
// small in files that only need pointers).
func awsString(s string) *string { return &s }
