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

	"github.com/aws/aws-sdk-go-v2/service/s3"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// This file pins the S3 bundle fetch: the REAL Terraform bundle layout
// (<prefix>/configs/<name>.xml, specs at the root — the fakes package
// cannot catch layout drift) and the SHA256 gate, whose digest MUST
// equal Terraform's config_files_hash. It runs under the package's
// single Ginkgo suite alongside the claim-lister specs.

// fakeS3 answers GetObject from a canned key map.
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

// bundleKeys is the full real-bundle key set (network/ssl/storage/
// backup-replica XMLs + chi.yaml); specs mutate from here.
func bundleKeys() map[string]string {
	return map[string]string{
		"bootstrap/configs/network.xml":                 "<network/>",
		"bootstrap/configs/ssl.xml":                     "<ssl/>",
		"bootstrap/configs/storage.xml":                 "<storage/>",
		"bootstrap/configs/backup-replica-settings.xml": "<backup-replica/>",
		"bootstrap/chi.yaml":                            "chi: spec",
	}
}

// fetch runs one FetchBundle against a canned S3 and returns the
// files written (plus the error, for the gate specs).
func fetch(keys map[string]string, bundle model.ConfigBundle) ([]string, error) {
	src := NewS3BundleSource("us-west-2").WithClient(&fakeS3{keys: keys})
	return src.FetchBundle(context.Background(), bundle, GinkgoT().TempDir())
}

var _ = Describe("S3BundleSource.FetchBundle", func() {
	Context("layout", func() {
		It("fetches the XMLs from <prefix>/configs/ and the specs from the root", func() {
			// The layout the stack uploads and the refresh path
			// consumes; <prefix>/<name>.xml 404s on a real bucket
			// (the live chinit-v0.1.0 boot failed exactly this way).
			written, err := fetch(bundleKeys(), model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap"})
			Expect(err).NotTo(HaveOccurred())
			Expect(written).To(HaveLen(5))
			for _, name := range []string{"network.xml", "ssl.xml", "storage.xml", "backup-replica-settings.xml", "chi.yaml"} {
				Expect(written).To(ContainElement(HaveSuffix("/" + name)))
			}
			// keeper.yaml NoSuchKey is benign (single-role stacks skip
			// it).
			Expect(written).NotTo(ContainElement(HaveSuffix("/keeper.yaml")))
		})

		It("is fatal on a missing static XML — the node never boots on a partial bundle", func() {
			keys := map[string]string{"bootstrap/chi.yaml": "chi: spec"}
			_, err := fetch(keys, model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap"})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("configs/"))
		})
	})

	Context("the SHA256 gate", func() {
		It("refuses a bundle whose digest does not match the pinned sha", func() {
			_, err := fetch(bundleKeys(), model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap", SHA256: "deadbeef"})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("SHA256 mismatch"))
		})

		It("accepts Terraform's own config_files_hash and refuses a one-bit content change", func() {
			// The gate digest MUST equal Terraform's
			// sha256(jsonencode(merge(config_files_all, {"chi.yaml": spec}))):
			// sorted key→content pairs, JSON-escaped values. A digest
			// in any other shape fails EVERY boot (the spec's sha256
			// is Terraform's value). Computed here independently of
			// chinit's code path.
			net := "<network>0.0.0.0</network>"
			ssl := "<ssl>\n<cert>/etc/clickhouse-server/ssl/server.crt</cert>\n</ssl>"
			stor := "<storage><disk>s3</disk></storage>"
			chi := "apiVersion: clickhouse.altinity.com/v1\nkind: ClickHouseInstallation\n"

			merge := map[string]string{
				"configs/network.xml":                 net,
				"configs/ssl.xml":                     ssl,
				"configs/storage.xml":                 stor,
				"configs/backup-replica-settings.xml": "<backup-replica/>",
				"chi.yaml":                            chi,
			}
			enc, err := json.Marshal(merge) // jsonencode: sorted pairs, escaped values
			Expect(err).NotTo(HaveOccurred())
			want := sha256.Sum256(enc)
			wantHex := hex.EncodeToString(want[:])

			keys := map[string]string{
				"bootstrap/configs/network.xml":                 net,
				"bootstrap/configs/ssl.xml":                     ssl,
				"bootstrap/configs/storage.xml":                 stor,
				"bootstrap/configs/backup-replica-settings.xml": "<backup-replica/>",
				"bootstrap/chi.yaml":                            chi,
			}
			written, err := fetch(keys, model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap", SHA256: wantHex})
			Expect(err).NotTo(HaveOccurred(), "the gate MUST accept Terraform's own digest; got: %v", err)
			Expect(written).To(HaveLen(5))

			// And a one-bit content change must be refused.
			keys["bootstrap/configs/storage.xml"] = stor + "\n<!-- edited -->"
			_, err = fetch(keys, model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap", SHA256: wantHex})
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("SHA256 mismatch"))
		})

		It("excludes keeper.yaml from the digest (Terraform's hash does not cover it)", func() {
			// A writer stack and a keeper stack with the same
			// config_files_hash both pass the gate.
			merge := map[string]string{
				"configs/network.xml":                 "<network/>",
				"configs/ssl.xml":                     "<ssl/>",
				"configs/storage.xml":                 "<storage/>",
				"configs/backup-replica-settings.xml": "<backup-replica/>",
				"chi.yaml":                            "chi: spec",
			}
			enc, err := json.Marshal(merge)
			Expect(err).NotTo(HaveOccurred())
			want := sha256.Sum256(enc)
			wantHex := hex.EncodeToString(want[:])

			keys := bundleKeys()
			keys["bootstrap/keeper.yaml"] = "keeper: spec"
			_, err = fetch(keys, model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap", SHA256: wantHex})
			Expect(err).NotTo(HaveOccurred(), "keeper.yaml presence must not affect the gate")
		})
	})
})
