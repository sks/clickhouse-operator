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

package azure

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// KeyVaultSource resolves Key Vault secrets over the managed-identity
// token — no az CLI, no login step, no static credentials.
//
// The token flow (IMDS → OAuth token for https://vault.azure.net → GET
// the secret) is the same one az login --identity ran, in-process.
type KeyVaultSource struct {
	client httpClient
	// VaultName is the Key Vault (from the spec's secrets section
	// context; empty → resolution failure with a clear message).
	VaultName string
	// TokenSource mints the bearer token (nil → IMDS managed identity).
	TokenSource func(ctx context.Context, resource string) (string, error)
}

// NewKeyVaultSource builds the source from the spec. The vault name is
// expected in the spec's cloud section; empty is an error at use time.
func NewKeyVaultSource(spec *model.BootSpec) *KeyVaultSource {
	name := ""
	if spec != nil {
		// Vault name rides the Cloud override field (kept flexible so
		// the spec shape does not hard-code provider field names).
		name = os.Getenv("CHINIT_AZURE_VAULT_NAME")
	}
	return &KeyVaultSource{
		client:    http.DefaultClient,
		VaultName: name,
		TokenSource: func(ctx context.Context, resource string) (string, error) {
			return managedIdentityToken(ctx, http.DefaultClient, resource)
		},
	}
}

// GetSecretValue resolves one secret by name. The unused narrow stub
// shape was a mistake kept out — use GetSecretValueCtx.
func (s *KeyVaultSource) GetSecretValueCtx(ctx context.Context, name, field string) (string, error) {
	if s.VaultName == "" {
		return "", chinitsvc.Fatal(fmt.Errorf("azure Key Vault name not configured (CHINIT_AZURE_VAULT_NAME)"))
	}
	token, err := s.TokenSource(ctx, "https://vault.azure.net")
	if err != nil {
		return "", fmt.Errorf("managed identity token: %w", err)
	}

	url := fmt.Sprintf("https://%s.vault.azure.net/secrets/%s?api-version=7.4", s.VaultName, name)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("key vault secret %q: status %d", name, resp.StatusCode)
	}
	var out struct {
		Value string `json:"value"`
	}
	if err := jsonUnmarshal(body, &out); err != nil {
		return "", err
	}
	if out.Value == "" {
		return "", fmt.Errorf("key vault secret %q is empty", name)
	}
	return out.Value, nil
}

// managedIdentityToken mints an access token from the IMDS identity
// endpoint for the given resource.
func managedIdentityToken(ctx context.Context, client httpClient, resource string) (string, error) {
	const tokenURL = "http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01"
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		tokenURL+"&resource="+resource, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata", "true")
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("identity token: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := jsonUnmarshal(body, &out); err != nil {
		return "", err
	}
	if out.AccessToken == "" {
		return "", fmt.Errorf("identity token: empty access_token")
	}
	return out.AccessToken, nil
}

// BlobBundleSource fetches the config bundle from Blob Storage via the
// same managed-identity token (storage resource scope).
type BlobBundleSource struct {
	client      httpClient
	spec        *model.BootSpec
	TokenSource func(ctx context.Context, resource string) (string, error)
}

// NewBlobBundleSource builds the fetcher.
func NewBlobBundleSource(spec *model.BootSpec) *BlobBundleSource {
	return &BlobBundleSource{
		client:      http.DefaultClient,
		spec:         spec,
		TokenSource: func(ctx context.Context, resource string) (string, error) {
			return managedIdentityToken(ctx, http.DefaultClient, resource)
		},
	}
}

// FetchBundle downloads every bundle member. Blob download URLs need a
// SAS token or a bearer; the managed identity token covers the bearer
// path for the storage account the identity can read.
func (b *BlobBundleSource) FetchBundle(ctx context.Context, bundle model.ConfigBundle, destDir string) ([]string, error) {
	if err := os.MkdirAll(destDir, 0755); err != nil {
		return nil, chinitsvc.Fatal(fmt.Errorf("create %s: %w", destDir, err))
	}
	token, err := b.TokenSource(ctx, "https://storage.azure.com/")
	if err != nil {
		return nil, fmt.Errorf("managed identity token (storage): %w", err)
	}

	var written []string
	hasher := sha256.New()
	for _, name := range bundleFiles {
		url := fmt.Sprintf("https://%s.blob.core.windows.net/%s/%s",
			bundle.Bucket, strings.TrimPrefix(bundle.Prefix, "/"), name)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := b.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch %s: %w", url, err)
		}
		body, readErr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound && (name == "chi.yaml" || name == "keeper.yaml") {
			continue // single-role stacks omit the other spec
		}
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("fetch %s: status %d", url, resp.StatusCode)
		}
		if len(body) == 0 {
			return nil, fmt.Errorf("%s is empty", url)
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
				"config bundle SHA256 mismatch: want %s got %s — refusing to install", bundle.SHA256, got))
		}
	}
	return written, nil
}
