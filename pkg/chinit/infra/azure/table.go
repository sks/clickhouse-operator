// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file or in compliance with the License.
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// bundleFiles mirrors the AWS provider's closed bundle member list (the
// Terraform bundle layout is identical across clouds).
var bundleFiles = []string{
	"network.xml",
	"ssl.xml",
	"storage.xml",
	"chi.yaml",
	"keeper.yaml",
}

// jsonUnmarshal is json.Unmarshal (single import point for the small
// files in this package).
func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// TableClaimer implements the ordinal lease over Azure Table Storage.
//
// Why not an SDK client: the table service is plain REST with a shared
// key or bearer; the conditional-put semantics Azure offers
// (If-Match: *) replace DynamoDB's condition expression, and one small
// HTTP surface keeps the provider testable with an injectable client.
type TableClaimer struct {
	client      httpClient
	accountName string
	TokenSource func(ctx context.Context, resource string) (string, error)
}

// NewTableClaimer builds the claimer. The storage account arrives via
// environment (the spec's config bucket IS the storage account in the
// Azure shape; the lease table uses the same account).
func NewTableClaimer(_ *model.BootSpec) *TableClaimer {
	account := strings.TrimSpace(os.Getenv("CHINIT_AZURE_STORAGE_ACCOUNT"))
	return &TableClaimer{
		client:      http.DefaultClient,
		accountName: account,
		TokenSource: func(ctx context.Context, resource string) (string, error) {
			return managedIdentityToken(ctx, http.DefaultClient, resource)
		},
	}
}

// leaseEntity is the table row: PartitionKey "readers", RowKey the
// hostname; lease_until is the TTL epoch; instance_id guards renewal.
type leaseEntity struct {
	PartitionKey string `json:"PartitionKey"`
	RowKey       string `json:"RowKey"`
	InstanceID   string `json:"instance_id"`
	IP           string `json:"ip,omitempty"`
	LeaseUntil   int64  `json:"lease_until"`
}

// Claim sweeps ordinals 0..count-1 with conditional inserts (If-None-Match
// on the entity, plus an expired-lease takeover), bounded by the deadline.
func (c *TableClaimer) Claim(ctx context.Context, req chinitport.ClaimRequest) (int, error) {
	if c.accountName == "" {
		return -1, chinitsvc.Fatal(fmt.Errorf("azure storage account not configured (CHINIT_AZURE_STORAGE_ACCOUNT)"))
	}
	deadline := time.Now().Add(time.Duration(req.DeadlineSeconds) * time.Second)
	for time.Now().Before(deadline) {
		for ordinal := 0; ordinal < req.Count; ordinal++ {
			hostname := req.Prefix + strconv.Itoa(ordinal)
			// Existing expired lease → DELETE then insert (Azure has no
			// conditional expression on properties; the takeover is a
			// two-step: read, check expiry, delete, insert).
			if c.takeExpired(ctx, req.Table, hostname) {
				if c.insert(ctx, req, hostname, ordinal) {
					return ordinal, nil
				}
				continue
			}
			if c.insertIfAbsent(ctx, req, hostname, ordinal) {
				return ordinal, nil
			}
		}
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-time.After(60 * time.Second):
		}
	}
	return -1, fmt.Errorf("failed to claim a reader ordinal after one lease window (all %d taken). Increase reader_count", req.Count)
}

// entityURL builds one entity's URL.
func (c *TableClaimer) entityURL(table, partition, rowKey string) string {
	return fmt.Sprintf("https://%s.table.core.windows.net/%s(PartitionKey='%s',RowKey='%s')",
		c.accountName, table, partition, rowKey)
}

// insertIfAbsent inserts with If-None-Match: * (fails when the entity
// exists — Azure's answer to DynamoDB's attribute_not_exists).
func (c *TableClaimer) insertIfAbsent(ctx context.Context, req chinitport.ClaimRequest, hostname string, ordinal int) bool {
	return c.putEntity(ctx, req.Table, hostname, true, leaseEntity{
		PartitionKey: "readers",
		RowKey:       hostname,
		InstanceID:   req.InstanceID,
		IP:           req.PrivateIP,
		LeaseUntil:   time.Now().Unix() + int64(req.LeaseSeconds),
	})
}

// insert writes without the absence condition (takeover after delete).
func (c *TableClaimer) insert(ctx context.Context, req chinitport.ClaimRequest, hostname string, ordinal int) bool {
	return c.putEntity(ctx, req.Table, hostname, false, leaseEntity{
		PartitionKey: "readers",
		RowKey:       hostname,
		InstanceID:   req.InstanceID,
		IP:           req.PrivateIP,
		LeaseUntil:   time.Now().Unix() + int64(req.LeaseSeconds),
	})
}

// putEntity performs the MERGE/INSERT call.
func (c *TableClaimer) putEntity(ctx context.Context, table, hostname string, ifNoneMatch bool, entity leaseEntity) bool {
	body, err := json.Marshal(entity)
	if err != nil {
		return false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("https://%s.table.core.windows.net/%s", c.accountName, table),
		bytes.NewReader(body))
	if err != nil {
		return false
	}
	token, err := c.TokenSource(ctx, "https://storage.azure.com/")
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json;odata=nometadata")
	req.Header.Set("Prefer", "return-no-content")
	if ifNoneMatch {
		req.Header.Set("If-None-Match", "*")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusNoContent
}

// takeExpired removes a lease that has expired, enabling the insert.
// The read is a plain GET; expiry check is local; the DELETE uses
// If-Match on the read's ETag so a concurrent renewal wins.
func (c *TableClaimer) takeExpired(ctx context.Context, table, hostname string) bool {
	url := c.entityURL(table, "readers", hostname)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	token, err := c.TokenSource(ctx, "https://storage.azure.com/")
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json;odata=nometadata")
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	body, _ := io.ReadAll(resp.Body)
	etag := strings.Trim(resp.Header.Get("ETag"), `"` )
	_ = resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false // absent: insertIfAbsent handles it
	}
	if resp.StatusCode != http.StatusOK {
		return false
	}
	var entity leaseEntity
	if err := jsonUnmarshal(body, &entity); err != nil {
		return false
	}
	if entity.LeaseUntil >= time.Now().Unix() {
		return false // still leased
	}
	// DELETE with If-Match so a concurrent renewer keeps its lease.
	del, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return false
	}
	del.Header.Set("Authorization", "Bearer "+token)
	if etag != "" {
		del.Header.Set("If-Match", `"`+etag+`"`)
	}
	dresp, err := c.client.Do(del)
	if err != nil {
		return false
	}
	defer func() { _ = dresp.Body.Close() }()
	return dresp.StatusCode == http.StatusNoContent
}

// Renew extends the holder's lease; the instance_id check is a
// read-compare-write guarded by the ETag (same race semantics as the
// DynamoDB condition).
func (c *TableClaimer) Renew(ctx context.Context, req chinitport.RenewRequest) error {
	if c.accountName == "" {
		return chinitsvc.Fatal(fmt.Errorf("azure storage account not configured"))
	}
	url := c.entityURL(req.Table, "readers", req.Hostname)
	get, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	token, err := c.TokenSource(ctx, "https://storage.azure.com/")
	if err != nil {
		return err
	}
	get.Header.Set("Authorization", "Bearer "+token)
	get.Header.Set("Accept", "application/json;odata=nometadata")
	resp, err := c.client.Do(get)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(resp.Body)
	etag := strings.Trim(resp.Header.Get("ETag"), `"`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("lease entity %q: status %d", req.Hostname, resp.StatusCode)
	}
	var entity leaseEntity
	if err := jsonUnmarshal(body, &entity); err != nil {
		return err
	}
	if entity.InstanceID != req.InstanceID {
		return fmt.Errorf("lease %q is held by instance %q, not %q — refusing to renew a stolen ordinal", req.Hostname, entity.InstanceID, req.InstanceID)
	}
	entity.LeaseUntil = time.Now().Unix() + int64(req.LeaseSeconds)
	patch, err := json.Marshal(entity)
	if err != nil {
		return err
	}
	mer, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(patch))
	if err != nil {
		return err
	}
	mer.Header.Set("Authorization", "Bearer "+token)
	mer.Header.Set("Content-Type", "application/json")
	mer.Header.Set("If-Match", `"`+etag+`"`)
	mresp, err := c.client.Do(mer)
	if err != nil {
		return err
	}
	defer func() { _ = mresp.Body.Close() }()
	if mresp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("renew %q: status %d", req.Hostname, mresp.StatusCode)
	}
	return nil
}

// Release deletes the node's lease (drain/termination).
func (c *TableClaimer) Release(ctx context.Context, req chinitport.ReleaseRequest) error {
	if c.accountName == "" {
		return chinitsvc.Fatal(fmt.Errorf("azure storage account not configured"))
	}
	url := c.entityURL(req.Table, "readers", req.Hostname)
	del, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	token, err := c.TokenSource(ctx, "https://storage.azure.com/")
	if err != nil {
		return err
	}
	del.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.client.Do(del)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("release %q: status %d", req.Hostname, resp.StatusCode)
	}
	return nil
}
