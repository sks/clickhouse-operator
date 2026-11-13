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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// PrivateDNS manages the node's A record in an Azure Private DNS zone
// via the ARM REST surface (the same API az network dns record-set
// called; the bearer is the managed identity token for ARM).
type PrivateDNS struct {
	client      httpClient
	TokenSource func(ctx context.Context, resource string) (string, error)
}

// NewPrivateDNS builds the registrar.
func NewPrivateDNS(_ *model.BootSpec) *PrivateDNS {
	return &PrivateDNS{
		client: http.DefaultClient,
		TokenSource: func(ctx context.Context, resource string) (string, error) {
			return managedIdentityToken(ctx, http.DefaultClient, resource)
		},
	}
}

// armScope is the ARM resource-id template: subscription/resource-group
// arrive via environment (the spec keeps provider-neutral; the wrapper
// user-data can pin them for Azure).
func armScope(zoneName string) (string, error) {
	sub := strings.TrimSpace(os.Getenv("CHINIT_AZURE_SUBSCRIPTION_ID"))
	rg := strings.TrimSpace(os.Getenv("CHINIT_AZURE_RESOURCE_GROUP"))
	if sub == "" || rg == "" {
		return "", fmt.Errorf("azure ARM scope not configured (CHINIT_AZURE_SUBSCRIPTION_ID, CHINIT_AZURE_RESOURCE_GROUP)")
	}
	return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/privateDnsZones/%s",
		sub, rg, zoneName), nil
}

// ResolveZoneID returns the ARM id of the private DNS zone by name.
// Azure's ARM id IS the zone id (Route53 needed a list+match; here the
// scope is deterministic once subscription/rg are known).
func (p *PrivateDNS) ResolveZoneID(ctx context.Context, zoneName string) (string, error) {
	return armScope(zoneName)
}

// recordSetDoc is the PUT body for the A record set.
type recordSetDoc struct {
	Properties struct {
		TTL             int    `json:"ttl"`
		ARecords        []aRec `json:"aRecords"`
	} `json:"properties"`
}

type aRec struct {
	IPv4Address string `json:"ipv4Address"`
}

// UpsertARecord creates or replaces the node's A record (ARM PUT on the
// record-set path is create-or-replace by construction).
func (p *PrivateDNS) UpsertARecord(ctx context.Context, req chinitport.ARecordRequest) error {
	if req.TTL == 0 {
		req.TTL = 60
	}
	scope, err := armScope(req.ZoneName)
	if err != nil {
		return err
	}
	var doc recordSetDoc
	doc.Properties.TTL = req.TTL
	doc.Properties.ARecords = []aRec{{IPv4Address: req.IP}}
	body, err := json.Marshal(doc)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://management.azure.com%s/A/%s?api-version=2020-06-01", scope, req.RecordName)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	token, err := p.TokenSource(ctx, "https://management.azure.com/")
	if err != nil {
		return fmt.Errorf("managed identity token (ARM): %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upsert A %q: status %d", req.RecordName, resp.StatusCode)
	}
	return nil
}

// DeleteARecord removes the node's A record (drain/termination).
func (p *PrivateDNS) DeleteARecord(ctx context.Context, req chinitport.ARecordRequest) error {
	scope, err := armScope(req.ZoneName)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://management.azure.com%s/A/%s?api-version=2020-06-01", scope, req.RecordName)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	token, err := p.TokenSource(ctx, "https://management.azure.com/")
	if err != nil {
		return fmt.Errorf("managed identity token (ARM): %w", err)
	}
	httpReq.Header.Set("Authorization", "Bearer "+token)
	resp, err := p.client.Do(httpReq)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete A %q: status %d", req.RecordName, resp.StatusCode)
	}
	return nil
}
