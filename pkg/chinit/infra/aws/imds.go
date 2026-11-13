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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// IMDS endpoints (v2: token PUT first, token-gated GETs — IMDSv1 is
// disabled fleet-wide per the security standard).
const (
	imdsTokenURL  = "http://169.254.169.254/latest/api/token"
	imdsBaseURL   = "http://169.254.169.254/latest/meta-data/"
	imdsSpotURL   = "http://169.254.169.254/latest/meta-data/spot/instance-action"
	tokenTTL      = "300"
	imdsTimeout   = 2 * time.Second
)

// httpClient abstracts the IMDS HTTP surface for tests.
type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// IMDSMetadata reads EC2 instance metadata over IMDSv2.
type IMDSMetadata struct {
	client httpClient
}

// NewIMDSMetadata builds the metadata reader with the default client.
func NewIMDSMetadata() *IMDSMetadata {
	return &IMDSMetadata{client: http.DefaultClient}
}

// WithClient injects an HTTP client (tests serve canned IMDS).
func (m *IMDSMetadata) WithClient(c httpClient) *IMDSMetadata {
	m.client = c
	return m
}

// mdField is one IMDS document path → field name in the result.
var mdFields = []struct{ path, hint string }{
	{"instance-id", ""},
	{"instance-type", ""},
	{"placement/region", ""},
	{"placement/availability-zone", ""},
	{"local-ipv4", ""},
	{"hostname", ""},
}

// Get returns the instance metadata. The token is fetched per call (300s
// TTL cached by callers that care; the boot calls Get once and the spot
// watcher mints its own).
func (m *IMDSMetadata) Get(ctx context.Context) (port.InstanceMetadata, error) {
	// Bound the whole metadata fetch: a network-less host (dev, a
	// paused instance) must fail fast so the retry policy owns the
	// pacing — the OS connect timeout is ~30s and would swallow it.
	ctx, cancel := context.WithTimeout(ctx, imdsTimeout)
	defer cancel()

	token, err := m.token(ctx)
	if err != nil {
		return port.InstanceMetadata{}, chinitsvc.Fatal(fmt.Errorf("IMDS token: %w", err))
	}

	out := port.InstanceMetadata{}
	for _, f := range mdFields {
		val, err := m.get(ctx, token, f.path)
		if err != nil {
			// Missing optional fields (spot instances lack some) are
			// skipped; the mandatory ones error below on emptiness.
			continue
		}
		switch f.path {
		case "instance-id":
			out.InstanceID = val
		case "placement/region":
			out.Region = val
		case "placement/availability-zone":
			out.Zone = val
		case "local-ipv4":
			out.PrivateIP = val
		case "hostname":
			out.HostnameHint = val
		}
	}
	if out.InstanceID == "" || out.PrivateIP == "" {
		return out, fmt.Errorf("IMDS incomplete: instance-id=%q private-ip=%q", out.InstanceID, out.PrivateIP)
	}
	return out, nil
}

// token mints an IMDSv2 session token.
func (m *IMDSMetadata) token(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, imdsTokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", tokenTTL)
	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IMDS token request: status %d", resp.StatusCode)
	}
	return string(body), nil
}

// get fetches one metadata path with the token.
func (m *IMDSMetadata) get(ctx context.Context, token, path string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imdsBaseURL+path, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token", token)
	resp, err := m.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IMDS GET %s: status %d", path, resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

// SpotWatcher polls the spot interruption endpoint and delivers the
// 2-minute notice exactly once.
type SpotWatcher struct {
	// client is injectable (tests serve canned responses).
	client httpClient
	// PollInterval bounds the poll cadence; the bash loop slept 5s.
	PollInterval time.Duration
}

// NewSpotWatcher builds the watcher with the default poll cadence.
func NewSpotWatcher() *SpotWatcher {
	return &SpotWatcher{client: http.DefaultClient, PollInterval: 5 * time.Second}
}

// spotAction is the IMDS instance-action document: {"action": "terminate", "time": "..."}.
type spotAction struct {
	Action string `json:"action"`
	Time   string `json:"time"`
}

// Watch polls until the notice arrives or the context ends. The channel
// receives once and closes — the drain is a one-shot, and after it the
// watcher has nothing left to say.
func (w *SpotWatcher) Watch(ctx context.Context) <-chan port.InterruptionNotice {
	ch := make(chan port.InterruptionNotice, 1)
	if w.PollInterval == 0 {
		w.PollInterval = 5 * time.Second
	}
	go func() {
		defer close(ch)
		for {
			if notice, ok := w.pollOnce(ctx); ok {
				ch <- notice
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.PollInterval):
			}
		}
	}()
	return ch
}

// pollOnce checks the endpoint: 200 carries the action document; 404 is
// "no notice" (the normal case); anything else is retried next tick.
func (w *SpotWatcher) pollOnce(ctx context.Context) (port.InterruptionNotice, bool) {
	token, err := w.token(ctx)
	if err != nil {
		return port.InterruptionNotice{}, false
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imdsSpotURL, nil)
	if err != nil {
		return port.InterruptionNotice{}, false
	}
	req.Header.Set("X-aws-ec2-metadata-token", token)
	resp, err := w.client.Do(req)
	if err != nil {
		return port.InterruptionNotice{}, false
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return port.InterruptionNotice{}, false
	}
	var action spotAction
	if err := json.Unmarshal(body, &action); err != nil {
		// The raw payload still drains the node; parse failures
		// conservatively use the 120s EC2 contract.
		return port.InterruptionNotice{DeadlineSeconds: 120, Raw: string(body)}, true
	}
	return port.InterruptionNotice{
		DeadlineSeconds: w.deadlineFrom(action.Time),
		Raw:            string(body),
	}, true
}

// deadlineFrom computes the remaining window from the action's epoch
// time; unparseable values fall back to the EC2 2-minute contract.
func (w *SpotWatcher) deadlineFrom(actionTime string) int {
	if actionTime == "" {
		return 120
	}
	// EC2 quotes unix seconds.
	if secs, err := strconv.ParseInt(actionTime, 10, 64); err == nil {
		d := int(secs - time.Now().Unix())
		if d > 0 {
			return d
		}
		return 1
	}
	// RFC3339 fallback.
	if t, err := time.Parse(time.RFC3339, actionTime); err == nil {
		d := int(time.Until(t).Seconds())
		if d > 0 {
			return d
		}
		return 1
	}
	return 120
}

// token mints the watcher's own IMDSv2 token (refreshed with the poll).
func (w *SpotWatcher) token(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, imdsTokenURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", tokenTTL)
	resp, err := w.client.Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IMDS token: status %d", resp.StatusCode)
	}
	return string(body), nil
}
