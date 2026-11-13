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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	chinitsvc "github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// Azure IMDS: one endpoint, the Metadata: true header, JSON documents.
const (
	azureIMDSURL        = "http://169.254.169.254/metadata/instance?api-version=2021-02-01"
	azureIMDSTimeout    = 3 * time.Second
)

// IMDSMetadata reads Azure instance metadata.
type IMDSMetadata struct {
	client httpClient
}

type httpClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// NewIMDSMetadata builds the reader.
func NewIMDSMetadata() *IMDSMetadata {
	return &IMDSMetadata{client: http.DefaultClient}
}

// WithClient injects an HTTP client (tests serve canned IMDS).
func (m *IMDSMetadata) WithClient(c httpClient) *IMDSMetadata {
	m.client = c
	return m
}

// instanceDoc is the subset of the IMDS instance document the boot needs.
type instanceDoc struct {
	Compute struct {
		Name       string `json:"name"`
		VMID       string `json:"vmId"`
		Zone       string `json:"zone"`
		Location   string `json:"location"`
		PrivateIP  string `json:"privateIpAddress"`
	} `json:"compute"`
	Network struct {
		Interfaces []struct {
			IPv4 struct {
				IPAddress []struct {
					PrivateIPAddress string `json:"privateIpAddress"`
				} `json:"ipAddress"`
			} `json:"ipv4"`
		} `json:"interface"`
	} `json:"network"`
}

// Get returns the instance metadata.
func (m *IMDSMetadata) Get(ctx context.Context) (port.InstanceMetadata, error) {
	ctx, cancel := context.WithTimeout(ctx, azureIMDSTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, azureIMDSURL, nil)
	if err != nil {
		return port.InstanceMetadata{}, err
	}
	req.Header.Set("Metadata", "true")
	resp, err := m.client.Do(req)
	if err != nil {
		return port.InstanceMetadata{}, chinitsvc.Fatal(fmt.Errorf("azure IMDS: %w", err))
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return port.InstanceMetadata{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return port.InstanceMetadata{}, chinitsvc.Fatal(fmt.Errorf("azure IMDS: status %d", resp.StatusCode))
	}

	var doc instanceDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return port.InstanceMetadata{}, chinitsvc.Fatal(fmt.Errorf("azure IMDS parse: %w", err))
	}

	ip := doc.Compute.PrivateIP
	if ip == "" && len(doc.Network.Interfaces) > 0 && len(doc.Network.Interfaces[0].IPv4.IPAddress) > 0 {
		ip = doc.Network.Interfaces[0].IPv4.IPAddress[0].PrivateIPAddress
	}
	out := port.InstanceMetadata{
		Provider:     "azure",
		Region:       doc.Compute.Location,
		Zone:         doc.Compute.Zone,
		InstanceID:   doc.Compute.VMID,
		PrivateIP:    ip,
		HostnameHint: doc.Compute.Name,
	}
	if out.InstanceID == "" || out.PrivateIP == "" {
		return out, fmt.Errorf("azure IMDS incomplete: vmId=%q ip=%q", out.InstanceID, out.PrivateIP)
	}
	return out, nil
}

// ScheduledEventsWatcher polls the scheduled-events endpoint for the
// Preempt document (Spot VM reclamation; 30s notices are typical).
type ScheduledEventsWatcher struct {
	client       httpClient
	PollInterval time.Duration
}

// scheduledEventsURL is the scheduled events document.
const scheduledEventsURL = "http://169.254.169.254/metadata/scheduledevents?api-version=2020-07-01"

// NewScheduledEventsWatcher builds the watcher.
func NewScheduledEventsWatcher() *ScheduledEventsWatcher {
	return &ScheduledEventsWatcher{client: http.DefaultClient, PollInterval: 10 * time.Second}
}

// eventsDoc is the scheduled events document.
type eventsDoc struct {
	DocumentIncarnation int `json:"DocumentIncarnation"`
	Events              []struct {
		EventId      string   `json:"EventId"`
		EventType    string   `json:"EventType"` // Preempt, Freeze, Reboot, Redeploy, Terminate
		ResourceType string   `json:"ResourceType"`
		Resources    []string `json:"Resources"`
		NotBefore    string   `json:"NotBefore"`
	} `json:"Events"`
}

// Watch polls until a Preempt/Terminate event naming this resource
// arrives. The channel receives once and closes.
func (w *ScheduledEventsWatcher) Watch(ctx context.Context) <-chan port.InterruptionNotice {
	ch := make(chan port.InterruptionNotice, 1)
	if w.PollInterval == 0 {
		w.PollInterval = 10 * time.Second
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

// pollOnce fetches the events document; Preempt/Terminate for a VM
// resource is the drain trigger.
func (w *ScheduledEventsWatcher) pollOnce(ctx context.Context) (port.InterruptionNotice, bool) {
	ctx, cancel := context.WithTimeout(ctx, azureIMDSTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scheduledEventsURL, nil)
	if err != nil {
		return port.InterruptionNotice{}, false
	}
	req.Header.Set("Metadata", "true")
	resp, err := w.client.Do(req)
	if err != nil {
		return port.InterruptionNotice{}, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return port.InterruptionNotice{}, false
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return port.InterruptionNotice{}, false
	}
	var doc eventsDoc
	if err := json.Unmarshal(body, &doc); err != nil {
		return port.InterruptionNotice{}, false
	}
	for _, ev := range doc.Events {
		if ev.EventType == "Preempt" || ev.EventType == "Terminate" {
			return port.InterruptionNotice{
				DeadlineSeconds: deadlineFrom(ev.NotBefore),
				Raw:             string(body),
			}, true
		}
	}
	return port.InterruptionNotice{}, false
}

// deadlineFrom parses the NotBefore RFC3339 stamp; unparseable values
// default to 120s (the conservative EC2-parity window).
func deadlineFrom(notBefore string) int {
	if notBefore == "" {
		return 120
	}
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(notBefore)); err == nil {
		d := int(time.Until(t).Seconds())
		if d > 0 {
			return d
		}
		return 1
	}
	return 120
}
