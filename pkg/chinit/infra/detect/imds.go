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

package detect

import (
	"context"
	"net/http"
	"time"
)

// IMDS endpoints. Both clouds use 169.254.169.254 with distinct
// authentication headers; a quick GET with a short timeout answers
// "am I on this platform?" without pulling credentials.
const (
	awsIMDSURL   = "http://169.254.169.254/latest/api/token"
	azureIMDSURL = "http://169.254.169.254/metadata/instance?api-version=2021-02-01"
)

// probeTimeout bounds each probe: detection must not hang a boot on a
// node whose network is not up yet (the engine retries the whole boot,
// but detection itself should be snappy).
const probeTimeout = 2 * time.Second

// probeAWSIMDS reports whether the EC2 IMDS answers. IMDSv2 requires the
// token PUT; a 404/403 from a non-EC2 host means "not AWS".
func probeAWSIMDS(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, awsIMDSURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "300")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK, nil
}

// probeAzureIMDS reports whether the Azure IMDS answers. Azure requires
// the Metadata: true header and returns 404 on other clouds.
func probeAzureIMDS(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, azureIMDSURL, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Metadata", "true")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer func() { _ = resp.Body.Close() }()
	return resp.StatusCode == http.StatusOK, nil
}
