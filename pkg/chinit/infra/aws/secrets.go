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

// Package aws implements chinit's cloud adapters for AWS: the Secrets
// Manager SecretSource and (future) the ASG lifecycle acker. All AWS
// specifics live here; the domain ports stay cloud-agnostic.
package aws

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// SecretsManagerSource implements port.SecretSource over AWS Secrets
// Manager, resolving via the instance role (IRSA on EKS, instance profile
// on EC2) — no static credentials ever.
type SecretsManagerSource struct {
	// Region pins the API endpoint; empty uses the SDK default chain
	// (AWS_REGION env / instance metadata).
	Region string
}

// NewSecretsManagerSource builds the AWS secret source.
func NewSecretsManagerSource(region string) *SecretsManagerSource {
	return &SecretsManagerSource{Region: region}
}

// GetSecretValue returns one secret value by name.
//
// Why: CHI specs carry valueFrom-style references (never plaintext); this
// resolves them at render/run time so passwords reach users.d without
// landing in Terraform state or spec files.
//
// Without it: specs would have to embed plaintext passwords, or the
// render path could not produce a complete users.d at all.
func (s *SecretsManagerSource) GetSecretValue(ctx context.Context, request port.SecretRequest) (string, error) {
	cfg, err := config.LoadDefaultConfig(ctx, func(opts *config.LoadOptions) error {
		if s.Region != "" {
			opts.Region = s.Region
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("load AWS config: %w", err)
	}
	client := secretsmanager.NewFromConfig(cfg)

	out, err := client.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{
		SecretId: &request.Name,
	})
	if err != nil {
		return "", fmt.Errorf("get secret %q: %w", request.Name, err)
	}
	if out.SecretString == nil {
		return "", fmt.Errorf("secret %q has no string value (binary secrets unsupported)", request.Name)
	}
	value := *out.SecretString
	// Structured secrets: pick the requested field from the JSON body.
	if request.Field != "" {
		var fields map[string]string
		if err := json.Unmarshal([]byte(value), &fields); err != nil {
			return "", fmt.Errorf("secret %q is not JSON, cannot pick field %q", request.Name, request.Field)
		}
		field, ok := fields[request.Field]
		if !ok {
			return "", fmt.Errorf("secret %q has no field %q", request.Name, request.Field)
		}
		return field, nil
	}
	return value, nil
}
