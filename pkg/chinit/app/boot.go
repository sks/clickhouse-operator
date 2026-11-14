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

package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/service"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/aws"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/azure"
	"github.com/altinity/clickhouse-operator/pkg/chinit/infra/detect"
	"gopkg.in/yaml.v3"
)

// registry is the provider factory. Adding a cloud is one constructor
// here plus one infra package — nothing in the pipeline changes.
func registry() port.Registry {
	r := port.Registry{}
	r.Register(detect.AWSName, func(cfg port.Config) port.Provider { return aws.New(cfg) })
	r.Register(detect.AzureName, func(cfg port.Config) port.Provider { return azure.New(cfg) })
	return r
}

// runBoot is the `chinit boot` entry: load spec, detect cloud, build
// the provider, probe metadata, run the pipeline.
func runBoot(args []string) error {
	flags, err := parseBootFlags(args)
	if err != nil {
		return err
	}

	spec, err := loadSpec(flags.specPath)
	if err != nil {
		return fail.Fatal(err)
	}

	ctx := context.Background()

	// Detect the cloud (flag > env > DMI > IMDS) — the spec's cloud
	// field is an override at the same precedence as the flag.
	detected, err := detect.Detect(ctx, detect.Options{
		Flag:    firstNonEmpty(flags.cloud, spec.Cloud),
		EnvName: detect.EnvDetection(),
	})
	if err != nil {
		return fail.Fatal(fmt.Errorf("cloud detection: %w", err))
	}
	slog.Info("Detected cloud",
		"cloud", detected.Provider,
		"via", detected.Via,
		"detail", detected.Detail)
	fmt.Printf("Detected cloud: %s (via %s: %s)\n", detected.Provider, detected.Via, detected.Detail)

	// Build the provider. In dry-run the metadata probe is optional: the
	// plan prints without it (a dev machine has no IMDS); a real boot
	// requires it — the pipeline's region and identity derive from it.
	provider, err := registry().Build(detected.Provider, port.Config{
		Spec: spec,
	})
	if err != nil {
		return fail.Fatal(err)
	}
	meta, err := provider.Metadata(ctx)
	if err != nil {
		if !flags.dryRun {
			return fail.Fatal(fmt.Errorf("instance metadata: %w", err))
		}
		slog.Warn("NOTE: metadata unavailable — printing the plan with an empty identity", "err", err)
		fmt.Printf("NOTE: metadata unavailable (%s) — printing the plan with an empty identity\n", err)
		meta = port.InstanceMetadata{Provider: detected.Provider}
	}
	if spec.Region == "" {
		spec.Region = meta.Region
	}

	// The boot pipeline.
	boot := &service.Boot{
		Spec:     spec,
		Provider: provider,
		Meta:     meta,
		SpecHash: spec.ConfigBundle.SHA256,
	}
	steps, err := boot.Steps(ctx)
	if err != nil {
		return fail.Fatal(err)
	}

	if flags.dryRun {
		fmt.Println("Dry-run: the pipeline would run these steps in order:")
		for _, st := range steps {
			fmt.Printf("  %-24s [%s]\n", st.Name(), st.Criticality())
		}
		return nil
	}

	store, err := service.NewFileStateStore(flags.statePath)
	if err != nil {
		return fail.Fatal(err)
	}
	engine := service.NewEngine(nil, store)
	// The spec hash keys resume; the identity persister writes the
	// settled identity the moment the claim/derivation finishes.
	engine = engine.WithIdentity(nil, detected.Provider, spec.ConfigBundle.SHA256)
	boot.PersistIdentity = engine.IdentityPersister(detected.Provider)
	res := engine.Run(ctx, steps)

	service.FormatTimingSummary(os.Stdout, res)
	service.DescribeFailures(os.Stderr, res)

	if res.Fatality != nil {
		return &exitCodeError{Code: exitBootFailed, Err: res.Fatality}
	}
	return nil
}

// loadSpec reads and validates chinit.yaml.
func loadSpec(path string) (*model.BootSpec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w (the cloud-init user-data wrapper must fetch the bundle, or pass -spec)", path, err)
	}
	var spec model.BootSpec
	if err := yaml.Unmarshal(data, &spec); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	return &spec, nil
}

// firstNonEmpty returns the first non-empty string.
func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
