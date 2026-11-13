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

// Package operator adapts the operator's own model packages to the chctl
// domain ports. This is the only place in the chctl tree where operator
// machinery types appear; the domain and service layers never see them.
package operator

import (
	"context"
	"sync"

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	chkNormalizer "github.com/altinity/clickhouse-operator/pkg/model/chk/normalizer"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/common/normalizer/subst"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
)

var chopOnce sync.Once

// bootstrapChop initializes the global chop singleton with compiled-in
// defaults, exactly as the operator's own off-kube unit tests do
// (chop.New(nil, nil, "")).
//
// Why: the normalizer and config generators read global operator
// configuration via chop.Config(); without a non-nil singleton every
// normalization/render panics. In-repo callers may have bootstrapped it
// already, so this is idempotent.
//
// The operator's extra-config-file scan paths are then pinned to a
// non-existent directory. The default is "config.d" RELATIVE TO THE
// PROCESS CWD (normalizeSectionClickHouseConfigurationFile builds
// RelativeToBasePath(ConfigFolderPath, "config.d") — and with no config
// file the folder path is empty, so the path stays the bare relative
// name). On a node the cwd is / and the scan finds nothing — but a
// render whose -out IS ./config.d (the validate harness, a developer
// shell) would feed its own previous output back into the next render
// as operator extra files: nondeterministic output that grows per run
// and drift-check false positives. Pinning to a path that cannot exist
// makes the scan a no-op everywhere, preserving on-cluster behavior
// (there chCommonConfigsPath is set explicitly in the operator's
// config file).
func bootstrapChop() {
	chopOnce.Do(func() {
		chop.New(nil, nil, "")
		cfg := chop.Config()
		// The scan paths are pinned and the ALREADY-scanned Runtime
		// maps cleared: chop.New → Init → Postprocess reads them above
		// (see the function comment), so pinning paths alone would
		// leave this process's maps polluted.
		const inert = "/nonexistent-chctl-operator-extra-configs"
		cfg.ClickHouse.Config.File.Path.Common = inert
		cfg.ClickHouse.Config.File.Path.Host = inert
		cfg.ClickHouse.Config.File.Path.User = inert
		cfg.Keeper.Config.File.Path.Common = inert
		cfg.Keeper.Config.File.Path.Host = inert
		cfg.Keeper.Config.File.Path.User = inert
		cfg.ClickHouse.Config.File.Runtime = v1.OperatorConfigFileRuntime{}
		cfg.Keeper.Config.File.Runtime = v1.OperatorConfigFileRuntime{}
	})
}

// SpecLoader implements port.SpecLoader over the operator's normalizer.
type SpecLoader struct{}

// NewSpecLoader builds the operator-backed spec loader.
func NewSpecLoader() *SpecLoader {
	return &SpecLoader{}
}

// LoadCHI parses, defaults and runtime-wires a CHI spec document.
//
// Why: rendering configs and running schemer operations require a
// *normalized* CR — the raw parsed spec has nil runtime addresses and
// panics inside the config generator's host-naming lookups.
//
// Without it: every consumer would re-derive the normalizer bootstrap
// order and drift from the operator's on-cluster behavior.
func (l *SpecLoader) LoadCHI(ctx context.Context, request port.LoadCHIRequest) (any, error) {
	bootstrapChop()

	raw := &v1.ClickHouseInstallation{}
	if err := parseSpec(request.Spec.Data, raw); err != nil {
		return nil, err
	}
	if request.Spec.Name != "" {
		raw.Name = request.Spec.Name
	}
	if request.Spec.Namespace != "" {
		raw.Namespace = request.Spec.Namespace
	}

	// Off-cluster there is no Kubernetes secret fetcher; specs must not
	// rely on secretKeyRef resolution. Options must be non-nil: the
	// normalizer dereferences Options() in ensureSubject, matching the
	// controller's NewOptions[...]() wiring.
	var secretGet subst.SecretGetter
	options := commonNormalizer.NewOptions[v1.ClickHouseInstallation]()

	n := normalizer.New(secretGet)
	cr, err := n.CreateTemplated(raw, options)
	if err != nil {
		return nil, err
	}
	// Hard rejects (removed user-setting syntax, FIPS violations) are
	// reported via status, not error — the controller checks status the
	// same way. Surface them as load errors.
	if status := cr.EnsureStatus(); status.GetStatus() == v1.StatusAborted {
		return nil, &SpecRejectedError{Errors: status.GetErrors()}
	}
	return cr, nil
}

// LoadCHK parses, defaults and runtime-wires a CHK spec document.
func (l *SpecLoader) LoadCHK(ctx context.Context, request port.LoadCHKRequest) (any, error) {
	bootstrapChop()

	raw := &chk.ClickHouseKeeperInstallation{}
	if err := parseSpec(request.Spec.Data, raw); err != nil {
		return nil, err
	}
	if request.Spec.Name != "" {
		raw.Name = request.Spec.Name
	}
	if request.Spec.Namespace != "" {
		raw.Namespace = request.Spec.Namespace
	}

	n := chkNormalizer.New()
	options := commonNormalizer.NewOptions[chk.ClickHouseKeeperInstallation]()
	cr, err := n.CreateTemplated(raw, options)
	if err != nil {
		return nil, err
	}
	if status := cr.EnsureStatus(); status.GetStatus() == v1.StatusAborted {
		return nil, &SpecRejectedError{Errors: status.GetErrors()}
	}
	return cr, nil
}

// SpecRejectedError marks a spec the normalizer refused (e.g. the
// RemovedSecretRefSyntax hardening).
type SpecRejectedError struct {
	// Errors are the normalizer's status errors.
	Errors []string
}

// Error implements error.
func (e *SpecRejectedError) Error() string {
	return "spec rejected: " + joinErrors(e.Errors)
}
