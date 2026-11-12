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

package operator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	chk "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse-keeper.altinity.com/v1"
	v1 "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	chiConfig "github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chkConfig "github.com/altinity/clickhouse-operator/pkg/model/chk/config"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
)

// ConfigRenderer implements port.ConfigRenderer over the operator's config
// file generators — the same code that produces ConfigMaps on Kubernetes.
type ConfigRenderer struct{}

// NewConfigRenderer builds the operator-backed renderer.
func NewConfigRenderer() *ConfigRenderer {
	return &ConfigRenderer{}
}

// RenderCHI writes the common, users and (when a host is selected) host
// config files for a normalized CHI.
//
// Why the generator (not hand-written templates): the operator's normalizer
// defaults, section naming, and user hardening apply identically
// off-cluster, and any generator improvement is inherited automatically.
//
// Without it: every deployment hand-maintains its own XML templates, which
// drift from ClickHouse LTS behavior and from each other.
func (r *ConfigRenderer) RenderCHI(ctx context.Context, request port.RenderCHIRequest) ([]port.RenderedFile, error) {
	bootstrapChop()

	cr, ok := request.NormalizedCR.(*v1.ClickHouseInstallation)
	if !ok || cr == nil {
		return nil, fmt.Errorf("normalized CR is not a ClickHouseInstallation")
	}
	if request.OutputDir == "" {
		return nil, fmt.Errorf("output dir is empty")
	}

	options := chiGeneratorOptions(cr)
	filesGenerator := managers.NewConfigFilesGenerator(
		managers.FilesGeneratorTypeClickHouse,
		cr,
		options,
	)

	rendered := make([]port.RenderedFile, 0, 16)

	commonFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupCommon, chiConfig.NewFilesGeneratorOptions())
	rendered = append(rendered, toRenderedFiles("common", commonFiles)...)

	usersFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupUsers)
	rendered = append(rendered, toRenderedFiles("users", usersFiles)...)

	if request.HostName != "" {
		host, err := findHost(cr, request.HostName)
		if err != nil {
			return nil, err
		}
		hostOptions := chiConfig.NewFilesGeneratorOptions().SetHost(host)
		hostFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupHost, hostOptions)
		rendered = append(rendered, toRenderedFiles("host", hostFiles)...)
	}

	if !request.DryRun {
		if err := writeFiles(rendered, request.OutputDir, request.UsersDir); err != nil {
			return nil, err
		}
	}

	return rendered, nil
}

// RenderCHK writes the common and host config files for a normalized CHK.
//
// Why: keeper nodes need identical config discipline (raft, TLS,
// prometheus endpoint); reusing the keeper generator keeps keeper configs
// on the same single-source footing.
func (r *ConfigRenderer) RenderCHK(ctx context.Context, request port.RenderCHKRequest) ([]port.RenderedFile, error) {
	bootstrapChop()

	cr, ok := request.NormalizedCR.(*chk.ClickHouseKeeperInstallation)
	if !ok || cr == nil {
		return nil, fmt.Errorf("normalized CR is not a ClickHouseKeeperInstallation")
	}
	if request.OutputDir == "" {
		return nil, fmt.Errorf("output dir is empty")
	}

	filesGenerator := managers.NewConfigFilesGenerator(
		managers.FilesGeneratorTypeKeeper,
		cr,
		chkGeneratorOptions(cr),
	)

	rendered := make([]port.RenderedFile, 0, 8)

	commonFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupCommon, chkConfig.NewFilesGeneratorOptions())
	rendered = append(rendered, toRenderedFiles("common", commonFiles)...)

	if request.HostName != "" {
		host, err := findKeeperHost(cr, request.HostName)
		if err != nil {
			return nil, err
		}
		hostOptions := chkConfig.NewFilesGeneratorOptions().SetHost(host)
		hostFiles := filesGenerator.CreateConfigFiles(interfaces.FilesGroupHost, hostOptions)
		rendered = append(rendered, toRenderedFiles("host", hostFiles)...)
	}

	if !request.DryRun {
		if err := writeFiles(rendered, request.OutputDir, ""); err != nil {
			return nil, err
		}
	}

	return rendered, nil
}

// chiGeneratorOptions mirrors the CHI controller's configGeneratorOptions:
// users/profiles/quotas/settings/files flow from the CR's configuration
// section into the generator.
//
// Why: skipping any field silently drops whole spec sections from the
// rendered files; the controller's own wiring is the reference.
func chiGeneratorOptions(cr *v1.ClickHouseInstallation) *chiConfig.GeneratorOptions {
	return &chiConfig.GeneratorOptions{
		Users:          cr.GetSpecT().GetConfiguration().GetUsers(),
		Profiles:       cr.GetSpecT().GetConfiguration().GetProfiles(),
		Quotas:         cr.GetSpecT().GetConfiguration().GetQuotas(),
		Settings:       cr.GetSpecT().GetConfiguration().GetSettings(),
		Files:          cr.GetSpecT().GetConfiguration().GetFiles(),
		DistributedDDL: cr.GetSpecT().GetDefaults().GetDistributedDDL(),
	}
}

// chkGeneratorOptions mirrors the CHK controller's configGeneratorOptions.
func chkGeneratorOptions(cr *chk.ClickHouseKeeperInstallation) *chkConfig.GeneratorOptions {
	return &chkConfig.GeneratorOptions{
		Settings: cr.GetSpecT().GetConfiguration().GetSettings(),
		Files:    cr.GetSpecT().GetConfiguration().GetFiles(),
	}
}

// findHost locates a host by name within a CHI.
//
// WalkHosts appends the closure's return value per host, so a clean walk
// still yields a []error of nils; filter them before treating the slice as
// failures.
func findHost(cr *v1.ClickHouseInstallation, name string) (*v1.Host, error) {
	var found *v1.Host
	errs := cr.WalkHosts(func(host *v1.Host) error {
		if host.Runtime.Address.HostName == name {
			found = host
		}
		return nil
	})
	if err := firstErr(errs); err != nil {
		return nil, fmt.Errorf("walk hosts: %v", err)
	}
	if found == nil {
		return nil, fmt.Errorf("host %q not found in CHI %q", name, cr.Name)
	}
	return found, nil
}

// findKeeperHost locates a keeper host by name within a CHK. Keeper CRs
// reuse the shared v1.Host type.
func findKeeperHost(cr *chk.ClickHouseKeeperInstallation, name string) (*v1.Host, error) {
	var found *v1.Host
	errs := cr.WalkHosts(func(host *v1.Host) error {
		if host.Runtime.Address.HostName == name {
			found = host
		}
		return nil
	})
	if err := firstErr(errs); err != nil {
		return nil, fmt.Errorf("walk keeper hosts: %v", err)
	}
	if found == nil {
		return nil, fmt.Errorf("keeper host %q not found in CHK %q", name, cr.Name)
	}
	return found, nil
}

// firstErr returns the first non-nil error, or nil.
func firstErr(errs []error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// toRenderedFiles maps generator output (filename → content) to
// RenderedFile, deterministically ordered so output stays stable across
// map iterations.
func toRenderedFiles(group string, files map[string]string) []port.RenderedFile {
	rendered := make([]port.RenderedFile, 0, len(files))
	for name, content := range files {
		rendered = append(rendered, port.RenderedFile{
			Group:   group,
			Path:    name,
			Content: content,
		})
	}
	sort.Slice(rendered, func(i, j int) bool {
		return rendered[i].Path < rendered[j].Path
	})
	return rendered
}

// writeFiles writes rendered files to disk. users-group files go to
// UsersDir (default OutputDir) so callers can keep config.d and users.d
// apart.
func writeFiles(rendered []port.RenderedFile, outputDir, usersDir string) error {
	if usersDir == "" {
		usersDir = outputDir
	}
	for _, file := range rendered {
		dir := outputDir
		if file.Group == "users" {
			dir = usersDir
		}
		path := filepath.Join(dir, filepath.Base(file.Path))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return fmt.Errorf("mkdir %s: %w", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(file.Content), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", path, err)
		}
	}
	return nil
}
