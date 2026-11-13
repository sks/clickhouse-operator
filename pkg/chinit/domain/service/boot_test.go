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

package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// validSpec is the baseline every mutation breaks one field at a time.
func validSpec() model.BootSpec {
	return model.BootSpec{
		Role:         model.RoleWriter,
		IdentityMode: model.IdentityStatic,
		ClusterName:  "logexplorer",
		ConfigBundle: model.ConfigBundle{Bucket: "data-bucket", Prefix: "bootstrap"},
		Disks:        model.Disks{MetadataLabel: "ch-metadata"},
		Secrets:      model.Secrets{DefaultPassword: "default-pw", AdminPassword: "admin-pw"},
	}
}

// TestSpecValidate_AcceptsBaseline: the valid spec passes.
func TestSpecValidate_AcceptsBaseline(t *testing.T) {
	spec := validSpec()
	require.NoError(t, spec.Validate())
}

// TestSpecValidate_RejectsBadShapes: table-driven invalid specs, each
// naming its field in the SpecError.
func TestSpecValidate_RejectsBadShapes(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*model.BootSpec)
		field  string
	}{
		{"unknown role", func(s *model.BootSpec) { s.Role = "coordinator" }, "role"},
		{"missing role", func(s *model.BootSpec) { s.Role = "" }, "role"},
		{"missing identity mode", func(s *model.BootSpec) { s.IdentityMode = "" }, "identity_mode"},
		{"unknown identity mode", func(s *model.BootSpec) { s.IdentityMode = "dns" }, "identity_mode"},
		{
			"lease reader without count",
			func(s *model.BootSpec) {
				s.Role = model.RoleReader
				s.IdentityMode = model.IdentityLease
				s.ReaderCount = 0
			},
			"reader_count",
		},
		{"no bucket", func(s *model.BootSpec) { s.ConfigBundle.Bucket = "" }, "config_bundle.bucket"},
		{"no metadata label", func(s *model.BootSpec) { s.Disks.MetadataLabel = "" }, "disks.metadata_label"},
		{"no cluster name", func(s *model.BootSpec) { s.ClusterName = "" }, "cluster_name"},
		{"no default secret", func(s *model.BootSpec) { s.Secrets.DefaultPassword = "" }, "secrets.default_password"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := validSpec()
			tc.mutate(&spec)
			err := spec.Validate()
			require.Error(t, err)
			var specErr *model.SpecError
			require.ErrorAs(t, err, &specErr)
			require.Equal(t, tc.field, specErr.Field)
		})
	}
}

// TestStaticHostname_OctetMap: the fixed-offset contract (writers
// +10..19, keepers +20..29, backup +30) — the keeper server_id garbage
// bug lives here if the map drifts.
func TestStaticHostname_OctetMap(t *testing.T) {
	cases := []struct {
		ip       string
		hostname string
		ordinal  int
	}{
		{"10.0.1.10", "writer-0", 0},
		{"10.0.1.19", "writer-9", 9},
		{"10.0.1.20", "keeper-0", 0},
		{"10.0.1.29", "keeper-9", 9},
		{"10.0.1.30", "backup-0", 0},
		{"10.0.1.5", "", -1},   // outside every role band
		{"10.0.1.31", "", -1},  // beyond backup
		{"not-an-ip", "", -1},  // unparseable
	}
	for _, tc := range cases {
		t.Run(tc.ip, func(t *testing.T) {
			hostname, ordinal := StaticHostname(tc.ip)
			require.Equal(t, tc.hostname, hostname)
			require.Equal(t, tc.ordinal, ordinal)
		})
	}
}

// TestCRName_RoleShapes: writers render 00-N, readers 01-N, backup is
// out of the CR (empty name — the signal every consumer keys on).
func TestCRName_RoleShapes(t *testing.T) {
	mk := func(role model.Role, ordinal int) *Boot {
		b := &Boot{Spec: &model.BootSpec{Role: role}}
		b.Identity.CROrdinal = ordinal
		return b
	}
	name, ordinal := mk(model.RoleWriter, 3).CRName()
	require.Equal(t, "00-3", name)
	require.Equal(t, 3, ordinal)

	name, ordinal = mk(model.RoleReader, 1).CRName()
	require.Equal(t, "01-1", name)
	require.Equal(t, 1, ordinal)

	name, _ = mk(model.RoleBackup, 0).CRName()
	require.Empty(t, name, "the backup replica is not a CR member")

	name, _ = mk(model.RoleWriter, -1).CRName()
	require.Empty(t, name, "an unset ordinal must not render a bogus CR name")
}
