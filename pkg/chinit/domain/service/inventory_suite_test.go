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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// TestInventorySuite is the entrypoint for the service package's
// Ginkgo specs: the live-claims inventory suite (inventory_test.go)
// and the writer-ASG identity/ceiling suite (ceiling_test.go). One
// RunSpecs per package — Ginkgo forbids a second call.
func TestInventorySuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Chinit Service Suite")
}
