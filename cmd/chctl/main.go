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

// chctl is the off-cluster ClickHouse control CLI: it renders node configs
// and runs schema/replica/keeper-safety operations from a CHI/CHK spec,
// using the operator's own normalizer, config generators and schemer —
// the same code paths the operator uses on Kubernetes.
//
// See docs/chctl.md for what it is for and how to wire it into node
// bootstrap, config refresh and lifecycle hooks.
package main

import (
	"fmt"
	"os"

	"github.com/altinity/clickhouse-operator/pkg/chctl/app"
)

func main() {
	// Application entry point
	if err := app.Run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
