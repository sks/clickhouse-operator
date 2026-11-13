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

package main

import (
	"os"

	"github.com/altinity/clickhouse-operator/pkg/chinit/app"
)

// chinit is the cloud-adaptive ClickHouse node bootstrapper: it
// replaces the cloud-init bash script with a testable Go binary that
// detects the cloud it runs on (AWS/Azure; the factory registry makes
// GCP/Civo follow-ups) and converges the node to its BootSpec with
// state-aware retries. Recurring maintenance jobs (schema-sync,
// replica-sync, lease-renew, spot-watch, …) are subcommands so the
// systemd units chinit installs are one ExecStart line.
func main() {
	app.Main(os.Args[1:])
}
