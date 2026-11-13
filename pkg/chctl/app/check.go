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
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/altinity/clickhouse-operator/pkg/chctl/domain/port"
)

// Per-file verdicts of `config render -check`. The three states are the
// whole contract: a file is either exactly what the render would
// install, different, or not installed at all — and the missing case
// is DRIFT, not silence (a deleted config file is as much drift as an
// edited one, and treating it otherwise would make a wiped config.d
// directory report "in sync").
const (
	verdictInSync  = "IN SYNC"
	verdictDrifted = "DRIFTED"
	verdictMissing = "MISSING"
)

// fileVerdict is one file's -check result.
type fileVerdict struct {
	// Path is the installed destination the file would be written to
	// (outDir — or usersDir for the users group — joined with the
	// rendered file name): the exact path an operator would edit or
	// delete to cause the reported drift.
	Path string
	// Status is one of the verdict* constants.
	Status string
}

// checkRenderDrift diffs the would-be-rendered files against their
// installed copies on disk and returns the per-file verdicts in render
// order, plus whether the whole tree is in sync. Nothing is written —
// the caller renders with DryRun and passes the returned content here.
//
// The destination mapping mirrors the renderer's writeFiles exactly
// (users group under usersDir, defaulting to outDir; file name = the
// base of the rendered path): what -check reports as drifted is by
// definition what the next real render would overwrite. A missing
// installed file counts as drift.
//
// Why here and not in the renderer: the check is a CLI concern (verdict
// lines and an exit code for the config refresher and health runbook),
// while the renderer stays a pure spec-to-XML machine with one output
// mode. The function is deterministic and side-effect free (disk reads
// only), so tests cover it with a temp directory.
func checkRenderDrift(rendered []port.RenderedFile, outDir, usersDir string) ([]fileVerdict, bool) {
	if usersDir == "" {
		usersDir = outDir
	}
	verdicts := make([]fileVerdict, 0, len(rendered))
	inSync := true
	for _, file := range rendered {
		dir := outDir
		if file.Group == "users" {
			dir = usersDir
		}
		destination := filepath.Join(dir, filepath.Base(file.Path))
		installed, err := os.ReadFile(destination)
		switch {
		case os.IsNotExist(err):
			verdicts = append(verdicts, fileVerdict{Path: destination, Status: verdictMissing})
			inSync = false
		case err != nil:
			// An unreadable destination (permissions, a directory in the
			// way) cannot be compared — reporting it as in sync would
			// hide a broken node from the refresher.
			verdicts = append(verdicts, fileVerdict{Path: destination, Status: verdictDrifted})
			inSync = false
		case string(installed) != file.Content:
			verdicts = append(verdicts, fileVerdict{Path: destination, Status: verdictDrifted})
			inSync = false
		default:
			verdicts = append(verdicts, fileVerdict{Path: destination, Status: verdictInSync})
		}
	}
	return verdicts, inSync
}

// printVerdicts writes one "<STATUS> <path>" line per file — machine
// greppable for the refresher, human-readable in a log.
func printVerdicts(w io.Writer, verdicts []fileVerdict) {
	for _, v := range verdicts {
		fmt.Fprintf(w, "%s %s\n", v.Status, v.Path)
	}
}

// driftError reports that `config render -check` found drift. It carries
// exit code 4 so scripts can tell "the tree needs a re-render" apart
// from usage errors (1) and operational failures (3) without parsing
// output.
type driftError struct {
	// drifted counts the DRIFTED and MISSING files.
	drifted int
	// total is the number of files the render would install.
	total int
}

// Error summarizes; the per-file verdicts are printed to stdout by
// runCheck before this surfaces.
func (e *driftError) Error() string {
	return fmt.Sprintf("config drift: %d of %d rendered file(s) missing or different on disk", e.drifted, e.total)
}

// ExitCode is the drift signal for the process exit.
func (e *driftError) ExitCode() int { return exitConfigDrift }

// runCheck is the tail of `config render -check`: it prints the
// per-file verdict and returns nil when everything is in sync, or a
// *driftError naming the count (the printed verdicts name the files).
func runCheck(rendered []port.RenderedFile, outDir, usersDir string, w io.Writer) error {
	verdicts, inSync := checkRenderDrift(rendered, outDir, usersDir)
	printVerdicts(w, verdicts)
	if inSync {
		return nil
	}
	drifted := 0
	for _, v := range verdicts {
		if v.Status != verdictInSync {
			drifted++
		}
	}
	return &driftError{drifted: drifted, total: len(verdicts)}
}
