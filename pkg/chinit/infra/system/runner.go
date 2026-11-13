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

// Package system implements chinit's host-side operations: command
// execution, disk mounting, OS tuning, systemd unit installation and the
// XML validation the bash script did inline.
//
// Why an injectable Runner: there is no pure-Go mkfs.ext4 or hostnamectl,
// and shelling out is fine — but untestable shelling out is what the bash
// script was. Every command goes through Runner; tests record invocations
// and script outcomes, so the mount logic (label-vs-fallback, fstab
// idempotency) is unit-tested without block devices.
package system

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// Runner executes external commands. Production is ExecRunner (real
// os/exec); tests inject a recording fake with scripted responses.
type Runner interface {
	// Run executes cmd with args and returns combined output. A
	// non-zero exit is an error carrying the output.
	Run(ctx context.Context, cmd string, args ...string) (string, error)
}

// ExecRunner is the real runner.
type ExecRunner struct{}

// Run runs the command, capturing combined stdout+stderr.
func (ExecRunner) Run(ctx context.Context, cmd string, args ...string) (string, error) {
	out, err := exec.CommandContext(ctx, cmd, args...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%s %s: %w: %s", cmd, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

// CommandError captures a failed command with its output for the boot
// log (the bash exit_if_error context, structured).
type CommandError struct {
	Cmd    string
	Output string
	Err    error
}

func (e *CommandError) Error() string {
	return fmt.Sprintf("%s failed: %s: %s", e.Cmd, e.Err, strings.TrimSpace(e.Output))
}

func (e *CommandError) Unwrap() error { return e.Err }

// LookPath reports whether a binary exists (the golden-AMI require step).
func LookPath(bin string) (string, error) { return exec.LookPath(bin) }
