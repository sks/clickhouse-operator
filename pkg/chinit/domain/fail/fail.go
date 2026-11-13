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

// Package fail holds the retry engine's error classification: the
// Retryable/Fatal split every step's errors carry.
//
// Why a leaf package: the engine (domain/service) and the host-ops
// adapters (infra/system) both classify errors — a shared package
// breaks what would otherwise be an import cycle (service imports
// system for the Runner seam; system classifies fatal conditions like
// SHA mismatches).
package fail

import (
	"errors"
	"fmt"
)

// FatalError marks an operation whose failure must not be retried:
// IAM AccessDenied, a SHA256 mismatch, a missing golden-AMI binary.
// Retrying masks a config bug and burns the boot window.
type FatalError struct {
	// Step, when set, is the failing step name (filled by the engine).
	Step string
	Err  error
}

func (e *FatalError) Error() string {
	if e.Step != "" {
		return fmt.Sprintf("step %s: fatal: %s", e.Step, e.Err)
	}
	return "fatal: " + e.Err.Error()
}

func (e *FatalError) Unwrap() error { return e.Err }

// Fatal wraps err as a FatalError. nil maps to nil.
func Fatal(err error) error {
	if err == nil {
		return nil
	}
	var fe *FatalError
	if errors.As(err, &fe) {
		return err
	}
	return &FatalError{Err: err}
}

// IsFatal reports whether err (or anything in its chain) is a FatalError.
func IsFatal(err error) bool {
	var fe *FatalError
	return errors.As(err, &fe)
}
