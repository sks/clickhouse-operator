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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// FileStateStore persists the state file (engine records + identity
// snapshot). The model.StateFile shape is the on-disk contract.
type FileStateStore struct {
	path string
}

// NewFileStateStore builds the store, creating the parent directory.
func NewFileStateStore(path string) (*FileStateStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("create %s: %w", filepath.Dir(path), err)
	}
	return &FileStateStore{path: path}, nil
}

// Load reads the state file; a missing file is an empty state (first
// boot), not an error.
func (s *FileStateStore) Load() (*modelState, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return &modelState{}, nil
	}
	if err != nil {
		return nil, err
	}
	var st modelState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("parse %s: %w", s.path, err)
	}
	return &st, nil
}

// Save writes the state file atomically (tmp+rename: a crash mid-boot
// never leaves a truncated state the next run chokes on).
func (s *FileStateStore) Save(st *modelState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}
