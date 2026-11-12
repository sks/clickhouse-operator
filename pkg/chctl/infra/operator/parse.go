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
	"encoding/json"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// parseSpec unmarshals spec YAML into a CR the same way the Kubernetes API
// server does: yaml → JSON → json.Unmarshal. The operator's Settings type
// implements UnmarshalJSON only (its yaml.v3 unmarshaler is commented out),
// so a direct yaml.Unmarshal into the CR silently yields empty
// users/profiles/settings — a spec that appears to load but renders nothing.
func parseSpec(yamlBytes []byte, cr any) error {
	jsonBytes, err := yamlToJSON(yamlBytes)
	if err != nil {
		return fmt.Errorf("convert spec YAML to JSON: %w", err)
	}
	if err := json.Unmarshal(jsonBytes, cr); err != nil {
		return err
	}
	return nil
}

// yamlToJSON converts a YAML document to JSON.
func yamlToJSON(yamlBytes []byte) ([]byte, error) {
	var node map[string]any
	if err := yaml.Unmarshal(yamlBytes, &node); err != nil {
		return nil, err
	}
	return json.Marshal(node)
}

// joinErrors formats status errors for an error message.
func joinErrors(errors []string) string {
	return strings.Join(errors, "; ")
}
