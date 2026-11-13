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

package detect

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDetect_PrecedenceChain: flag > env > DMI > IMDS — each layer wins
// over the ones below it.
func TestDetect_PrecedenceChain(t *testing.T) {
	ctx := context.Background()
	noProbes := Options{
		DMIVendorFunc: func() (string, error) { return "", errors.New("no dmi") },
		ProbeAWS:      func(context.Context) (bool, error) { return false, nil },
		ProbeAzure:    func(context.Context) (bool, error) { return false, nil },
	}

	t.Run("flag wins over everything", func(t *testing.T) {
		res, err := Detect(ctx, Options{
			Flag:         "AWS", // case-normalized
			EnvName:      "azure",
			DMIVendorFunc: func() (string, error) { return "Microsoft Corporation", nil },
		})
		require.NoError(t, err)
		require.Equal(t, AWSName, res.Provider)
		require.Equal(t, "flag", res.Via)
	})

	t.Run("env wins over dmi", func(t *testing.T) {
		res, err := Detect(ctx, Options{
			EnvName:      "azure",
			DMIVendorFunc: func() (string, error) { return "Amazon EC2", nil },
		})
		require.NoError(t, err)
		require.Equal(t, AzureName, res.Provider)
		require.Equal(t, "env", res.Via)
	})

	t.Run("dmi amazon signature", func(t *testing.T) {
		res, err := Detect(ctx, Options{
			DMIVendorFunc: func() (string, error) { return "Amazon EC2", nil },
		})
		require.NoError(t, err)
		require.Equal(t, AWSName, res.Provider)
		require.Equal(t, "dmi", res.Via)
	})

	t.Run("dmi microsoft signature", func(t *testing.T) {
		res, err := Detect(ctx, Options{
			DMIVendorFunc: func() (string, error) { return "Microsoft Corporation", nil },
		})
		require.NoError(t, err)
		require.Equal(t, AzureName, res.Provider)
	})

	t.Run("imds fallback when dmi is generic", func(t *testing.T) {
		res, err := Detect(ctx, Options{
			DMIVendorFunc: func() (string, error) { return "QEMU", nil },
			ProbeAWS:      func(context.Context) (bool, error) { return true, nil },
		})
		require.NoError(t, err)
		require.Equal(t, AWSName, res.Provider)
		require.Equal(t, "imds", res.Via)
	})

	t.Run("azure imds fallback", func(t *testing.T) {
		res, err := Detect(ctx, Options{
			DMIVendorFunc: func() (string, error) { return "QEMU", nil },
			ProbeAWS:      func(context.Context) (bool, error) { return false, nil },
			ProbeAzure:    func(context.Context) (bool, error) { return true, nil },
		})
		require.NoError(t, err)
		require.Equal(t, AzureName, res.Provider)
	})

	t.Run("no signal is an error", func(t *testing.T) {
		_, err := Detect(ctx, noProbes)
		require.Error(t, err)
		require.Contains(t, err.Error(), "could not detect cloud provider")
	})
}
