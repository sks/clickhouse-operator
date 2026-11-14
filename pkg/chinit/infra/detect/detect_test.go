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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Detect", func() {
	// Each layer wins over the ones below it — the boot log's "via"
	// names the layer that answered, which is the operator's first
	// triage hint when a node picks the wrong cloud.
	DescribeTable("the precedence chain flag > env > dmi > imds",
		func(opts Options, wantProvider, wantVia string) {
			res, err := Detect(context.Background(), opts)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Provider).To(Equal(wantProvider))
			if wantVia != "" {
				Expect(res.Via).To(Equal(wantVia))
			}
		},
		Entry("the flag wins over everything", Options{
			Flag:          "AWS", // case-normalized
			EnvName:       "azure",
			DMIVendorFunc: func() (string, error) { return "Microsoft Corporation", nil },
		}, AWSName, "flag"),
		Entry("the env wins over dmi", Options{
			EnvName:       "azure",
			DMIVendorFunc: func() (string, error) { return "Amazon EC2", nil },
		}, AzureName, "env"),
		Entry("the dmi amazon signature", Options{
			DMIVendorFunc: func() (string, error) { return "Amazon EC2", nil },
		}, AWSName, "dmi"),
		Entry("the dmi microsoft signature", Options{
			DMIVendorFunc: func() (string, error) { return "Microsoft Corporation", nil },
		}, AzureName, ""),
		Entry("imds answers when dmi is generic", Options{
			DMIVendorFunc: func() (string, error) { return "QEMU", nil },
			ProbeAWS:      func(context.Context) (bool, error) { return true, nil },
		}, AWSName, "imds"),
		Entry("the azure imds fallback", Options{
			DMIVendorFunc: func() (string, error) { return "QEMU", nil },
			ProbeAWS:      func(context.Context) (bool, error) { return false, nil },
			ProbeAzure:    func(context.Context) (bool, error) { return true, nil },
		}, AzureName, ""),
	)

	It("errors when no signal answers", func() {
		_, err := Detect(context.Background(), Options{
			DMIVendorFunc: func() (string, error) { return "", errors.New("no dmi") },
			ProbeAWS:      func(context.Context) (bool, error) { return false, nil },
			ProbeAzure:    func(context.Context) (bool, error) { return false, nil },
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("could not detect cloud provider"))
	})
})
