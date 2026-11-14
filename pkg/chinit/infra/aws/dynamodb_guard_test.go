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

package aws

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins the empty-lease-table guards on the DynamoDB claimer
// (defense in depth for the rendered-spec blind spot: a chinit.yaml
// without claimer_table_name used to put every lease call against
// DynamoDB table "" — a validation failure undebuggable from the ASG's
// boot loop). The spec layer fails first; these guards hold the line
// for any other caller (manual SSM runs, a stale state file).

// noopClaimDynamoDB implements the claimer's DynamoDBAPI with no-op
// calls: the empty-table guards must fire BEFORE any client call, so
// the fake needs to exist but never answer (a call reaching it fails
// the guard contract implicitly — no assertion would pass).
type noopClaimDynamoDB struct{}

func (f *noopClaimDynamoDB) PutItem(context.Context, *dynamodb.PutItemInput, ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return &dynamodb.PutItemOutput{}, nil
}

func (f *noopClaimDynamoDB) UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return &dynamodb.UpdateItemOutput{}, nil
}

func (f *noopClaimDynamoDB) DeleteItem(context.Context, *dynamodb.DeleteItemInput, ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}

var _ = Describe("DynamoDBClaimer empty-table guards", func() {
	It("rejects a claim with no table before any DynamoDB call", func() {
		claimer := NewDynamoDBClaimer("us-east-1").WithClient(&noopClaimDynamoDB{})
		_, err := claimer.Claim(context.Background(), chinitport.ClaimRequest{
			Table:           "",
			Prefix:          "writer-",
			Count:           4,
			InstanceID:      "i-guard",
			LeaseSeconds:    900,
			DeadlineSeconds: 1,
		})
		Expect(err).To(MatchError(ContainSubstring("requires a lease table")))
	})

	It("rejects a renewal with no table before any DynamoDB call", func() {
		claimer := NewDynamoDBClaimer("us-east-1").WithClient(&noopClaimDynamoDB{})
		err := claimer.Renew(context.Background(), chinitport.RenewRequest{
			Table:      "",
			Hostname:   "writer-1",
			InstanceID: "i-guard",
		})
		Expect(err).To(MatchError(ContainSubstring("requires a lease table")))
	})

	It("rejects a release with no table before any DynamoDB call", func() {
		claimer := NewDynamoDBClaimer("us-east-1").WithClient(&noopClaimDynamoDB{})
		err := claimer.Release(context.Background(), chinitport.ReleaseRequest{
			Table:    "",
			Hostname: "writer-1",
		})
		Expect(err).To(MatchError(ContainSubstring("requires a lease table")))
	})
})
