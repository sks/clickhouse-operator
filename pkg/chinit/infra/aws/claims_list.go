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
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// DynamoDBScanAPI is the Scan surface the lister needs (tests fake it).
// Kept separate from DynamoDBAPI so the claimer's fake contract and
// the lister's stay independently evolvable.
type DynamoDBScanAPI interface {
	Scan(ctx context.Context, in *dynamodb.ScanInput, opts ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error)
}

// DynamoDBClaimLister implements LeaseLister's discovery half over the
// claimer's own table: one Scan enumerates every live lease.
type DynamoDBClaimLister struct {
	region string
	client DynamoDBScanAPI
}

// NewDynamoDBClaimLister builds the lister.
func NewDynamoDBClaimLister(region string) *DynamoDBClaimLister {
	return &DynamoDBClaimLister{region: region}
}

// WithClient injects a concrete client (tests).
func (l *DynamoDBClaimLister) WithClient(cl DynamoDBScanAPI) *DynamoDBClaimLister {
	l.client = cl
	return l
}

func (l *DynamoDBClaimLister) clientOrInit(ctx context.Context) (DynamoDBScanAPI, error) {
	if l.client != nil {
		return l.client, nil
	}
	cfg, err := awsConfig(ctx, l.region)
	if err != nil {
		return nil, err
	}
	l.client = dynamodb.NewFromConfig(cfg)
	return l.client, nil
}

// ListActiveClaims returns every lease row with lease_until > now.
//
// Cost note: a full-table Scan on a PAY_PER_REQUEST table, but the
// table is small (one row per live node — tens of items at most) and
// the invocations are hourly-scale (boot render, hourly schema-sync,
// weekly GCs), so the RCU cost is negligible. The FilterExpression
// keeps expiring rows out of the wire response while the code filter
// (belt and braces — a row can pass the filter by clock skew) keeps
// them out of the result.
func (l *DynamoDBClaimLister) ListActiveClaims(ctx context.Context, req chinitport.ListClaimsRequest) ([]chinitport.Claim, error) {
	client, err := l.clientOrInit(ctx)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	var claims []chinitport.Claim
	var start map[string]dynamodbtypes.AttributeValue
	for {
		out, err := client.Scan(ctx, &dynamodb.ScanInput{
			TableName:        awsString(req.Table),
			FilterExpression: awsString("lease_until > :now"),
			ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
				":now": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(now, 10)},
			},
			ExclusiveStartKey: start,
		})
		if err != nil {
			return nil, fmt.Errorf("scan lease table %s: %w", req.Table, err)
		}
		for _, item := range out.Items {
			var row claimItem
			if err := attributevalue.UnmarshalMap(item, &row); err != nil {
				return nil, fmt.Errorf("unmarshal claim row in %s: %w", req.Table, err)
			}
			// Code-side liveness filter: the Scan's FilterExpression is
			// evaluated against the same clock, but a row that expired
			// between the Scan call and item decode must never reach
			// the inventory.
			if row.LeaseUntil <= now {
				continue
			}
			claims = append(claims, chinitport.Claim{
				Hostname:   row.Hostname,
				InstanceID: row.InstanceID,
				IP:         row.IP,
			})
		}
		if out.LastEvaluatedKey == nil {
			break
		}
		start = out.LastEvaluatedKey
	}
	return claims, nil
}
