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

// DynamoDBAPI is the DynamoDB surface the claimer needs (tests fake it).
type DynamoDBAPI interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, in *dynamodb.UpdateItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// claimItem is the lease row: hostname is the key; instance_id guards
// renewal (only the holder extends); lease_until is the TTL epoch.
type claimItem struct {
	Hostname   string `dynamodbav:"hostname"`
	InstanceID string `dynamodbav:"instance_id"`
	IP         string `dynamodbav:"ip"`
	LeaseUntil int64  `dynamodbav:"lease_until"`
}

// DynamoDBClaimer implements the ordinal lease over DynamoDB conditional
// puts — the exact semantics the bash script encoded, without the JSON
// string-building.
type DynamoDBClaimer struct {
	region string
	client DynamoDBAPI
}

// NewDynamoDBClaimer builds the claimer.
func NewDynamoDBClaimer(region string) *DynamoDBClaimer {
	return &DynamoDBClaimer{region: region}
}

// WithClient injects a concrete client (tests).
func (c *DynamoDBClaimer) WithClient(cl DynamoDBAPI) *DynamoDBClaimer {
	c.client = cl
	return c
}

func (c *DynamoDBClaimer) clientOrInit(ctx context.Context) (DynamoDBAPI, error) {
	if c.client != nil {
		return c.client, nil
	}
	cfg, err := awsConfig(ctx, c.region)
	if err != nil {
		return nil, err
	}
	c.client = dynamodb.NewFromConfig(cfg)
	return c.client, nil
}

// Claim sweeps ordinals 0..count-1 until one conditional put succeeds,
// bounded by the request's deadline (one lease window: an
// instance-refresh replacement boots while the old node still holds its
// lease and its renewal cron keeps it alive until the instance dies).
//
// The conditional put (attribute_not_exists(hostname) OR lease_until <
// now) is the atomicity primitive: two readers racing for reader-0
// cannot both win, and an expired lease is claimable by anyone.
func (c *DynamoDBClaimer) Claim(ctx context.Context, req chinitport.ClaimRequest) (int, error) {
	// An empty table name would put every conditional item into a
	// bogus PutItem call (the rendered-spec blind spot: the claim
	// targets "" and DynamoDB's validation error is undebuggable from
	// a boot loop). Guard here too, not only at the spec layer.
	if req.Table == "" {
		return -1, fmt.Errorf("ordinal claim requires a lease table (ClaimRequest.Table is empty — check claimer_table_name in the chinit spec)")
	}
	client, err := c.clientOrInit(ctx)
	if err != nil {
		return -1, err
	}

	deadline := time.Now().Add(time.Duration(req.DeadlineSeconds) * time.Second)
	for time.Now().Before(deadline) {
		for ordinal := 0; ordinal < req.Count; ordinal++ {
			hostname := req.Prefix + strconv.Itoa(ordinal)
			now := time.Now().Unix()
			item, err := attributevalue.MarshalMap(claimItem{
				Hostname:   hostname,
				InstanceID: req.InstanceID,
				IP:         req.PrivateIP,
				LeaseUntil: now + int64(req.LeaseSeconds),
			})
			if err != nil {
				return -1, fmt.Errorf("marshal claim item: %w", err)
			}
			_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
				TableName:           awsString(req.Table),
				Item:                item,
				ConditionExpression: awsString("attribute_not_exists(hostname) OR lease_until < :now"),
				ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
					":now": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(now, 10)},
				},
			})
			if err == nil {
				return ordinal, nil
			}
			// ConditionalCheckFailed is the expected "taken" answer —
			// the sweep continues with the next ordinal.
			if !isConditionalCheckFailed(err) {
				return -1, fmt.Errorf("claim %s: %w", hostname, err)
			}
		}
		// All ordinals leased: wait out the sweep interval, then retry
		// until the deadline (a dead node's lease expires).
		select {
		case <-ctx.Done():
			return -1, ctx.Err()
		case <-time.After(60 * time.Second):
		}
	}
	return -1, fmt.Errorf("failed to claim a reader ordinal after one lease window (all %d taken). Increase reader_count", req.Count)
}

// isConditionalCheckFailed reports whether err is DynamoDB's
// ConditionalCheckFailed (the "someone else holds it" signal).
func isConditionalCheckFailed(err error) bool {
	var ccf *dynamodbtypes.ConditionalCheckFailedException
	return errorsAs(err, &ccf)
}

// Renew extends the holder's lease; the instance_id condition makes a
// stolen-ordinal renewal by a different node a no-op failure.
func (c *DynamoDBClaimer) Renew(ctx context.Context, req chinitport.RenewRequest) error {
	// Same empty-table guard as Claim: a renewal against "" fails in
	// DynamoDB validation, not with a reason an operator can act on.
	if req.Table == "" {
		return fmt.Errorf("lease renewal requires a lease table (RenewRequest.Table is empty)")
	}
	client, err := c.clientOrInit(ctx)
	if err != nil {
		return err
	}
	lease := time.Now().Unix() + int64(req.LeaseSeconds)
	_, err = client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           awsString(req.Table),
		Key:                 claimKey(req.Hostname),
		UpdateExpression:    awsString("SET lease_until = :lease"),
		ConditionExpression: awsString("instance_id = :me"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":lease": &dynamodbtypes.AttributeValueMemberN{Value: strconv.FormatInt(lease, 10)},
			":me":    &dynamodbtypes.AttributeValueMemberS{Value: req.InstanceID},
		},
	})
	return err
}

// Release deletes the node's claim (spot drain / termination).
func (c *DynamoDBClaimer) Release(ctx context.Context, req chinitport.ReleaseRequest) error {
	// Same empty-table guard as Claim: the drain must say what it
	// could not do, not surface DynamoDB's own validation error.
	if req.Table == "" {
		return fmt.Errorf("lease release requires a lease table (ReleaseRequest.Table is empty)")
	}
	client, err := c.clientOrInit(ctx)
	if err != nil {
		return err
	}
	_, err = client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: awsString(req.Table),
		Key:       claimKey(req.Hostname),
	})
	return err
}

// claimKey builds the hostname key attribute.
func claimKey(hostname string) map[string]dynamodbtypes.AttributeValue {
	return map[string]dynamodbtypes.AttributeValue{
		"hostname": &dynamodbtypes.AttributeValueMemberS{Value: hostname},
	}
}
