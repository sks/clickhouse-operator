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
	"errors"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This suite pins the LeaseLister half of the DynamoDB claim table: the
// inventory sources (clusterHosts, specInventory) treat ListActiveClaims
// as the cluster's live member set, so the specs assert exactly which
// rows survive — active leases in, expired/foreign rows out, pages
// concatenated.

// fakeScanDynamoDB answers Scan calls from a canned in-memory claim
// table (the same claimItem shape the claimer writes) with optional
// pagination. It implements ONLY DynamoDBScanAPI — the lister's seam —
// so the claimer's fake contract and this one stay independent.
type fakeScanDynamoDB struct {
	// rows is the raw claim table keyed by hostname.
	rows map[string]claimItem
	// pageSize, when > 0, forces paginated Scans of this many items.
	pageSize int
	// scanErr, when set, fails the Scan outright.
	scanErr error
	// tables records every table name the lister scanned.
	tables []string
}

func (f *fakeScanDynamoDB) Scan(_ context.Context, in *dynamodb.ScanInput, _ ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	f.tables = append(f.tables, *in.TableName)
	if in.TableName == nil || *in.TableName == "" {
		return nil, errors.New("missing table name")
	}
	// Filter to ACTIVE rows only, mirroring the server-side
	// FilterExpression the real call sends: lease_until > now. Sorted
	// by hostname so paginated traversal is deterministic (map range
	// order is randomized per call).
	now := time.Now().Unix()
	var active []claimItem
	for _, row := range f.rows {
		if row.LeaseUntil > now {
			active = append(active, row)
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i].Hostname < active[j].Hostname })
	// Pagination: honor ExclusiveStartKey, emit LastEvaluatedKey when a
	// pageSize is configured and more rows remain.
	start := 0
	if in.ExclusiveStartKey != nil {
		if host, ok := in.ExclusiveStartKey["hostname"].(*dynamodbtypes.AttributeValueMemberS); ok {
			for i, row := range active {
				if row.Hostname == host.Value {
					start = i + 1
					break
				}
			}
		}
	}
	end := len(active)
	if f.pageSize > 0 && start+f.pageSize < len(active) {
		end = start + f.pageSize
	}
	page := active[start:end]
	items := make([]map[string]dynamodbtypes.AttributeValue, 0, len(page))
	for _, row := range page {
		item, err := attributevalue.MarshalMap(row)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	var lastKey map[string]dynamodbtypes.AttributeValue
	if end < len(active) && len(page) > 0 {
		last := page[len(page)-1]
		lastKey = map[string]dynamodbtypes.AttributeValue{
			"hostname": &dynamodbtypes.AttributeValueMemberS{Value: last.Hostname},
		}
	}
	return &dynamodb.ScanOutput{Items: items, LastEvaluatedKey: lastKey}, nil
}

// seedClaims builds a claim table from live/expired row spec.
func seedClaims(live []chinitport.Claim, expired []chinitport.Claim) map[string]claimItem {
	rows := map[string]claimItem{}
	for _, c := range live {
		rows[c.Hostname] = claimItem{
			Hostname:   c.Hostname,
			InstanceID: c.InstanceID,
			IP:         c.IP,
			LeaseUntil: time.Now().Unix() + 600,
		}
	}
	for _, c := range expired {
		rows[c.Hostname] = claimItem{
			Hostname:   c.Hostname,
			InstanceID: c.InstanceID,
			IP:         c.IP,
			LeaseUntil: time.Now().Unix() - 600,
		}
	}
	return rows
}

var _ = Describe("DynamoDBClaimLister.ListActiveClaims", func() {
	liveFleet := []chinitport.Claim{
		{Hostname: "writer-0", InstanceID: "i-w0", IP: "10.0.1.10"},
		{Hostname: "writer-2", InstanceID: "i-w2", IP: "10.0.1.12"},
		{Hostname: "reader-0", InstanceID: "i-r0", IP: "10.0.1.40"},
	}

	It("returns every ACTIVE claim with its identity fields", func() {
		client := &fakeScanDynamoDB{rows: seedClaims(liveFleet, nil)}
		lister := NewDynamoDBClaimLister("us-east-1").WithClient(client)

		claims, err := lister.ListActiveClaims(context.Background(), chinitport.ListClaimsRequest{Table: "node-claims"})
		Expect(err).NotTo(HaveOccurred())
		Expect(claims).To(ConsistOf(liveFleet))
		// The scan targets the requested table.
		Expect(client.tables).To(Equal([]string{"node-claims"}))
	})

	It("drops expired lease rows (a vacant slot is not a member)", func() {
		// writer-1's lease expired (the ASG scaled it in): the row is
		// still in the table but must NOT become inventory — the exact
		// vacant-slot bug live-claims discovery exists to kill.
		client := &fakeScanDynamoDB{rows: seedClaims(
			liveFleet,
			[]chinitport.Claim{{Hostname: "writer-1", InstanceID: "i-w1", IP: "10.0.1.11"}},
		)}
		lister := NewDynamoDBClaimLister("us-east-1").WithClient(client)

		claims, err := lister.ListActiveClaims(context.Background(), chinitport.ListClaimsRequest{Table: "node-claims"})
		Expect(err).NotTo(HaveOccurred())
		Expect(claims).To(ConsistOf(liveFleet))
		Expect(claims).NotTo(ContainElement(chinitport.Claim{Hostname: "writer-1"}))
	})

	It("concatenates paginated scans into one result", func() {
		// A table big enough for LastEvaluatedKey pages: the inventory
		// consumer must see the full member set, never page one only.
		client := &fakeScanDynamoDB{rows: seedClaims(liveFleet, nil), pageSize: 1}
		lister := NewDynamoDBClaimLister("us-east-1").WithClient(client)

		claims, err := lister.ListActiveClaims(context.Background(), chinitport.ListClaimsRequest{Table: "node-claims"})
		Expect(err).NotTo(HaveOccurred())
		Expect(claims).To(ConsistOf(liveFleet))
	})

	It("propagates a scan failure to the caller (no silent empty list)", func() {
		// The call sites fall back to static inventory on error — a
		// swallowed error here would render the fallback invisible and
		// the boot would carry an empty inventory.
		client := &fakeScanDynamoDB{scanErr: errors.New("dynamodb down")}
		lister := NewDynamoDBClaimLister("us-east-1").WithClient(client)

		_, err := lister.ListActiveClaims(context.Background(), chinitport.ListClaimsRequest{Table: "node-claims"})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("dynamodb down"))
		Expect(err.Error()).To(ContainSubstring("node-claims"))
	})
})

// The provider-capability wiring: AsLeaseLister must find the lister on
// the real AWS provider (the inventory sources probe the capability,
// not the concrete type).
var _ = Describe("AWS provider lease-listing capability", func() {
	It("satisfies chinitport.LeaseLister via AsLeaseLister", func() {
		provider := New(chinitport.Config{Region: "us-east-1"})
		Expect(chinitport.AsLeaseLister(provider)).NotTo(BeNil())
	})
})
