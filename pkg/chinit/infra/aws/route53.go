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

	"github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// errorsAs is errors.As (indirection keeps the dynamodb.go file's SDK
// imports to the two it needs).
func errorsAs(err error, target interface{}) bool { return errors.As(err, target) }

// Route53API is the Route53 surface the registrar needs (tests fake it).
type Route53API interface {
	ListHostedZonesByName(ctx context.Context, in *route53.ListHostedZonesByNameInput, opts ...func(*route53.Options)) (*route53.ListHostedZonesByNameOutput, error)
	ChangeResourceRecordSets(ctx context.Context, in *route53.ChangeResourceRecordSetsInput, opts ...func(*route53.Options)) (*route53.ChangeResourceRecordSetsOutput, error)
}

// Route53Registrar manages the node's private-zone A record.
type Route53Registrar struct {
	region string
	client Route53API
}

// NewRoute53Registrar builds the registrar.
func NewRoute53Registrar(region string) *Route53Registrar {
	return &Route53Registrar{region: region}
}

// WithClient injects a concrete client (tests).
func (r *Route53Registrar) WithClient(c Route53API) *Route53Registrar {
	r.client = c
	return r
}

func (r *Route53Registrar) clientOrInit(ctx context.Context) (Route53API, error) {
	if r.client != nil {
		return r.client, nil
	}
	cfg, err := awsConfig(ctx, r.region)
	if err != nil {
		return nil, err
	}
	r.client = route53.NewFromConfig(cfg)
	return r.client, nil
}

// ResolveZoneID finds the private zone by exact DNS name (Terraform
// cannot pass the zone id without a module cycle; boot resolution is
// the documented workaround, cached in the identity snapshot).
func (r *Route53Registrar) ResolveZoneID(ctx context.Context, zoneName string) (string, error) {
	client, err := r.clientOrInit(ctx)
	if err != nil {
		return "", err
	}
	out, err := client.ListHostedZonesByName(ctx, &route53.ListHostedZonesByNameInput{
		DNSName: awsString(zoneName),
	})
	if err != nil {
		return "", err
	}
	want := zoneName
	if len(want) == 0 || want[len(want)-1] != '.' {
		want += "."
	}
	for _, z := range out.HostedZones {
		if z.Name != nil && *z.Name == want {
			id := *z.Id
			// The API returns /hostedzone/<id>; the change API wants
			// the bare id.
			return trimHostedZonePrefix(id), nil
		}
	}
	return "", &zoneNotFoundError{Zone: zoneName}
}

// zoneNotFoundError names the resolution failure (WARNING-class: the
// boot continues without self-registration; the caller decides).
type zoneNotFoundError struct{ Zone string }

func (e *zoneNotFoundError) Error() string {
	return "no Route53 hosted zone named " + e.Zone
}

func trimHostedZonePrefix(id string) string {
	const prefix = "/hostedzone/"
	if len(id) > len(prefix) && id[:len(prefix)] == prefix {
		return id[len(prefix):]
	}
	return id
}

// UpsertARecord creates or replaces the node's A record.
func (r *Route53Registrar) UpsertARecord(ctx context.Context, req chinitport.ARecordRequest) error {
	client, err := r.clientOrInit(ctx)
	if err != nil {
		return err
	}
	if req.TTL == 0 {
		req.TTL = 60
	}
	_, err = client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: awsString(req.ZoneID),
		ChangeBatch: &route53types.ChangeBatch{
			Changes: []route53types.Change{{
				Action: route53types.ChangeActionUpsert,
				ResourceRecordSet: &route53types.ResourceRecordSet{
					Name:            awsString(fqdn(req.RecordName, req.ZoneName)),
					Type:            route53types.RRTypeA,
					TTL:             int64Ptr(req.TTL),
					ResourceRecords: []route53types.ResourceRecord{{Value: awsString(req.IP)}},
				},
			}},
		},
	})
	return err
}

// DeleteARecord removes the node's A record (drain/termination).
func (r *Route53Registrar) DeleteARecord(ctx context.Context, req chinitport.ARecordRequest) error {
	client, err := r.clientOrInit(ctx)
	if err != nil {
		return err
	}
	_, err = client.ChangeResourceRecordSets(ctx, &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: awsString(req.ZoneID),
		ChangeBatch: &route53types.ChangeBatch{
			Changes: []route53types.Change{{
				Action: route53types.ChangeActionDelete,
				ResourceRecordSet: &route53types.ResourceRecordSet{
					Name: awsString(fqdn(req.RecordName, req.ZoneName)),
					Type: route53types.RRTypeA,
					TTL:  int64Ptr(req.TTL),
				},
			}},
		},
	})
	return err
}

// fqdn joins the short hostname and the zone name.
func fqdn(name, zone string) string {
	if zone == "" {
		return name
	}
	if len(zone) > 0 && zone[len(zone)-1] == '.' {
		return name + "." + zone[:len(zone)-1] + "."
	}
	return name + "." + zone + "."
}

// int64Ptr is the numeric pointer helper.
func int64Ptr(v int) *int64 { return &[]int64{int64(v)}[0] }
