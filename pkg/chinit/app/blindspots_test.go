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

package app

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	chinitport "github.com/altinity/clickhouse-operator/pkg/chinit/domain/port"
)

// This file pins the logging layer the blind-spot fixes ride on:
//
//   - the level-split handler routes INFO to stdout and WARN/ERROR to
//     stderr — the journal split the human-readable boot log always
//     had, now preserved for the structured records;
//   - the drain's peer-fallback WARNING reaches slog (structured attrs
//     — the "why" is an attribute, not buried in the message) while
//     the raw WARNING line keeps its fmt.Stderr form (the maintenance
//     suite pins that contract);
//   - runLeaseRenew emits its attempt + outcome records.

// captureStdout mirrors maintenance_test.go's captureStderr for the
// stdout side of the level split.
func captureStdout(run func()) string {
	logPath := filepath.Join(os.TempDir(), "chinit-app-stdout.log")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		Fail(err.Error())
	}
	orig := os.Stdout
	os.Stdout = f
	defer func() { os.Stdout = orig }()
	run()
	if err := f.Close(); err != nil {
		Fail(err.Error())
	}
	data, err := os.ReadFile(logPath)
	if err != nil {
		Fail(err.Error())
	}
	return string(data)
}

// captureSlog swaps the default logger for a buffer-backed one and
// returns the buffer plus the restore func.
func captureSlog() (*bytes.Buffer, func()) {
	buf := &bytes.Buffer{}
	orig := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return buf, func() { slog.SetDefault(orig) }
}

var _ = Describe("levelSplitHandler", func() {
	It("routes Info to stdout and Warn to stderr", func() {
		var out, errOut bytes.Buffer
		logger := slog.New(NewLevelSplitHandler(&out, &errOut))

		logger.Info("boot progress", "step", "settle_identity")
		logger.Warn("degraded", "reason", "lease outage")

		Expect(out.String()).To(ContainSubstring("INFO"))
		Expect(out.String()).To(ContainSubstring("boot progress"))
		Expect(out.String()).To(ContainSubstring("step=settle_identity"))
		Expect(out.String()).NotTo(ContainSubstring("degraded"))

		Expect(errOut.String()).To(ContainSubstring("WARN"))
		Expect(errOut.String()).To(ContainSubstring("degraded"))
		Expect(errOut.String()).To(ContainSubstring("reason=\"lease outage\""))
		Expect(errOut.String()).NotTo(ContainSubstring("boot progress"))
	})

	It("drops Debug records (the boot log is operator-facing)", func() {
		var out, errOut bytes.Buffer
		logger := slog.New(NewLevelSplitHandler(&out, &errOut))
		logger.Debug("chatty internals")
		Expect(out.String()).To(BeEmpty())
		Expect(errOut.String()).To(BeEmpty())
	})

	It("InitLogging installs a handler that honors an os.Stderr swap", func() {
		// The journal-capture contract the existing suites rely on:
		// swapped os.Stdout/os.Stderr file handles capture slog
		// records too (both TextHandlers resolve the *os.File vars at
		// record time).
		InitLogging()
		DeferCleanup(func() { InitLogging() })

		stderr := captureStderr(func() {
			slog.Warn("warning through the swapped handle")
		})
		stdout := captureStdout(func() {
			slog.Info("info through the swapped handle")
		})
		Expect(stderr).To(ContainSubstring("warning through the swapped handle"))
		Expect(stdout).To(ContainSubstring("info through the swapped handle"))
	})
})

var _ = Describe("drain fallback logging", func() {
	It("emits the peer-fallback warning through slog with the why as an attribute", func() {
		// The drain's degraded path: live discovery cannot answer (the
		// bare provider has no lister), the fallback peer is
		// writer-0, and BOTH contracts hold — the raw WARNING line the
		// journal greps and the structured record the log pipeline
		// consumes.
		buf, restore := captureSlog()
		DeferCleanup(restore)

		st := drainer()
		spec := &inventorySpec{
			WriterMax:        4,
			ReaderMax:        10,
			ReaderHostPrefix: "reader-",
		}
		peer := selectDrainPeer(context.Background(), st, spec, nil, "azure")
		Expect(peer).NotTo(BeNil())
		Expect(peer.Hostname).To(Equal("writer-0"))

		log := buf.String()
		Expect(log).To(ContainSubstring("WARNING: drain peer fell back to writer-0"))
		Expect(log).To(ContainSubstring("weekly dead-replica GC is the safety net"))
		// The structured "why": the no-capability cause is an
		// attribute, queryable — never only embedded in the message.
		Expect(log).To(ContainSubstring("why="))
		Expect(log).To(ContainSubstring("no lease-listing capability"))
	})

	It("logs the drain's phases on the critical path", func() {
		// The full drain against the fixture fakes: the start record
		// carries the identity, reason and budget; each phase lands as
		// the drain walks the hook's sequence.
		buf, restore := captureSlog()
		DeferCleanup(restore)

		f, fixtures := drainFixtures()
		DeferCleanup(fixtures)
		f.lister.claims = []chinitport.Claim{{Hostname: "writer-0"}}

		st := drainer()
		provider := &drainProvider{name: "aws", lister: f.lister, claimer: f.claimer, dns: f.dns}
		Expect(drain(context.Background(), st, provider, drainOptions{
			DeadlineSeconds: 240,
			Reason:          drainReasonTerminationHook,
		})).To(Succeed())

		log := buf.String()
		Expect(log).To(ContainSubstring("drain: draining node"))
		Expect(log).To(ContainSubstring("hostname=writer-1"))
		Expect(log).To(ContainSubstring("reason=termination-hook"))
		Expect(log).To(ContainSubstring("budget_seconds=240"))
		Expect(log).To(ContainSubstring("drain: query issued"))
		Expect(log).To(ContainSubstring("drain: dropping replica on peer"))
		Expect(log).To(ContainSubstring("drain: DNS A record released"))
		Expect(log).To(ContainSubstring("drain: lease released"))
		Expect(log).To(ContainSubstring("drain: complete"))
	})
})

var _ = Describe("lease-renew logging", func() {
	It("logs the renewal attempt and its success", func() {
		buf, restore := captureSlog()
		DeferCleanup(restore)

		claimer := &drainClaimer{}
		origBuild := buildProvider
		buildProvider = func(string, string) (chinitport.Provider, error) {
			return &drainProvider{name: "aws", claimer: claimer}, nil
		}
		DeferCleanup(func() { buildProvider = origBuild })

		dir := filepath.Join(os.TempDir(), "chinit-app-blindspot-test")
		Expect(os.MkdirAll(dir, 0755)).To(Succeed())
		statePath := filepath.Join(dir, "state.json")
		state := `{"identity":{"role":"writer","hostname":"writer-1","instance_id":"i-w1","cluster_name":"logexplorer","dns_zone":"ch.internal","lease_table":"node-claims"}}`
		Expect(os.WriteFile(statePath, []byte(state), 0644)).To(Succeed())

		Expect(runLeaseRenew([]string{"-state", statePath})).To(Succeed())
		log := buf.String()
		Expect(log).To(ContainSubstring("lease-renew: extending lease"))
		Expect(log).To(ContainSubstring("hostname=writer-1"))
		Expect(log).To(ContainSubstring("lease_table=node-claims"))
		Expect(log).To(ContainSubstring("lease-renew: lease extended"))
	})

	It("logs the renewal failure with the cause as an attribute", func() {
		buf, restore := captureSlog()
		DeferCleanup(restore)

		origBuild := buildProvider
		buildProvider = func(string, string) (chinitport.Provider, error) {
			return &drainProvider{name: "aws", claimer: &failingRenewClaimer{}}, nil
		}
		DeferCleanup(func() { buildProvider = origBuild })

		dir := filepath.Join(os.TempDir(), "chinit-app-blindspot-test")
		Expect(os.MkdirAll(dir, 0755)).To(Succeed())
		statePath := filepath.Join(dir, "state.json")
		state := `{"identity":{"role":"writer","hostname":"writer-1","instance_id":"i-w1","lease_table":"node-claims"}}`
		Expect(os.WriteFile(statePath, []byte(state), 0644)).To(Succeed())

		Expect(runLeaseRenew([]string{"-state", statePath})).NotTo(Succeed())
		Expect(buf.String()).To(ContainSubstring("lease-renew: renewal failed"))
		Expect(buf.String()).To(ContainSubstring("err="))
	})
})

// failingRenewClaimer answers a renewal error.
type failingRenewClaimer struct{}

func (c *failingRenewClaimer) ClaimOrdinal(context.Context, chinitport.ClaimRequest) (int, error) {
	return 0, nil
}

func (c *failingRenewClaimer) RenewLease(context.Context, chinitport.RenewRequest) error {
	return errors.New("dynamodb throttled")
}

func (c *failingRenewClaimer) ReleaseLease(context.Context, chinitport.ReleaseRequest) error {
	return nil
}
