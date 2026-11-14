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
	"context"
	"errors"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// This file pins the retry engine's primitives: Do (the retry/fatal
// classification), backoff growth/cap, and the [TIMING]/failure
// summaries' format stability. The engine's state-driven behaviors
// (resume, criticality, identity persistence) live in
// identity_test.go — one subject per file.

// fakeClock runs instantly (Sleep is a no-op through the policy).
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time                  { return c.now }
func (c *fakeClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }

// recordingStateStore captures saves (specs assert resume behavior).
type recordingStateStore struct {
	loaded  *modelState
	saved   []*modelState
	loadErr error
}

func (s *recordingStateStore) Load() (*modelState, error) {
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.loaded, nil
}

func (s *recordingStateStore) Save(st *modelState) error {
	s.saved = append(s.saved, st)
	return nil
}

var _ = Describe("Do", func() {
	It("retries retryable failures until success", func() {
		calls := 0
		err := Do(context.Background(), Policy{Attempts: 3, BaseDelay: time.Millisecond}, func(context.Context) error {
			calls++
			if calls < 2 {
				return errors.New("transient")
			}
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(calls).To(Equal(2))
	})

	It("does not retry a fatal error — retrying a config bug burns the boot window", func() {
		calls := 0
		err := Do(context.Background(), Policy{Attempts: 5, BaseDelay: time.Millisecond}, func(context.Context) error {
			calls++
			return fail.Fatal(errors.New("sha mismatch"))
		})
		Expect(err).To(HaveOccurred())
		Expect(fail.IsFatal(err)).To(BeTrue(), "the fatal classification must survive the wrap")
		Expect(calls).To(Equal(1), "a fatal error must not be retried")
	})

	It("surfaces the last error after exhausting the attempts", func() {
		calls := 0
		err := Do(context.Background(), Policy{Attempts: 3, BaseDelay: time.Millisecond}, func(context.Context) error {
			calls++
			return errors.New("always down")
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("after 3 attempts"))
		Expect(calls).To(Equal(3))
	})
})

var _ = Describe("backoff", func() {
	It("grows by the factor and caps at MaxDelay", func() {
		p := Policy{BaseDelay: time.Second, MaxDelay: 5 * time.Second, Factor: 2}
		// Jitter excluded by pinning: 0.9..1.1x stays within a
		// tolerance band.
		for attempt, want := range map[int]time.Duration{
			1: time.Second,
			2: 2 * time.Second,
			3: 4 * time.Second,
		} {
			got := backoff(p, attempt)
			Expect(float64(got)).To(BeNumerically("~", float64(want), float64(want)*0.11),
				"attempt %d: jittered %s must stay within 10%% of %s", attempt, got, want)
		}
		// Capped: attempts beyond the growth stay at MaxDelay
		// regardless of the exponent — a fleet reboot never waits
		// minutes between tries.
		for _, attempt := range []int{4, 10} {
			Expect(backoff(p, attempt)).To(BeNumerically("<=", p.MaxDelay),
				"attempt %d must cap at MaxDelay", attempt)
		}
	})
})

var _ = Describe("Engine.Run", func() {
	It("skips steps the state file records done (resume)", func() {
		store := &recordingStateStore{loaded: &modelState{Steps: []stepRecord{
			{Name: "fetch_config_bundle", Status: "done"},
		}}}
		engine := NewEngine(&fakeClock{now: time.Now()}, store)

		var ran []string
		steps := []Step{
			StepFunc("fetch_config_bundle", Critical, onceFatal, func(context.Context) error {
				ran = append(ran, "fetch")
				return nil
			}),
			StepFunc("os_optimizations", Critical, onceFatal, func(context.Context) error {
				ran = append(ran, "os")
				return nil
			}),
		}
		res := engine.Run(context.Background(), steps)

		Expect(res.Fatality).NotTo(HaveOccurred())
		Expect(ran).To(Equal([]string{"os"}), "the done step is skipped; the new step runs")
		Expect(res.Steps[0].Skipped).To(BeTrue())
		Expect(res.Steps[1].Skipped).To(BeFalse())
	})

	It("aborts on a Critical failure; later steps do not run", func() {
		store := &recordingStateStore{}
		engine := NewEngine(nil, store)

		var ran []string
		res := engine.Run(context.Background(), []Step{
			StepFunc("mount_data_disks", Critical, onceFatal, func(context.Context) error {
				ran = append(ran, "mount")
				return fail.Fatal(errors.New("no disk"))
			}),
			StepFunc("start_clickhouse", Critical, onceFatal, func(context.Context) error {
				ran = append(ran, "start")
				return nil
			}),
		})

		Expect(res.Fatality).To(HaveOccurred())
		Expect(res.FatalityStep()).To(Equal("mount_data_disks"))
		Expect(ran).To(Equal([]string{"mount"}), "the step after a critical failure must not run")
	})

	It("continues past a BestEffort failure and records it as a warning", func() {
		// The "reader must join even if a peer is down" invariant.
		store := &recordingStateStore{}
		engine := NewEngine(nil, store)

		var ran []string
		res := engine.Run(context.Background(), []Step{
			StepFunc("health_gate", BestEffort, onceFatal, func(context.Context) error {
				ran = append(ran, "health")
				return errors.New("peer down")
			}),
			StepFunc("boot_finished", Critical, onceFatal, func(context.Context) error {
				ran = append(ran, "boot")
				return nil
			}),
		})

		Expect(res.Fatality).NotTo(HaveOccurred())
		Expect(ran).To(Equal([]string{"health", "boot"}))
		Expect(res.Steps[0].Failed()).To(BeTrue(), "the best-effort failure is recorded")

		var sb strings.Builder
		DescribeFailures(&sb, res)
		Expect(sb.String()).To(ContainSubstring("WARNING: best-effort step health_gate failed"))
		Expect(sb.String()).To(ContainSubstring("boot continues"))
	})
})

var _ = Describe("FormatTimingSummary", func() {
	It("keeps the [TIMING] line shape the dashboards grep", func() {
		res := RunResult{Steps: []StepOutcome{
			{Name: "fetch_config_bundle", Duration: 2 * time.Second},
			{Name: "mount_data_disks", Duration: 3 * time.Second},
		}}
		var sb strings.Builder
		FormatTimingSummary(&sb, res)
		out := sb.String()
		Expect(out).To(ContainSubstring("[TIMING] CLOUD-INIT TIMING SUMMARY"))
		Expect(out).To(ContainSubstring("fetch_config_bundle"))
		Expect(out).To(ContainSubstring("Total cloud-init time: 5s"))
	})
})
