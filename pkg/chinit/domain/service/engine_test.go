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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
)

// fakeClock runs instantly (Sleep is a no-op through the policy).
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time                  { return c.now }
func (c *fakeClock) Since(t time.Time) time.Duration { return c.now.Sub(t) }

// recordingStateStore captures saves (tests assert resume behavior).
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

// TestDo_RetriesRetryableUntilSuccess: a retryable failure followed by
// success must consume attempts and succeed.
func TestDo_RetriesRetryableUntilSuccess(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{Attempts: 3, BaseDelay: time.Millisecond}, func(context.Context) error {
		calls++
		if calls < 2 {
			return errors.New("transient")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
}

// TestDo_FatalErrorDoesNotRetry: a FatalError on the first attempt must
// return immediately — retrying a config bug burns the boot window.
func TestDo_FatalErrorDoesNotRetry(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{Attempts: 5, BaseDelay: time.Millisecond}, func(context.Context) error {
		calls++
		return fail.Fatal(errors.New("sha mismatch"))
	})
	require.Error(t, err)
	require.True(t, fail.IsFatal(err), "the fatal classification must survive the wrap")
	require.Equal(t, 1, calls, "a fatal error must not be retried")
}

// TestDo_ExhaustsAttempts: persistent retryable failures surface the
// last error after the attempts run out.
func TestDo_ExhaustsAttempts(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{Attempts: 3, BaseDelay: time.Millisecond}, func(context.Context) error {
		calls++
		return errors.New("always down")
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "after 3 attempts")
	require.Equal(t, 3, calls)
}

// TestBackoff_GrowsAndCaps: base * factor^(attempt-1) capped at MaxDelay
// (jitter excluded by pinning: 0.9..1.1x stays within a tolerance band).
func TestBackoff_GrowsAndCaps(t *testing.T) {
	p := Policy{BaseDelay: time.Second, MaxDelay: 5 * time.Second, Factor: 2}
	for attempt, want := range map[int]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
	} {
		got := backoff(p, attempt)
		require.InDelta(t, float64(want), float64(got), float64(want)*0.11,
			"attempt %d: jittered %s must stay within 10%% of %s", attempt, got, want)
	}
	// Capped: attempts beyond the growth stay at MaxDelay regardless of
	// the exponent — a fleet reboot never waits minutes between tries.
	for _, attempt := range []int{4, 10} {
		got := backoff(p, attempt)
		require.LessOrEqual(t, got, p.MaxDelay, "attempt %d must cap at MaxDelay", attempt)
	}
}

// TestEngine_ResumeSkipsDoneSteps: steps recorded done in the state
// file are skipped; the rest run.
func TestEngine_ResumeSkipsDoneSteps(t *testing.T) {
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

	require.NoError(t, res.Fatality)
	require.Equal(t, []string{"os"}, ran, "the done step is skipped; the new step runs")
	require.True(t, res.Steps[0].Skipped)
	require.False(t, res.Steps[1].Skipped)
}

// TestEngine_CriticalFailureAborts: a Critical step failure stops the
// pipeline and surfaces the fatality; later steps do not run.
func TestEngine_CriticalFailureAborts(t *testing.T) {
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

	require.Error(t, res.Fatality)
	require.Equal(t, "mount_data_disks", res.FatalityStep())
	require.Equal(t, []string{"mount"}, ran, "the step after a critical failure must not run")
}

// TestEngine_BestEffortFailureContinues: a BestEffort failure is
// recorded, the pipeline continues, the boot result is a warning —
// the "reader must join even if a peer is down" invariant.
func TestEngine_BestEffortFailureContinues(t *testing.T) {
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

	require.NoError(t, res.Fatality)
	require.Equal(t, []string{"health", "boot"}, ran)
	require.True(t, res.Steps[0].Failed(), "the best-effort failure is recorded")

	var sb strings.Builder
	DescribeFailures(&sb, res)
	require.Contains(t, sb.String(), "WARNING: best-effort step health_gate failed")
	require.Contains(t, sb.String(), "boot continues")
}

// TestTimingSummary_FormatStability: the [TIMING] lines keep the grep
// contract dashboards rely on.
func TestTimingSummary_FormatStability(t *testing.T) {
	res := RunResult{Steps: []StepOutcome{
		{Name: "fetch_config_bundle", Duration: 2 * time.Second},
		{Name: "mount_data_disks", Duration: 3 * time.Second},
	}}
	var sb strings.Builder
	FormatTimingSummary(&sb, res)
	out := sb.String()
	require.Contains(t, out, "[TIMING] CLOUD-INIT TIMING SUMMARY")
	require.Contains(t, out, "fetch_config_bundle")
	require.Contains(t, out, "Total cloud-init time: 5s")
}
