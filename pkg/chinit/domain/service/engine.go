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

// Package service implements chinit's boot orchestration: the state-aware
// retry engine and the step pipeline.
//
// Why an engine and not bash `set -e` toggling: the script had ~16 ad-hoc
// `sleep` loops with no shared policy, no error classification and no
// memory of what already succeeded — a re-run redid everything. The
// engine classifies errors (fatal ones stop the boot so the ASG replaces
// the node; retryable ones back off), gives every step a deadline, and
// records outcomes in the state file so a re-run resumes instead of
// repeating. Convergence (probe-then-act) lives inside each step; the
// engine owns repetition.
package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"strings"
	"time"

	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/fail"
	"github.com/altinity/clickhouse-operator/pkg/chinit/domain/model"
)

// FatalError, Fatal and IsFatal live in domain/fail (the leaf package
// the host-ops adapters also classify through); the aliases keep the
// engine's call sites short.
type FatalError = fail.FatalError

// Fatal wraps err as a FatalError (see fail.Fatal).
func Fatal(err error) error { return fail.Fatal(err) }

// IsFatal reports whether err is a FatalError (see fail.IsFatal).
func IsFatal(err error) bool { return fail.IsFatal(err) }

// Policy is one step's retry contract. Zero values mean "sane default";
// only the knobs a step actually needs are set.
type Policy struct {
	// Attempts is the max tries (0 → 1). Duration-bounded steps (the
	// claim sweep) set Attempts high and rely on Deadline.
	Attempts int
	// BaseDelay is the first retry delay; each subsequent delay grows by
	// Factor (0 → 2) up to MaxDelay (0 → 30s). Jitter spreads herd
	// restarts across an ASG.
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Factor    float64
	// Deadline bounds the WHOLE operation including waits (0 → none).
	// Steps that must finish within a known window (the 2-minute spot
	// drain; the one-lease-window claim) set this.
	Deadline time.Duration
	// Sleep abstracts waiting so tests inject a fake clock. nil →
	// time.Sleep.
	Sleep func(d time.Duration)
}

func (p Policy) withDefaults() Policy {
	if p.Attempts == 0 {
		p.Attempts = 1
	}
	if p.BaseDelay == 0 {
		p.BaseDelay = 2 * time.Second
	}
	if p.MaxDelay == 0 {
		p.MaxDelay = 30 * time.Second
	}
	if p.Factor == 0 {
		p.Factor = 2
	}
	if p.Sleep == nil {
		p.Sleep = time.Sleep
	}
	return p
}

// retryable decides whether to try again. Fatal errors never retry; nil
// context is fine; everything else retries until the policy exhausts.
func retryable(err error) bool { return err != nil && !IsFatal(err) }

// Do runs op under the policy: classification, backoff with jitter,
// deadline enforcement. The op receives the remaining-deadline context so
// long operations (S3 fetch, IMDS probe) abort instead of overrunning.
func Do(ctx context.Context, p Policy, op func(ctx context.Context) error) error {
	p = p.withDefaults()

	var lastErr error
	for attempt := 1; attempt <= p.Attempts; attempt++ {
		if p.Deadline > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, p.Deadline)
			// Cancel on every path: a returning op may finish early;
			// the next attempt gets a fresh window below.
			defer cancel()
		}
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("retry deadline exceeded before attempt %d: %w", attempt, err)
		}

		lastErr = op(ctx)
		if lastErr == nil {
			return nil
		}
		if !retryable(lastErr) {
			return lastErr
		}
		if attempt == p.Attempts {
			break
		}

		delay := backoff(p, attempt)
		if p.Deadline > 0 {
			if remaining := p.Deadline - elapsed(ctx); remaining <= delay {
				// No window left for another attempt: fail now
				// instead of sleeping past the deadline.
				return fmt.Errorf("retry window exhausted after attempt %d: %w", attempt, lastErr)
			}
		}
		p.Sleep(delay)
	}
	if lastErr == nil {
		lastErr = errors.New("retry policy exhausted")
	}
	return fmt.Errorf("after %d attempts: %w", p.Attempts, lastErr)
}

// elapsed reports how much of the deadline has been consumed. The
// context's own machinery is the clock — tests inject deadlines the same
// way production does.
func elapsed(ctx context.Context) time.Duration {
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}
	return 0
}

// backoff computes the attempt's delay: BaseDelay * Factor^(attempt-1),
// capped at MaxDelay, plus up to 20% jitter.
func backoff(p Policy, attempt int) time.Duration {
	d := p.BaseDelay
	for i := 1; i < attempt; i++ {
		d = time.Duration(float64(d) * p.Factor)
		if d >= p.MaxDelay {
			return p.MaxDelay
		}
	}
	if d > p.MaxDelay {
		d = p.MaxDelay
	}
	// Jitter: 0.9x..1.1x. A fleet rebooting together must not stampede
	// the metadata service or the lease table in lockstep.
	jitter := 0.9 + 0.2*rand.Float64()
	return time.Duration(float64(d) * jitter)
}

// Clock abstracts time for the engine (step timestamps, durations). The
// default is the wall clock; tests run the pipeline instantly.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
}

type wallClock struct{}

func (wallClock) Now() time.Time                  { return time.Now() }
func (wallClock) Since(t time.Time) time.Duration { return time.Since(t) }

// NewWallClock returns the production clock.
func NewWallClock() Clock { return wallClock{} }

// Criticality preserves the bash script's `set -e` vs `|| echo WARNING`
// split as a first-class property of each step.
type Criticality int

const (
	// Critical: failure stops the boot (exit non-zero) so the ASG
	// replaces the node. Disks, secrets, render, start.
	Critical Criticality = iota
	// BestEffort: failure logs a loud WARNING and the boot continues —
	// a reader booting while a peer is down must still join (the gate
	// wedge was a real ASG boot-loop incident).
	BestEffort
)

func (c Criticality) String() string {
	if c == BestEffort {
		return "best-effort"
	}
	return "critical"
}

// Step is one convergent boot operation. Steps probe real state before
// acting (idempotent), so a re-run resumes.
type Step interface {
	// Name is the step identity in the state file and [TIMING] output.
	Name() string
	// Criticality declares the failure semantics.
	Criticality() Criticality
	// Policy is the retry contract for the step's operations.
	Policy() Policy
	// Run executes the step once (retries wrap it).
	Run(ctx context.Context) error
}

// StepFunc adapts a function to Step. Most steps are closures over the
// spec, provider and host; the struct form is reserved for steps that
// need their own state.
func StepFunc(name string, crit Criticality, policy Policy, run func(ctx context.Context) error) Step {
	return stepFunc{name: name, crit: crit, policy: policy, run: run}
}

type stepFunc struct {
	name   string
	crit   Criticality
	policy Policy
	run    func(ctx context.Context) error
}

func (s stepFunc) Name() string                  { return s.name }
func (s stepFunc) Criticality() Criticality      { return s.crit }
func (s stepFunc) Policy() Policy                { return s.policy }
func (s stepFunc) Run(ctx context.Context) error { return s.run(ctx) }

// Engine runs the ordered pipeline with state tracking, retries and the
// [TIMING] summary.
type Engine struct {
	clock   Clock
	nowFunc func() time.Time
	// stateStore persists step results; nil → in-memory only (tests).
	stateStore StateStore
	// snapshot, when set, is persisted with the state so the job
	// subcommands reload the settled identity (nil → nothing extra
	// is written; unit tests exercise the engine without identity).
	snapshot *bootIdentity
	// specHash invalidates prior step results: a run against a NEW
	// bundle hash re-runs every step (re-render), a re-run against the
	// SAME hash resumes (skips done steps).
	specHash string
	// shared is the ONE in-memory state object the run and the
	// identity persister both mutate. Without it the persister's
	// Load+Save races the run's Load-at-start+Save-per-step: the
	// run's stale copy (loaded BEFORE settle_identity) overwrites
	// the persisted identity on the very next step record — the
	// live chinit-cutover finding (schema-sync found "no settled
	// identity" in a state.json whose node-env proved the settle
	// had run).
	shared *modelState
}

// StateStore persists the boot state file.
type StateStore interface {
	Load() (*modelState, error)
	Save(st *modelState) error
}

// NewEngine builds an engine. nil clock/store use the defaults.
func NewEngine(clock Clock, store StateStore) *Engine {
	if clock == nil {
		clock = NewWallClock()
	}
	return &Engine{clock: clock, stateStore: store}
}

// WithIdentity pins the settled-identity snapshot and the spec hash the
// run executes against (the boot sets both after the identity step; the
// state file carries them for the job subcommands and for resume
// invalidation).
func (e *Engine) WithIdentity(identity *model.NodeIdentity, cloud, specHash string) *Engine {
	if identity != nil {
		e.snapshot = toBootIdentity(identity, cloud)
	}
	e.specHash = specHash
	return e
}

// IdentityPersister returns a closure the boot's settle step calls the
// moment identity is settled: it merges the snapshot into the loaded
// state and saves immediately (a later crash must not lose it, and the
// job subcommands read the identity from the state file). No-op when
// the engine has no store (unit tests).
func (e *Engine) IdentityPersister(cloud string) func(model.NodeIdentity) {
	if e.stateStore == nil {
		return nil
	}
	return func(identity model.NodeIdentity) {
		// Mutate the SHARED state (the run's copy), then save: the
		// run's next per-step Save must carry the identity, not
		// clobber it. A nil shared state (persister called outside
		// a run) falls back to a fresh load.
		var state *modelState
		if e.shared != nil {
			state = e.shared
		} else if loaded, err := e.stateStore.Load(); err == nil && loaded != nil {
			state = loaded
		} else {
			state = &modelState{}
		}
		state.Identity = toBootIdentity(&identity, cloud)
		if e.specHash != "" {
			state.SpecHash = e.specHash
		}
		_ = e.stateStore.Save(state)
	}
}

// toBootIdentity maps the domain identity onto the storage shape.
func toBootIdentity(identity *model.NodeIdentity, cloud string) *bootIdentity {
	return &bootIdentity{
		Role:        string(identity.Role),
		Hostname:    identity.Hostname,
		InstanceID:  identity.InstanceID,
		PrivateIP:   identity.PrivateIP,
		Region:      identity.Region,
		ClusterName: identity.ClusterName,
		DNSZone:     identity.DNSZone,
		CRName:      identity.CRName,
		CROrdinal:   identity.CROrdinal,
		ZoneID:      identity.ZoneID,
		LeaseTable:  identity.LeaseTable,
		Cloud:       cloud,
	}
}

// RunResult is the pipeline outcome: every step's status and whether the
// boot overall succeeded.
type RunResult struct {
	Steps    []StepOutcome
	Fatality error // the Critical failure, if any
}

// StepOutcome is one step's engine-level result.
type StepOutcome struct {
	Name     string
	Crit     Criticality
	Duration time.Duration
	Err      error
	Skipped  bool
}

// Failed reports whether the step failed (BestEffort failures included:
// the summary must surface them even though the boot continued).
func (o StepOutcome) Failed() bool { return o.Err != nil }

// Run executes the steps in order, honoring: resume (skip steps the
// state file records done), retries (each step's Policy), criticality
// (stop or continue), and the fatal-error short-circuit. A Critical
// failure aborts remaining steps; a BestEffort failure continues.
func (e *Engine) Run(ctx context.Context, steps []Step) RunResult {
	res := RunResult{Steps: make([]StepOutcome, 0, len(steps))}

	var state *modelState
	if e.stateStore != nil {
		if loaded, err := e.stateStore.Load(); err == nil && loaded != nil {
			state = loaded
		}
	}
	if state == nil {
		state = &modelState{}
	}
	// The identity persister and this run share ONE state object so
	// their Save calls cannot clobber each other (see Engine.shared).
	e.shared = state

	// Spec-hash invalidation: a step recorded done under a DIFFERENT
	// bundle hash is stale (the configs it installed are not what the
	// current release vouches for) — wipe the records and re-run.
	if e.specHash != "" && state.SpecHash != "" && state.SpecHash != e.specHash {
		state.Steps = nil
	}
	if e.snapshot != nil {
		state.Identity = e.snapshot
	}
	if e.specHash != "" {
		state.SpecHash = e.specHash
	}

	for _, st := range steps {
		outcome := StepOutcome{Name: st.Name(), Crit: st.Criticality()}

		// Resume: a step recorded done (and not superseded by a newer
		// spec hash — the caller re-runs deliberately when it wants
		// re-execution, e.g. a new bundle hash) is skipped.
		if prior := state.findStep(st.Name()); prior != nil && prior.Status == "done" {
			outcome.Skipped = true
			res.Steps = append(res.Steps, outcome)
			continue
		}

		start := e.clock.Now()
		err := Do(ctx, st.Policy(), st.Run)
		outcome.Duration = e.clock.Since(start)
		outcome.Err = err

		// Record: done / failed. Fatal errors annotate the step name.
		var fe *FatalError
		if errors.As(err, &fe) {
			fe.Step = st.Name()
		}
		state.recordStep(stepRecord{
			Name:       st.Name(),
			Status:     statusOf(err),
			DurationMs: outcome.Duration.Milliseconds(),
			StartedAt:  start.UTC().Format(time.RFC3339),
		})
		if err != nil && e.stateStore != nil {
			// Persist even on failure: the next run must know what
			// succeeded, and triage wants the step timeline.
			_ = e.stateStore.Save(state)
		}
		res.Steps = append(res.Steps, outcome)

		if err != nil {
			if st.Criticality() == Critical {
				res.Fatality = err
				// Persist the final state before exiting.
				if e.stateStore != nil {
					_ = e.stateStore.Save(state)
				}
				return res
			}
			// BestEffort: log-and-continue is the caller's job; the
			// outcome is in the result for the summary.
		}
	}

	if e.stateStore != nil {
		_ = e.stateStore.Save(state)
	}
	return res
}

// Since is the Clock's duration helper.
func (e *Engine) Since(t time.Time) time.Duration { return e.clock.Now().Sub(t) }

func statusOf(err error) string {
	if err == nil {
		return "done"
	}
	return "failed"
}

// modelState / stepRecord are the engine's storage view of the state
// file (kept separate from the model types so the engine owns resume
// semantics and the model stays a plain data description).
type modelState struct {
	// Identity is the settled snapshot (claimed/derived hostname, CR
	// name, lease, zone) — the recurring-job subcommands reload it
	// instead of re-deriving (the node-env contract, typed).
	Identity *bootIdentity `json:"identity,omitempty"`
	// SpecHash is the config bundle SHA the steps ran against; a new
	// hash invalidates prior results so the pipeline re-renders.
	SpecHash string       `json:"spec_hash,omitempty"`
	Steps    []stepRecord `json:"steps"`
}

// bootIdentity is the persisted subset of model.NodeIdentity (kept
// local to the engine so the storage shape evolves independently of the
// domain type).
type bootIdentity struct {
	Role        string `json:"role"`
	Hostname    string `json:"hostname"`
	InstanceID  string `json:"instance_id"`
	PrivateIP   string `json:"private_ip"`
	Region      string `json:"region"`
	ClusterName string `json:"cluster_name"`
	DNSZone     string `json:"dns_zone"`
	CRName      string `json:"cr_name,omitempty"`
	CROrdinal   int    `json:"cr_ordinal"`
	ZoneID      string `json:"zone_id,omitempty"`
	LeaseTable  string `json:"lease_table,omitempty"`
	Cloud       string `json:"cloud,omitempty"`
}

type stepRecord struct {
	Name       string `json:"name"`
	Status     string `json:"status"`
	DurationMs int64  `json:"duration_ms"`
	StartedAt  string `json:"started_at"`
}

func (m *modelState) findStep(name string) *stepRecord {
	for i := range m.Steps {
		if m.Steps[i].Name == name {
			return &m.Steps[i]
		}
	}
	return nil
}

func (m *modelState) recordStep(r stepRecord) {
	if prior := m.findStep(r.Name); prior != nil {
		*prior = r
		return
	}
	m.Steps = append(m.Steps, r)
}

// FormatTimingSummary prints the [TIMING] block the bash script printed
// (dashboards grep the format; chinit keeps it byte-compatible in shape).
func FormatTimingSummary(w io.Writer, res RunResult) {
	total := time.Duration(0)
	for _, s := range res.Steps {
		total += s.Duration
	}
	fmt.Fprintln(w, "============================================================================")
	fmt.Fprintln(w, "[TIMING] CLOUD-INIT TIMING SUMMARY")
	fmt.Fprintln(w, "============================================================================")
	for _, s := range res.Steps {
		if s.Skipped {
			fmt.Fprintf(w, "[TIMING]   %-40s %5s (skipped: already done)\n", s.Name, "0s")
			continue
		}
		fmt.Fprintf(w, "[TIMING]   %-40s %4ds\n", s.Name, int(s.Duration.Seconds()))
	}
	fmt.Fprintln(w, "============================================================================")
	fmt.Fprintf(w, "[TIMING] Total cloud-init time: %ds\n", int(total.Seconds()))
	fmt.Fprintln(w, "============================================================================")
}

// DescribeFailures renders the failed steps (WARNING lines for
// BestEffort, ERROR for the Critical one) — the boot log's triage section.
func DescribeFailures(w io.Writer, res RunResult) {
	for _, s := range res.Steps {
		if s.Err == nil {
			continue
		}
		if res.Fatality != nil && s.Name == res.FatalityStep() {
			fmt.Fprintf(w, "ERROR: critical step %s failed: %s — boot aborted\n", s.Name, s.Err)
			continue
		}
		fmt.Fprintf(w, "WARNING: best-effort step %s failed: %s — boot continues\n", s.Name, s.Err)
	}
}

// FatalityStep returns the name of the step whose Critical failure aborted
// the run ("" when the run completed).
func (r RunResult) FatalityStep() string {
	var fe *FatalError
	if errors.As(r.Fatality, &fe) && fe.Step != "" {
		return fe.Step
	}
	return ""
}

// errStep is a helper for tests: a step that always fails.
func errStep(name string, crit Criticality) Step {
	return StepFunc(name, crit, Policy{Attempts: 1}, func(context.Context) error {
		return errors.New(name + " failed")
	})
}

var _ = strings.TrimSpace
