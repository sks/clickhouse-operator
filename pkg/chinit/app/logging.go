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
	"context"
	"io"
	"log/slog"
	"os"
)

// chinit's structured logging: the boot log keeps its human-readable
// contract (cloud-init greps it) while the critical paths gain
// slog records — INFO on stdout, WARN/ERROR on stderr, the levels an
// operator's journal filters already expect. The [TIMING] block and
// DescribeFailures stay on their raw fmt paths on purpose: dashboards
// grep their exact format.
//
// Why a level-split handler instead of two loggers or a custom
// io.Writer: slog's Writer cannot see the record's level, so the split
// has to happen at the Handler layer. Wrapping two TextHandlers and
// dispatching by level is the whole mechanism (~30 lines) — no custom
// formatting to drift from slog's own output shape.

// InitLogging installs the process-wide level-split logger. It is
// idempotent: chinit is one process per subcommand, but Main dispatches
// every entry through here so the seam also serves tests that capture
// os.Stdout/os.Stderr (the writers below resolve the *os.File vars at
// record time — a swapped handle captures the records exactly like the
// raw fmt.Fprintf paths).
func InitLogging() {
	slog.SetDefault(slog.New(NewLevelSplitHandler(stdoutWriter{}, stderrWriter{})))
}

// stdoutWriter/stderrWriter indirect through the os.Stdout/os.Stderr
// VARIABLES (not the values captured at init): the existing suites
// capture output by swapping those vars, and the slog records must
// land in the captured file the same way the raw fmt lines do.
type stdoutWriter struct{}
type stderrWriter struct{}

func (stdoutWriter) Write(p []byte) (int, error) { return os.Stdout.Write(p) }
func (stderrWriter) Write(p []byte) (int, error) { return os.Stderr.Write(p) }

// levelSplitHandler routes records by severity: <= Info goes to the
// stdout handler, Warn and above to the stderr handler. Both write
// slog's standard text format.
type levelSplitHandler struct {
	info slog.Handler
	warn slog.Handler
}

// NewLevelSplitHandler builds the routing handler over two underlying
// TextHandlers (info → out, warn+ → err), both at LevelDebug so the
// route decision is the only filter.
func NewLevelSplitHandler(out, err io.Writer) slog.Handler {
	opts := &slog.HandlerOptions{Level: slog.LevelDebug}
	return &levelSplitHandler{
		info: slog.NewTextHandler(out, opts),
		warn: slog.NewTextHandler(err, opts),
	}
}

func (h *levelSplitHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= slog.LevelInfo
}

func (h *levelSplitHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level >= slog.LevelWarn {
		return h.warn.Handle(ctx, r)
	}
	return h.info.Handle(ctx, r)
}

func (h *levelSplitHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &levelSplitHandler{info: h.info.WithAttrs(attrs), warn: h.warn.WithAttrs(attrs)}
}

func (h *levelSplitHandler) WithGroup(name string) slog.Handler {
	return &levelSplitHandler{info: h.info.WithGroup(name), warn: h.warn.WithGroup(name)}
}
