/*
Copyright 2026 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package transform

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ilhamster/tracey/test_trace"
	"github.com/ilhamster/tracey/trace"
)

func checkORSpanTimes(t *testing.T,
	tr trace.Trace[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload],
	want map[string][2]time.Duration,
) {
	t.Helper()
	if err := trace.Check(tr, false); err != nil {
		t.Fatalf("invalid trace: %v\n%s", err, testtrace.TPP.PrettyPrintTraceSpans(tr))
	}
	if len(tr.RootSpans()) != len(want) {
		t.Fatalf("got %d spans, want %d", len(tr.RootSpans()), len(want))
	}
	for _, span := range tr.RootSpans() {
		name := span.Payload().String()
		if got := [2]time.Duration{span.Start(), span.End()}; got != want[name] {
			t.Errorf("span %s: got %v, want %v", name, got, want[name])
		}
		for _, es := range span.ElementarySpans() {
			if dep := es.Incoming(); dep != nil && dep.Options().Includes(trace.MultipleOriginsWithOrSemantics) {
				if len(dep.Origins()) != 2 {
					t.Errorf("OR dependency %s: got %d origins, want 2", dep.Payload(), len(dep.Origins()))
				}
			}
		}
	}
}

func TestTransformORWithLateOrigin(t *testing.T) {
	for _, downstream := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			for _, delay := range []time.Duration{0, 5} {
				for _, scale := range []float64{0, 1, 4} {
					t.Run(fmt.Sprintf("downstream_%t/reverse_%t/delay_%d/scale_%g", downstream, reverse, delay, scale), func(t *testing.T) {
						roots := []testtrace.RootSpanFn{
							testtrace.RootSpan(0, 10, "A", testtrace.ParentCategories()),
							testtrace.RootSpan(10+delay, 20+delay, "W", testtrace.ParentCategories()),
							testtrace.RootSpan(15+delay, 30+delay, "B", testtrace.ParentCategories()),
						}
						if reverse {
							slices.Reverse(roots)
						}
						builder := testtrace.NewTestingTraceBuilder(t).WithRootSpans(roots...).
							WithDependency(testtrace.Send, "OR join", trace.MultipleOriginsWithOrSemantics,
								testtrace.Origin(testtrace.Paths("A"), 10),
								testtrace.Origin(testtrace.Paths("B"), 30+delay),
								testtrace.Destination(testtrace.Paths("W"), 10+delay))
						if downstream {
							builder.WithDependency(testtrace.Signal, "W releases B",
								testtrace.Origin(testtrace.Paths("W"), 15+delay),
								testtrace.Destination(testtrace.Paths("B"), 15+delay))
						}
						original := builder.Build()
						if err := trace.Check(original, false); err != nil {
							t.Fatalf("invalid original: %v", err)
						}
						transformed, err := New[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload]().
							WithSpansScaledBy(spanFinderPattern(t, "A"), scale).
							TransformTrace(original)
						if err != nil {
							t.Fatalf("transform: %v", err)
						}
						aEnd := time.Duration(10 * scale)
						wStart := aEnd + delay
						bStart := 15 + delay
						if downstream {
							bStart = wStart + 5
						} else {
							wStart = min(wStart, bStart+15+delay)
						}
						checkORSpanTimes(t, transformed, map[string][2]time.Duration{
							"A": {0, aEnd}, "W": {wStart, wStart + 10}, "B": {bStart, bStart + 15},
						})
					})
				}
			}
		}
	}
}

func TestTransformInteractingORDependencies(t *testing.T) {
	// W waits for A or V; V waits for Q or W. Releasing both OR destinations
	// at a stall would incorrectly start V at 100 rather than 20. After
	// slowing A, V must instead release first, making V the origin that starts W.
	for _, test := range []struct {
		name           string
		scale          float64
		gateWUntilVEnd bool
		wStart, vStart time.Duration
	}{
		{name: "identity", scale: 1, wStart: 10, vStart: 20},
		{name: "changed_winner", scale: 20, wStart: 110, vStart: 100},
		{name: "first_candidate_gated", scale: 1, gateWUntilVEnd: true, wStart: 110, vStart: 100},
	} {
		t.Run(test.name, func(t *testing.T) {
			original := testtrace.NewTestingTraceBuilder(t).WithRootSpans(
				testtrace.RootSpan(0, 10, "A", testtrace.ParentCategories()),
				testtrace.RootSpan(0, 100, "Q", testtrace.ParentCategories()),
				testtrace.RootSpan(10, 20, "W", testtrace.ParentCategories()),
				testtrace.RootSpan(20, 30, "V", testtrace.ParentCategories()),
			).WithDependency(testtrace.Send, "W join", trace.MultipleOriginsWithOrSemantics,
				testtrace.Origin(testtrace.Paths("A"), 10),
				testtrace.Origin(testtrace.Paths("V"), 30),
				testtrace.Destination(testtrace.Paths("W"), 10),
			).WithDependency(testtrace.Send, "V join", trace.MultipleOriginsWithOrSemantics,
				testtrace.Origin(testtrace.Paths("Q"), 100),
				testtrace.Origin(testtrace.Paths("W"), 20),
				testtrace.Destination(testtrace.Paths("V"), 20),
			).Build()
			if err := trace.Check(original, false); err != nil {
				t.Fatalf("invalid original: %v", err)
			}
			transformation := New[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload]().
				WithSpansScaledBy(spanFinderPattern(t, "A"), test.scale)
			if test.gateWUntilVEnd {
				transformation.WithSpansGatedBy(spanFinderPattern(t, "W"),
					func() SpanGater[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload] {
						return &gateUntilVEnds{}
					})
			}
			transformed, err := transformation.TransformTrace(original)
			if err != nil {
				t.Fatalf("transform: %v", err)
			}
			checkORSpanTimes(t, transformed, map[string][2]time.Duration{
				"A": {0, time.Duration(10 * test.scale)}, "Q": {0, 100},
				"W": {test.wStart, test.wStart + 10}, "V": {test.vStart, test.vStart + 10},
			})
		})
	}
}

type gateUntilVEnds struct{ ended bool }

func (*gateUntilVEnds) SpanStarting(trace.Span[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload], bool) {
}

func (gate *gateUntilVEnds) SpanEnding(span trace.Span[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload], _ bool) {
	if span.Payload().String() == "V" {
		gate.ended = true
	}
}

func (gate *gateUntilVEnds) SpanCanStart(trace.Span[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload]) bool {
	return gate.ended
}

func TestTransformORDoesNotBypassUnresolvedPredecessor(t *testing.T) {
	original := testtrace.NewTestingTraceBuilder(t).WithRootSpans(
		testtrace.RootSpan(0, 10, "A", testtrace.ParentCategories()),
		testtrace.RootSpan(0, 20, "B", testtrace.ParentCategories()),
		testtrace.RootSpan(0, 40, "W", testtrace.ParentCategories()),
		testtrace.RootSpan(20, 50, "P", testtrace.ParentCategories()),
		testtrace.RootSpan(35, 60, "Q", testtrace.ParentCategories()),
	).WithSuspend(testtrace.Paths("W"), 20, 30).
		WithDependency(testtrace.Send, "first join", trace.MultipleOriginsWithOrSemantics,
			testtrace.Origin(testtrace.Paths("A"), 10),
			testtrace.Origin(testtrace.Paths("P"), 50),
			testtrace.Destination(testtrace.Paths("W"), 10)).
		WithDependency(testtrace.Send, "second join", trace.MultipleOriginsWithOrSemantics,
			testtrace.Origin(testtrace.Paths("B"), 20),
			testtrace.Origin(testtrace.Paths("Q"), 60),
			testtrace.Destination(testtrace.Paths("W"), 30)).
		WithDependency(testtrace.Signal, "release P",
			testtrace.Origin(testtrace.Paths("W"), 20), testtrace.Destination(testtrace.Paths("P"), 20)).
		WithDependency(testtrace.Signal, "release Q",
			testtrace.Origin(testtrace.Paths("W"), 35), testtrace.Destination(testtrace.Paths("Q"), 35)).Build()
	if err := trace.Check(original, false); err != nil {
		t.Fatalf("invalid original: %v", err)
	}
	transformed, err := New[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload]().
		WithSpansScaledBy(spanFinderPattern(t, "A"), 10).TransformTrace(original)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	checkORSpanTimes(t, transformed, map[string][2]time.Duration{
		"A": {0, 100}, "B": {0, 20}, "W": {0, 120}, "P": {110, 140}, "Q": {115, 140},
	})
}

func TestTransformORStillHonorsGatesAndRequiredDependencies(t *testing.T) {
	original := testtrace.NewTestingTraceBuilder(t).WithRootSpans(
		testtrace.RootSpan(0, 10, "A", testtrace.ParentCategories()),
		testtrace.RootSpan(10, 20, "W", testtrace.ParentCategories()),
		testtrace.RootSpan(15, 30, "B", testtrace.ParentCategories()),
	).WithDependency(testtrace.Send, "OR join", trace.MultipleOriginsWithOrSemantics,
		testtrace.Origin(testtrace.Paths("A"), 10), testtrace.Origin(testtrace.Paths("B"), 30),
		testtrace.Destination(testtrace.Paths("W"), 10)).
		WithDependency(testtrace.Signal, "W releases B",
			testtrace.Origin(testtrace.Paths("W"), 15), testtrace.Destination(testtrace.Paths("B"), 15)).Build()
	t.Run("allowed_gate", func(t *testing.T) {
		transformed, err := New[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload]().
			WithSpansGatedBy(spanFinderPattern(t, "W"),
				NewConcurrencyLimiter[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload](1)).
			TransformTrace(original)
		if err != nil {
			t.Fatalf("transform: %v", err)
		}
		checkORSpanTimes(t, transformed, map[string][2]time.Duration{
			"A": {0, 10}, "W": {10, 20}, "B": {15, 30},
		})
	})
	t.Run("gated", func(t *testing.T) {
		_, err := New[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload]().
			WithSpansGatedBy(spanFinderPattern(t, "W"),
				NewConcurrencyLimiter[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload](0)).
			TransformTrace(original)
		if err == nil || !strings.Contains(err.Error(), "remain gated") {
			t.Fatalf("got %v, want unresolved gate error", err)
		}
	})
	t.Run("no_resolved_origin", func(t *testing.T) {
		_, err := New[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload]().
			WithAddedDependencies(posSpec(t, "W @0%"), posSpec(t, "A @0%"), testtrace.Signal, 0).
			TransformTrace(original)
		if err == nil || !strings.Contains(err.Error(), "could not transform") {
			t.Fatalf("got %v, want unresolved dependency error", err)
		}
	})
}

func TestTransformORRejectsRetroactiveResolution(t *testing.T) {
	// The input is valid. A negative scaling of the return signal introduces
	// time travel, allowing B's later-scheduled end to move W's start earlier
	// after W has already run. Fail rather than silently returning stale times.
	original := testtrace.NewTestingTraceBuilder(t).WithRootSpans(
		testtrace.RootSpan(0, 100, "A", testtrace.ParentCategories()),
		testtrace.RootSpan(100, 110, "W", testtrace.ParentCategories()),
		testtrace.RootSpan(200, 201, "B", testtrace.ParentCategories()),
	).WithDependency(testtrace.Send, "OR join", trace.MultipleOriginsWithOrSemantics,
		testtrace.Origin(testtrace.Paths("A"), 100), testtrace.Origin(testtrace.Paths("B"), 201),
		testtrace.Destination(testtrace.Paths("W"), 100)).
		WithDependency(testtrace.Signal, "W releases B",
			testtrace.Origin(testtrace.Paths("W"), 110), testtrace.Destination(testtrace.Paths("B"), 200)).Build()
	if err := trace.Check(original, false); err != nil {
		t.Fatalf("invalid original: %v", err)
	}
	_, err := New[time.Duration, testtrace.StringPayload, testtrace.StringPayload, testtrace.StringPayload]().
		WithDependenciesScaledBy(nil, nil, []trace.DependencyType{testtrace.Signal}, -1).
		TransformTrace(original)
	if err == nil || !strings.Contains(err.Error(), "late OR origin") {
		t.Fatalf("got %v, want retroactive OR resolution error", err)
	}
}
