/*
	Copyright 2025 Google Inc.

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

package trace

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestCheck(t *testing.T) {
	for _, test := range []struct {
		description string
		buildTrace  func(t *testing.T) Trace[time.Duration, payload, payload, payload]
		wantError   bool
	}{{
		description: "OK trace",
		buildTrace: func(t *testing.T) Trace[time.Duration, payload, payload, payload] {
			trace := NewTrace(
				DurationComparator,
				&testNamer{},
			)
			a := trace.NewRootSpan(0, 100, "")
			b, err := a.NewChildSpan(DurationComparator, 30, 70, "")
			if err != nil {
				t.Fatal(err.Error())
			}
			c := trace.NewRootSpan(40, 70, "")
			spawn := trace.NewDependency(FirstUserDefinedDependencyType, "")
			if err := spawn.SetOriginSpan(DurationComparator, b, 35); err != nil {
				t.Error(err.Error())
			}
			if err := spawn.AddDestinationSpan(DurationComparator, c, 40); err != nil {
				t.Error(err.Error())
			}
			ret := trace.NewDependency(FirstUserDefinedDependencyType+1, "")
			if err := ret.SetOriginSpan(DurationComparator, c, 70); err != nil {
				t.Error(err.Error())
			}
			if err := ret.AddDestinationSpan(DurationComparator, b, 70); err != nil {
				t.Error(err.Error())
			}
			return trace
		},
		wantError: false,
	}, {
		description: "partial dependency",
		buildTrace: func(t *testing.T) Trace[time.Duration, payload, payload, payload] {
			trace := NewTrace(
				DurationComparator,
				&testNamer{},
			)
			a := trace.NewRootSpan(0, 100, "")
			if err := trace.NewDependency(FirstUserDefinedDependencyType, "").
				SetOriginSpan(DurationComparator, a, 50); err != nil {
				t.Error(err.Error())
			}
			return trace
		},
		wantError: true,
	}, {
		description: "negative dependency edge",
		buildTrace: func(t *testing.T) Trace[time.Duration, payload, payload, payload] {
			trace := NewTrace(
				DurationComparator,
				&testNamer{},
			)
			a := trace.NewRootSpan(0, 100, "")
			b := trace.NewRootSpan(0, 100, "")
			dep := trace.NewDependency(FirstUserDefinedDependencyType, "")
			if err := dep.SetOriginSpan(DurationComparator, a, 70); err != nil {
				t.Error(err.Error())
			}
			if err := dep.AddDestinationSpan(DurationComparator, b, 30); err != nil {
				t.Error(err.Error())
			}
			return trace
		},
		wantError: true,
	}, {
		description: "cycle reachable from entry elementary spans",
		buildTrace: func(t *testing.T) Trace[time.Duration, payload, payload, payload] {
			trace := NewMutableTrace(
				DurationComparator,
				&testNamer{},
			)
			a0 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(0)
			a1 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(0)
			if _, err := trace.NewMutableRootSpan([]MutableElementarySpan[time.Duration, payload, payload, payload]{a0, a1}, "A"); err != nil {
				t.Error(err.Error())
			}
			b0 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(0)
			b1 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(0)
			if _, err := trace.NewMutableRootSpan([]MutableElementarySpan[time.Duration, payload, payload, payload]{b0, b1}, "B"); err != nil {
				t.Error(err.Error())
			}
			trace.NewMutableDependency(FirstUserDefinedDependencyType).
				WithOriginElementarySpan(DurationComparator, b1).
				WithDestinationElementarySpan(a1)
			trace.NewMutableDependency(FirstUserDefinedDependencyType).
				WithOriginElementarySpan(DurationComparator, a1).
				WithDestinationElementarySpan(b0)
			return trace
		},
		wantError: true,
	}, {
		description: "cycle unreachable from entry elementary spans",
		buildTrace: func(t *testing.T) Trace[time.Duration, payload, payload, payload] {
			trace := NewMutableTrace(
				DurationComparator,
				&testNamer{},
			)
			a0 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(0)
			a1 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(0)
			if _, err := trace.NewMutableRootSpan([]MutableElementarySpan[time.Duration, payload, payload, payload]{a0, a1}, "A"); err != nil {
				t.Error(err.Error())
			}
			b0 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(0)
			b1 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(0)
			if _, err := trace.NewMutableRootSpan([]MutableElementarySpan[time.Duration, payload, payload, payload]{b0, b1}, "B"); err != nil {
				t.Error(err.Error())
			}
			trace.NewMutableDependency(FirstUserDefinedDependencyType).
				WithOriginElementarySpan(DurationComparator, b1).
				WithDestinationElementarySpan(b0)
			return trace
		},
		wantError: true,
	}} {
		t.Run(test.description, func(t *testing.T) {
			tr := test.buildTrace(t)
			checkErr := Check(tr, true)
			if (checkErr != nil) != test.wantError {
				t.Errorf("Check() returned unexpected error %v", checkErr)
			}
		})
	}
}

func TestCheckDependencyAlsoHasSequentialPredecessor(t *testing.T) {
	for _, options := range []DependencyOption{DefaultDependencyOptions, MultipleOriginsWithAndSemantics, MultipleOriginsWithOrSemantics} {
		tr := NewMutableTrace(DurationComparator, &testNamer{})
		a0 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(10)
		a1 := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(10).WithEnd(20)
		if _, err := tr.NewMutableRootSpan([]MutableElementarySpan[time.Duration, payload, payload, payload]{a0, a1}, "A"); err != nil {
			t.Fatal(err)
		}
		dep := tr.NewMutableDependency(FirstUserDefinedDependencyType, options).
			WithOriginElementarySpan(DurationComparator, a0).
			WithDestinationElementarySpan(a1)
		if options != DefaultDependencyOptions {
			b := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(5)
			if _, err := tr.NewMutableRootSpan([]MutableElementarySpan[time.Duration, payload, payload, payload]{b}, "B"); err != nil {
				t.Fatal(err)
			}
			dep.WithOriginElementarySpan(DurationComparator, b)
		}
		if err := Check(tr, true); err != nil {
			t.Errorf("overlapping predecessor and origin (%v): %v", options, err)
		}
	}
}

func TestCheckORContinuationCycle(t *testing.T) {
	for _, test := range []struct {
		name              string
		options           DependencyOption
		independentOrigin bool
		blockedSequential bool
		wantError         bool
	}{
		{"OR with independent origin", MultipleOriginsWithOrSemantics, true, false, false},
		{"OR without independent origin", MultipleOriginsWithOrSemantics, false, false, true},
		{"OR with blocked sequential predecessor", MultipleOriginsWithOrSemantics, true, true, true},
		{"AND with independent origin", MultipleOriginsWithAndSemantics, true, false, true},
	} {
		for _, reverseRoots := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverseRoots=%t", test.name, reverseRoots), func(t *testing.T) {
				tr := NewMutableTrace(DurationComparator, &testNamer{})
				pre := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(20)
				join := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(20).WithEnd(20)
				child := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(20).WithEnd(20)
				independent := NewMutableElementarySpan[time.Duration, payload, payload, payload]().WithStart(0).WithEnd(20)
				type root struct {
					name payload
					es   []MutableElementarySpan[time.Duration, payload, payload, payload]
				}
				roots := []root{
					{"waiter", []MutableElementarySpan[time.Duration, payload, payload, payload]{pre, join}},
					{"child", []MutableElementarySpan[time.Duration, payload, payload, payload]{child}},
				}
				if test.independentOrigin {
					roots = append(roots, root{"independent", []MutableElementarySpan[time.Duration, payload, payload, payload]{independent}})
				}
				if reverseRoots {
					slices.Reverse(roots)
				}
				for _, root := range roots {
					if _, err := tr.NewMutableRootSpan(root.es, root.name); err != nil {
						t.Fatal(err)
					}
				}
				wait := tr.NewMutableDependency(FirstUserDefinedDependencyType, test.options).
					WithOriginElementarySpan(DurationComparator, child).
					WithDestinationElementarySpan(join)
				if test.independentOrigin {
					wait.WithOriginElementarySpan(DurationComparator, independent)
				}
				// The child completes as a consequence of the waiter's continuation,
				// at the same timestamp as the independent completion and join.
				continuation := tr.NewMutableDependency(FirstUserDefinedDependencyType).
					WithOriginElementarySpan(DurationComparator, join).
					WithDestinationElementarySpan(child)
				if test.blockedSequential {
					pre.WithStart(20)
					continuation.WithDestinationElementarySpan(pre)
				}
				if err := Check(tr, false); (err != nil) != test.wantError {
					t.Errorf("Check() = %v, wantError %t", err, test.wantError)
				}
			})
		}
	}
}
