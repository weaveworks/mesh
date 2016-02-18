package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func TestStateConvergence(t *testing.T) {
	var (
		seed  = time.Now().UnixNano()
		r     = rand.New(rand.NewSource(seed))
		nKeys = r.Intn(100) + 100
		nVals = r.Intn(10) + 5
		ops   = []map[string]int{}
		final = map[string]int{}
	)

	// For each key, generate nVals values.
	// Remember the largest: it should "stick".

	for i := 0; i < nKeys; i++ {
		key := strconv.Itoa(i)
		maxval := 0
		for j := 0; j < nVals; j++ {
			val := r.Intn(nVals * 100)
			if val > maxval {
				maxval = val
			}
			ops = append(ops, map[string]int{key: int(val)})
		}
		final[key] = maxval
	}

	// No matter what order the ops are applied,
	// the end result should be the same:
	// each key should have its maxval.

	for i := 0; i < 10; i++ {
		st := newState()
		var debug bytes.Buffer

		merge := func(index int) {
			st.Merge(newState().mergeComplete(ops[index]))
			fmt.Fprintf(&debug, "Merge %v\n", ops[index])
		}

		// Random order + some duplication
		for _, index := range r.Perm(len(ops)) {
			merge(index)
			if r.Intn(100) < 10 {
				merge(index)
			}
		}

		for key, want := range final {
			if have := st.set[key]; want != have {
				t.Logf("seed=%d, nKeys=%d, nVals=%d", seed, nKeys, nVals)
				t.Logf("%s", debug.String())
				t.Fatalf("%q: want %d, have %d", key, want, have)
			}
		}
	}
}

func TestStateMergeReceived(t *testing.T) {
	for _, testcase := range []struct {
		initial map[string]int
		merge   map[string]int
		want    map[string]int
	}{
		{
			map[string]int{},
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
		},
		{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
			map[string]int{},
		},
		{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"c": 3},
			map[string]int{"c": 3},
		},
		{
			map[string]int{"b": 3},
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1}, // we drop keys that don't semantically merge
		},
	} {
		initial, merge := testcase.initial, testcase.merge // mergeReceived modifies arguments
		delta := newState().mergeComplete(initial).(*state).mergeReceived(merge)
		if want, have := testcase.want, delta.(*state).set; !reflect.DeepEqual(want, have) {
			t.Errorf("%v mergeReceived %v: want %v, have %v", testcase.initial, testcase.merge, want, have)
		}
	}
}

func TestStateMergeDelta(t *testing.T) {
	for _, testcase := range []struct {
		initial map[string]int
		merge   map[string]int
		want    map[string]int
	}{
		{
			map[string]int{},
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
		},
		{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
			nil,
		},
		{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"c": 3},
			map[string]int{"c": 3},
		},
		{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"b": 3},
			map[string]int{"b": 3},
		},
	} {
		initial, merge := testcase.initial, testcase.merge // mergeDelta modifies arguments
		delta := newState().mergeComplete(initial).(*state).mergeDelta(merge)
		if want := testcase.want; want == nil {
			if delta != nil {
				t.Errorf("%v mergeDelta %v: want nil, have non-nil", testcase.initial, testcase.merge)
			}
		} else {
			if have := delta.(*state).set; !reflect.DeepEqual(want, have) {
				t.Errorf("%v mergeDelta %v: want %v, have %v", testcase.initial, testcase.merge, want, have)
			}
		}
	}
}

func TestStateMergeComplete(t *testing.T) {
	for _, testcase := range []struct {
		initial map[string]int
		merge   map[string]int
		want    map[string]int
	}{
		{
			map[string]int{},
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
		},
		{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
		},
		{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"c": 3},
			map[string]int{"a": 1, "b": 2, "c": 3},
		},
		{
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 0, "b": 3},
			map[string]int{"a": 1, "b": 3},
		},
	} {
		st := newState().mergeComplete(testcase.initial).(*state).mergeComplete(testcase.merge).(*state)
		if want, have := testcase.want, st.set; !reflect.DeepEqual(want, have) {
			t.Errorf("%v mergeComplete %v: want %v, have %v", testcase.initial, testcase.merge, want, have)
		}
	}
}
