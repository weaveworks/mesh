package main

import (
	"io/ioutil"
	"log"
	"testing"
	"time"
)

func TestStateMachineGet(t *testing.T) {
	sm := stateMachineFixture()
	for key, want := range map[string]string{
		"a": "123",
		"b": "456",
		"c": "",
	} {
		have, err := sm.get(key)
		if want != "" && err != nil {
			t.Errorf("get(%s): %v", key, err)
			continue
		}
		if want != have {
			t.Errorf("get(%s): want %q, have %q", key, want, have)
		}
	}
}

func TestStateMachineWatch(t *testing.T) {
	sm := stateMachineFixture()
	results := make(chan string, 1)
	cancel, err := sm.watch("a", results)
	if err != nil {
		t.Fatal(err)
	}

	val := "xyz"
	if err := sm.post("a", val); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-results:
		if want, have := val, result; want != have {
			t.Errorf("want %q, have %q", want, have)
		}
	default:
		t.Fatal("watch didn't deliver a result")
	}

	if err := sm.post("b", val); err != nil {
		t.Fatal(err)
	}
	select {
	case <-results:
		t.Errorf("watch delivered unexpected result")
	default:
	}

	close(cancel)
	select {
	case result, ok := <-results:
		if ok {
			t.Errorf("canceled watch, but got result %q", result)
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("canceled watch, but results chan is still open")
	}
}

func stateMachineFixture() *stateMachine {
	sm := newStateMachine(log.New(ioutil.Discard, "", 0))
	sm.post("a", "123")
	sm.post("b", "456")
	return sm
}
