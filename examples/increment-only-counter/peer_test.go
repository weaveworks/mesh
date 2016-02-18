package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"reflect"
	"testing"

	"github.com/weaveworks/mesh"
)

func TestPeerOnGossip(t *testing.T) {
	for _, testcase := range []struct {
		initial map[string]int
		msg     map[string]int
		want    map[string]int
	}{
		{
			map[string]int{},
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
		},
		{
			map[string]int{"a": 1},
			map[string]int{"a": 0, "b": 2},
			map[string]int{"b": 2},
		},
		{
			map[string]int{"a": 9},
			map[string]int{"a": 8},
			nil,
		},
	} {
		p := newPeer(log.New(ioutil.Discard, "", 0))
		p.st.mergeComplete(testcase.initial)
		buf, _ := json.Marshal(testcase.msg)
		delta, err := p.OnGossip(buf)
		if err != nil {
			t.Errorf("%v OnGossip %v: %v", testcase.initial, testcase.msg, err)
			continue
		}
		if want := testcase.want; want == nil {
			if delta != nil {
				t.Errorf("%v OnGossip %v: want nil, have non-nil", testcase.initial, testcase.msg)
			}
		} else {
			if have := delta.(*state).set; !reflect.DeepEqual(want, have) {
				t.Errorf("%v OnGossip %v: want %v, have %v", testcase.initial, testcase.msg, want, have)
			}
		}
	}
}

func TestPeerOnGossipBroadcast(t *testing.T) {
	for _, testcase := range []struct {
		initial map[string]int
		msg     map[string]int
		want    map[string]int
	}{
		{
			map[string]int{},
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
		},
		{
			map[string]int{"a": 1},
			map[string]int{"a": 0, "b": 2},
			map[string]int{"b": 2},
		},
		{
			map[string]int{"a": 9},
			map[string]int{"a": 8},
			map[string]int{}, // OnGossipBroadcast returns received, which should never be nil
		},
	} {
		p := newPeer(log.New(ioutil.Discard, "", 0))
		p.st.mergeComplete(testcase.initial)
		buf, _ := json.Marshal(testcase.msg)
		delta, err := p.OnGossipBroadcast(mesh.UnknownPeerName, buf)
		if err != nil {
			t.Errorf("%v OnGossipBroadcast %v: %v", testcase.initial, testcase.msg, err)
			continue
		}
		if want, have := testcase.want, delta.(*state).set; !reflect.DeepEqual(want, have) {
			t.Errorf("%v OnGossipBroadcast %v: want %v, have %v", testcase.initial, testcase.msg, want, have)
		}
	}
}

func TestPeerOnGossipUnicast(t *testing.T) {
	for _, testcase := range []struct {
		initial map[string]int
		msg     map[string]int
		want    map[string]int
	}{
		{
			map[string]int{},
			map[string]int{"a": 1, "b": 2},
			map[string]int{"a": 1, "b": 2},
		},
		{
			map[string]int{"a": 1},
			map[string]int{"a": 0, "b": 2},
			map[string]int{"a": 1, "b": 2},
		},
		{
			map[string]int{"a": 9},
			map[string]int{"a": 8},
			map[string]int{"a": 9},
		},
	} {
		p := newPeer(log.New(ioutil.Discard, "", 0))
		p.st.mergeComplete(testcase.initial)
		buf, _ := json.Marshal(testcase.msg)
		if err := p.OnGossipUnicast(mesh.UnknownPeerName, buf); err != nil {
			t.Errorf("%v OnGossipBroadcast %v: %v", testcase.initial, testcase.msg, err)
			continue
		}
		if want, have := testcase.want, p.st.set; !reflect.DeepEqual(want, have) {
			t.Errorf("%v OnGossipBroadcast %v: want %v, have %v", testcase.initial, testcase.msg, want, have)
		}
	}
}
