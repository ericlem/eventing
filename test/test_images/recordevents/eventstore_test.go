/*
Copyright 2020 The Knative Authors
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

package main

import (
	"encoding/json"
	"strconv"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go"

	"knative.dev/eventing/test/test_images/recordevents/requests"
)

// Test that adding and getting a bunch of events stores them all
// and that retrieving the events retrieves the correct events.
func TestAddGetMany(t *testing.T) {
	es := newEventStore()

	count := 10009
	for i := 0; i < count; i++ {
		ce := cloudevents.NewEvent(cloudevents.VersionV1)
		ce.SetType("knative.dev.test.event.a")
		ce.SetSource("https://source.test.event.knative.dev/foo")
		ce.SetID(strconv.FormatInt(int64(i), 10))
		es.StoreEvent(ce, nil)
		minAvail := es.MinAvail()
		maxSeen := es.MaxSeen()
		if minAvail != 1 {
			t.Fatalf("Pass %d Bad min: %d, expected %d", i, minAvail, 1)
		}
		if maxSeen != i+1 {
			t.Fatalf("Pass %d Bad max: %d, expected %d", i, maxSeen, i+1)
		}

	}
	for i := 1; i <= count; i++ {
		evInfoBytes, err := es.GetEventInfoBytes(i)
		if err != nil {
			t.Fatalf("Error calling get on item %d (range 1-%d): %s", i, count, err)
		}
		if len(evInfoBytes) == 0 {
			t.Fatalf("Empty info bytes")
		}

		var evInfo requests.EventInfo
		err = json.Unmarshal(evInfoBytes, &evInfo)
		if err != nil {
			t.Fatalf("Error unmarshalling stored JSON: %s", err)
		}

		if evInfo.Event == nil {
			t.Fatalf("Unexpected empty event info event %d: %+v", i, evInfo)
		}
		if len(evInfo.ValidationError) != 0 {
			t.Fatalf("Unexpected error for stored event %d: %s", i, evInfo.ValidationError)
		}

		// Make sure it's the expected event
		seenID := evInfo.Event.ID()
		expectedID := strconv.FormatInt(int64(i-1), 10)
		if seenID != expectedID {
			t.Errorf("Incorrect id on retrieval: %s, expected %s", seenID, expectedID)
		}
	}
	_, err := es.GetEventInfoBytes(count + 1)
	if err == nil {
		t.Errorf("Unexpected non-error return for getinfo of %d", count+1)
	}

	_, err = es.GetEventInfoBytes(0)
	if err == nil {
		t.Errorf("Unexpected non-error return for getinfo of %d", 0)
	}

}

func TestEmpty(t *testing.T) {
	es := newEventStore()
	min := es.MinAvail()
	if min != -1 {
		t.Errorf("Invalid min: %d, expected %d", min, -1)
	}
	max := es.MaxSeen()
	if max != -1 {
		t.Errorf("Invalid max: %d, expected %d", max, -1)
	}

	for i := -2; i < 2; i++ {
		_, err := es.GetEventInfoBytes(0)
		if err == nil {
			t.Errorf("Unexpected non-error return for getinfo of %d", i)
		}
	}

}

func TestAddGetSingleValid(t *testing.T) {
	expectedType := "knative.dev.test.event.a"
	expectedSource := "https://source.test.event.knative.dev/foo"
	expectedID := "111"
	es := newEventStore()

	headers := make(map[string][]string)
	headers["foo"] = []string{"bar", "baz"}
	ce := cloudevents.NewEvent(cloudevents.VersionV1)
	ce.SetType(expectedType)
	ce.SetSource(expectedSource)
	ce.SetID(expectedID)
	es.StoreEvent(ce, headers)
	minAvail := es.MinAvail()
	maxSeen := es.MaxSeen()
	if minAvail != maxSeen {
		t.Fatalf("Expected match, saw %d, %d", minAvail, maxSeen)
	}

	evInfoBytes, err := es.GetEventInfoBytes(minAvail)
	if err != nil {
		t.Fatalf("Error calling get: %s", err)
	}
	var evInfo requests.EventInfo
	err = json.Unmarshal(evInfoBytes, &evInfo)
	if err != nil {
		t.Fatalf("Error unmarshalling stored JSON: %s", err)
	}

	if evInfo.Event == nil {
		t.Fatalf("Unexpected empty event info event: %+v", evInfo)
	}
	if len(evInfo.ValidationError) != 0 {
		t.Fatalf("Unexpected error for stored event: %s", evInfo.ValidationError)
	}
	if len(evInfo.HTTPHeaders) != 1 {
		t.Fatalf("Unexpected header contents for stored event: %+v", evInfo.HTTPHeaders)
	}
	if len(evInfo.HTTPHeaders["foo"]) != 2 {
		t.Fatalf("Unexpected header contents for stored event: %+v", evInfo.HTTPHeaders)
	}
	if evInfo.HTTPHeaders["foo"][0] != "bar" || evInfo.HTTPHeaders["foo"][1] != "baz" {
		t.Fatalf("Unexpected header contents for stored event: %+v", evInfo.HTTPHeaders)
	}
	seenID := evInfo.Event.ID()
	if seenID != expectedID {
		t.Errorf("Incorrect id on retrieval: %s, expected %s", seenID, expectedID)
	}
	seenSource := evInfo.Event.Source()
	if seenSource != expectedSource {
		t.Errorf("Incorrect source on retrieval: %s, expected %s", seenSource, expectedSource)
	}
	seenType := evInfo.Event.Type()
	if seenType != expectedType {
		t.Errorf("Incorrect type on retrieval: %s, expected %s", seenType, expectedType)
	}
}

func TestAddGetSingleInvalid(t *testing.T) {
	es := newEventStore()

	headers := make(map[string][]string)
	headers["foo"] = []string{"bar", "baz"}
	ce := cloudevents.NewEvent(cloudevents.VersionV1)
	ce.SetType("knative.dev.test.event.a")
	// No source
	ce.SetID("111")
	es.StoreEvent(ce, headers)
	minAvail := es.MinAvail()
	maxSeen := es.MaxSeen()
	if minAvail != maxSeen {
		t.Fatalf("Expected match, saw %d, %d", minAvail, maxSeen)
	}

	evInfoBytes, err := es.GetEventInfoBytes(minAvail)
	if err != nil {
		t.Fatalf("Error calling get: %s", err)
	}
	var evInfo requests.EventInfo
	err = json.Unmarshal(evInfoBytes, &evInfo)
	if err != nil {
		t.Fatalf("Error unmarshalling stored JSON: %s", err)
	}
	if evInfo.Event != nil {
		t.Fatalf("Unexpected event info: %+v", evInfo)
	}
	if len(evInfo.ValidationError) == 0 {
		t.Fatalf("Unexpected empty error for stored event: %s", evInfo.ValidationError)
	}
	if len(evInfo.HTTPHeaders) != 1 {
		t.Fatalf("Unexpected header contents for stored event: %+v", evInfo.HTTPHeaders)
	}
	if len(evInfo.HTTPHeaders["foo"]) != 2 {
		t.Fatalf("Unexpected header contents for stored event: %+v", evInfo.HTTPHeaders)
	}
	if evInfo.HTTPHeaders["foo"][0] != "bar" || evInfo.HTTPHeaders["foo"][1] != "baz" {
		t.Fatalf("Unexpected header contents for stored event: %+v", evInfo.HTTPHeaders)
	}
}
