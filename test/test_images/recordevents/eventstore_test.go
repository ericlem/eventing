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
	"strconv"
	"testing"

	cloudevents "github.com/cloudevents/sdk-go"
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
		es.StoreEvent(ce)
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
		evInfo, err := es.GetEventInfo(i)
		if err != nil {
			t.Fatalf("Error calling get on item %d (range 1-%d): %s", i, count, err)
		}
		if len(evInfo.EventJSON) == 0 {
			t.Fatalf("Unexpected empty event info event %d: %+v", i, evInfo)
		}
		if len(evInfo.ValidationError) != 0 {
			t.Fatalf("Unexpected error for stored event %d: %s", i, evInfo.ValidationError)
		}

		e := cloudevents.Event{}
		err = e.UnmarshalJSON(evInfo.EventJSON)
		if err != nil {
			t.Fatalf("Error unmarshalling stored JSON: %s", err)
		}
		// Make sure it's the expected event
		seenID := e.ID()
		expectedID := strconv.FormatInt(int64(i-1), 10)
		if seenID != expectedID {
			t.Errorf("Incorrect id on retrieval: %s, expected %s", seenID, expectedID)
		}
	}
	_, err := es.GetEventInfo(count + 1)
	if err == nil {
		t.Errorf("Unexpected non-error return for getinfo of %d", count+1)
	}

	_, err = es.GetEventInfo(0)
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
		_, err := es.GetEventInfo(0)
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

	ce := cloudevents.NewEvent(cloudevents.VersionV1)
	ce.SetType(expectedType)
	ce.SetSource(expectedSource)
	ce.SetID(expectedID)
	es.StoreEvent(ce)
	minAvail := es.MinAvail()
	maxSeen := es.MaxSeen()
	if minAvail != maxSeen {
		t.Fatalf("Expected match, saw %d, %d", minAvail, maxSeen)
	}

	evInfo, err := es.GetEventInfo(minAvail)
	if err != nil {
		t.Fatalf("Error calling get: %s", err)
	}
	if len(evInfo.EventJSON) == 0 {
		t.Fatalf("Unexpected empty event info event: %+v", evInfo)
	}
	if len(evInfo.ValidationError) != 0 {
		t.Fatalf("Unexpected error for stored event: %s", evInfo.ValidationError)
	}

	e := cloudevents.Event{}
	err = e.UnmarshalJSON(evInfo.EventJSON)
	if err != nil {
		t.Fatalf("Error unmarshalling stored JSON: %s", err)
	}

	seenID := e.ID()
	if seenID != expectedID {
		t.Errorf("Incorrect id on retrieval: %s, expected %s", seenID, expectedID)
	}
	seenSource := e.Source()
	if seenSource != expectedSource {
		t.Errorf("Incorrect source on retrieval: %s, expected %s", seenSource, expectedSource)
	}
	seenType := e.Type()
	if seenType != expectedType {
		t.Errorf("Incorrect type on retrieval: %s, expected %s", seenType, expectedType)
	}
}

func TestAddGetSingleInvalid(t *testing.T) {
	es := newEventStore()

	ce := cloudevents.NewEvent(cloudevents.VersionV1)
	ce.SetType("knative.dev.test.event.a")
	// No source
	ce.SetID("111")
	es.StoreEvent(ce)
	minAvail := es.MinAvail()
	maxSeen := es.MaxSeen()
	if minAvail != maxSeen {
		t.Fatalf("Expected match, saw %d, %d", minAvail, maxSeen)
	}

	evInfo, err := es.GetEventInfo(minAvail)
	if err != nil {
		t.Fatalf("Error calling get: %s", err)
	}
	if len(evInfo.EventJSON) != 0 {
		t.Fatalf("Unexpected event info: %+v", evInfo)
	}
	if len(evInfo.ValidationError) == 0 {
		t.Fatalf("Unexpected empty error for stored event: %s", evInfo.ValidationError)
	}
}
