/*
Copyright 2020 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"

	cloudevents "github.com/cloudevents/sdk-go"
	"go.uber.org/zap"

	"knative.dev/eventing/pkg/kncloudevents"
	"knative.dev/eventing/pkg/tracing"
	"knative.dev/eventing/test/test_images/recordevents/requests"
)

type eventRecorder struct {
	es *eventStore
}

func newEventRecorder() *eventRecorder {
	return &eventRecorder{es: newEventStore()}
}

func (er *eventRecorder) StartServer(port int) {
	http.HandleFunc(requests.GetMinMaxPath, er.handleMinMax)
	http.HandleFunc(requests.GetEntryPath, er.handleGetEntry)
	http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
}

func (er *eventRecorder) handleGet(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/json")
	w.WriteHeader(http.StatusOK)

}

func (er *eventRecorder) handleMinMax(w http.ResponseWriter, r *http.Request) {
	//XXX: fixme, make these atomic
	minMax := requests.MinMaxResponse{
		MinAvail: er.es.MinAvail(),
		MaxSeen:  er.es.MaxSeen(),
	}
	respBytes, err := json.Marshal(minMax)
	if err != nil {
		panic(fmt.Errorf("Internal error: json marshal shouldn't fail: (%s) (%+v)", err, minMax))
	}

	w.Header().Set("Content-Type", "text/json")
	w.WriteHeader(http.StatusOK)
	w.Write(respBytes)
}

func (er *eventRecorder) handleGetEntry(w http.ResponseWriter, r *http.Request) {
	// If we extend this much at all we should vendor a better mux(gorilla, etc)
	path := strings.TrimLeft(r.URL.Path, "/")
	getPrefix := strings.TrimLeft(requests.GetEntryPath, "/")
	suffix := strings.TrimLeft(strings.TrimPrefix(path, getPrefix), "/")

	seqNum, err := strconv.ParseInt(suffix, 10, 32)
	if err != nil {
		http.Error(w, "Can't parse event sequence number in request", http.StatusBadRequest)
		return
	}

	entryBytes, err := er.es.GetEventInfoBytes(int(seqNum))
	if err != nil {
		http.Error(w, "Couldn't find requested event", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/json")
	w.WriteHeader(http.StatusOK)
	w.Write(entryBytes)
}

func (er *eventRecorder) handler(ctx context.Context, event cloudevents.Event) {
	cloudevents.HTTPTransportContextFrom(ctx)

	tx := cloudevents.HTTPTransportContextFrom(ctx)

	er.es.StoreEvent(event, map[string][]string(tx.Header))
	//XXX: remove
	log.Printf("Stored event\n")
	if err := event.Validate(); err == nil {
		log.Printf("%s", event.Data.([]byte))
	} else {
		log.Printf("error validating the event: %v", err)
	}
}

func main() {
	er := newEventRecorder()

	logger, _ := zap.NewDevelopment()
	if err := tracing.SetupStaticPublishing(logger.Sugar(), "", tracing.AlwaysSample); err != nil {
		log.Fatalf("Unable to setup trace publishing: %v", err)
	}
	c, err := kncloudevents.NewDefaultClient()
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

	log.Fatalf("failed to start receiver: %s", c.StartReceiver(context.Background(), er.handler))
}
