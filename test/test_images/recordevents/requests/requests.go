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

package requests

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"time"

	cloudevents "github.com/cloudevents/sdk-go"

	"knative.dev/eventing/test/lib/resources"
)

type MinMaxResponse struct {
	MinAvail int
	MaxSeen  int
}

type EventInfo struct {
	ValidationError string
	Event           *cloudevents.Event
	HTTPHeaders     map[string][]string
}

const GetMinMaxPath = "/minmax"
const GetEntryPath = "/entry/"

func GetMinMax(host string, port int) (minRet int, maxRet int, errRet error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d%s", host, port, GetMinMaxPath))
	if err != nil {
		return -1, -1, fmt.Errorf("http get error: %s", err)
	}
	defer resp.Body.Close()
	bodyContents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return -1, -1, fmt.Errorf("Error reading response body %w", err)
	}
	if resp.StatusCode != 200 {
		return -1, -1, fmt.Errorf("Error %d reading response", resp.StatusCode)
	}
	minMaxResponse := MinMaxResponse{}
	err = json.Unmarshal(bodyContents, &minMaxResponse)
	if err != nil {
		return -1, -1, fmt.Errorf("Error unmarshalling response %w", err)
	}
	if minMaxResponse.MinAvail == 0 || minMaxResponse.MaxSeen == 0 {
		return -1, -1, fmt.Errorf("Invalid decoded json: %+v", minMaxResponse)
	}

	return minMaxResponse.MinAvail, minMaxResponse.MaxSeen, nil
}

func GetEntry(host string, port int, seqno int) (EventInfo, error) {
	resp, err := http.Get(fmt.Sprintf("http://%s:%d%s/%d", host, port, GetEntryPath, seqno))
	if err != nil {
		return EventInfo{}, fmt.Errorf("http get err %s", err)
	}
	defer resp.Body.Close()
	bodyContents, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return EventInfo{}, fmt.Errorf("Error reading response body %w", err)
	}
	if resp.StatusCode != 200 {
		return EventInfo{}, fmt.Errorf("Error %d reading response", resp.StatusCode)
	}
	entryResponse := EventInfo{}
	err = json.Unmarshal(bodyContents, &entryResponse)
	if err != nil {
		return EventInfo{}, fmt.Errorf("Error unmarshalling response %w", err)
	}
	if len(entryResponse.ValidationError) == 0 && entryResponse.Event == nil {
		return EventInfo{}, fmt.Errorf("Invalid decoded json: %+v", entryResponse)
	}

	return entryResponse, nil
}

type EventInfoMatchFunc func(EventInfo) bool
type EventMatchFunc func(*cloudevents.Event) bool

func backOff(timeout time.Duration, testFunc func() error) error {
	maxWait := timeout / 10
	curWait := 1 * time.Second
	start := time.Now()

	for {
		err := testFunc()
		if err == nil {
			return nil
		}
		if timeout > 0 && time.Now().Sub(start) > timeout {
			return err
		}

		time.Sleep(curWait)
		curWait *= 2
		if curWait > maxWait {
			curWait = maxWait
		}
	}
}

type EventInfoStore struct {
	podName      string
	podNamespace string
	podPort      int
	forwardPID   int

	host string
	port int
}

func NewEventInfoStore(podName string, podNamespace string, podPort int, timeout time.Duration) (*EventInfoStore, error) {
	ei := &EventInfoStore{podName: podName, podNamespace: podNamespace, podPort: podPort}
	start := time.Now()
	err := ei.forwardPort(timeout)
	if err != nil {
		return nil, err
	}
	if timeout > 0 {
		timeout = timeout - time.Now().Sub(start)
		if timeout < 0 {
			timeout = 1
		}
	}

	err = ei.waitTillUp(timeout)
	if err != nil {
		return nil, err
	} else {
		return ei, nil
	}
}

func (ei *EventInfoStore) forwardPort(timeout time.Duration) error {
	portRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	portMin := 30000
	portMax := 60000

	return backOff(timeout, func() error {
		localPort := portMin + portRand.Intn(portMax-portMin)
		pid, err := resources.PortForward(printfn, ei.podName, localPort, ei.podPort, ei.podNamespace)
		if err == nil {
			ei.forwardPID = pid
			ei.port = localPort
			ei.host = "localhost"
		}
		return err
	})
}

func (ei *EventInfoStore) Cleanup() error {
	pid := ei.forwardPID
	ei.forwardPID = 0
	if pid != 0 {
		ps := os.Process{Pid: pid}
		return ps.Kill()
	}
	return nil
}

///XXX: remove me
func printfn(str string, ints ...interface{}) {
	fmt.Printf(fmt.Sprintf("XXXEML3:%s", str), ints...)
}

func (ei *EventInfoStore) waitTillUp(timeout time.Duration) error {
	return backOff(timeout, func() error {
		_, _, err := GetMinMax(ei.host, ei.port)
		return err
	})
}

func (ei *EventInfoStore) Find(f EventInfoMatchFunc) ([]EventInfo, error) {
	allMatch := []EventInfo{}

	min, max, err := GetMinMax(ei.host, ei.port)
	if err != nil {
		return nil, fmt.Errorf("error getting MinMax %s", err)
	}
	if min == -1 {
		return nil, nil
	}
	for i := min; i <= max; i++ {
		e, err := GetEntry(ei.host, ei.port, i)
		if err != nil {
			return nil, fmt.Errorf("error getting GetEntry %s", err)
		}

		if f(e) {
			allMatch = append(allMatch, e)
		}
	}
	return allMatch, nil
}

func ValidEvFunc(evf EventMatchFunc) EventInfoMatchFunc {
	return func(ei EventInfo) bool {
		if ei.Event == nil {
			return false
		} else {
			return evf(ei.Event)
		}
	}

}

func (ei *EventInfoStore) AtLeastNMatch(f EventInfoMatchFunc, n int, timeout time.Duration) ([]EventInfo, error) {
	var matchRet []EventInfo
	err := backOff(timeout, func() error {
		allMatch, err := ei.Find(f)
		if err != nil {
			return fmt.Errorf("unexpected error during find: %s", err)
		}
		count := len(allMatch)
		if count < n {
			return fmt.Errorf("saw %d/%d matching events", count, n)
		}
		matchRet = allMatch
		return nil
	})
	return matchRet, err
}
