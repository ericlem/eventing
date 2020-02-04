// +build e2e

/*
Copyright 2019 The Knative Authors
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

package e2e

import (
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"

	duckv1beta1 "knative.dev/pkg/apis/duck/v1beta1"
	pkgTest "knative.dev/pkg/test"

	"knative.dev/eventing/test/lib"
	"knative.dev/eventing/test/lib/resources"
	"knative.dev/eventing/test/test_images/recordevents/requests"

	sourcesv1alpha1 "knative.dev/eventing/pkg/apis/legacysources/v1alpha1"
	eventingtesting "knative.dev/eventing/pkg/reconciler/testing"
)

func printfn(str string, ints ...interface{}) {
	fmt.Printf(fmt.Sprintf("XXXEML3:%s", str), ints...)
}

func TestContainerSource(t *testing.T) {
	const (
		containerSourceName = "e2e-container-source"
		templateName        = "e2e-container-source-template"
		// the heartbeats image is built from test_images/heartbeats
		imageName = "heartbeats"

		loggerPodName = "e2e-container-source-logger-pod"
	)

	client := setup(t, true)
	defer tearDown(client)

	// create event logger pod and service
	loggerPod := resources.EventRecordPod(loggerPodName)
	//loggerPod := resources.EventLoggerPod(loggerPodName)
	client.CreatePodOrFail(loggerPod, lib.WithService(loggerPodName))

	// create container source
	data := fmt.Sprintf("TestContainerSource%s", uuid.NewUUID())
	// args are the arguments passing to the container, msg is used in the heartbeats image
	args := []string{"--msg=" + data}
	// envVars are the environment variables of the container
	envVars := []corev1.EnvVar{{
		Name:  "POD_NAME",
		Value: templateName,
	}, {
		Name:  "POD_NAMESPACE",
		Value: client.Namespace,
	}}
	containerSource := eventingtesting.NewContainerSource(
		containerSourceName,
		client.Namespace,
		eventingtesting.WithContainerSourceSpec(sourcesv1alpha1.ContainerSourceSpec{
			Template: &corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name: templateName,
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:            imageName,
						Image:           pkgTest.ImagePath(imageName),
						ImagePullPolicy: corev1.PullAlways,
						Args:            args,
						Env:             envVars,
					}},
				},
			},
			Sink: &duckv1beta1.Destination{Ref: resources.ServiceRef(loggerPodName)},
		}),
	)
	client.CreateLegacyContainerSourceOrFail(containerSource)

	// wait for all test resources to be ready
	if err := client.WaitForAllTestResourcesReady(); err != nil {
		t.Fatalf("Failed to get all test resources ready: %v", err)
	}

	port, err := resources.PortForward(printfn, loggerPodName, 8081, 8081, client.Namespace)
	if err != nil {
		t.Fatalf("Error forwarding port")
	}
	defer resources.Cleanup(port)

	min, max, err1 := requests.GetMinMax("localhost", 8081)
	fmt.Printf("XXXEML1a: min %d max %d: %s\n", min, max, err1)

	// verify the logger service receives the event
	expectedCount := 2
	if err := client.CheckLog(loggerPodName, lib.CheckerContainsAtLeast(data, expectedCount)); err != nil {
		t.Fatalf("String %q does not appear at least %d times in logs of logger pod %q: %v", data, expectedCount, loggerPodName, err)
	}

	min, max, err1 = requests.GetMinMax("localhost", 8081)
	fmt.Printf("XXXEML2b: min %d max %d: %s\n", min, max, err1)
}
