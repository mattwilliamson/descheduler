/*
Copyright 2017 The Kubernetes Authors.

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

package strategies

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"sigs.k8s.io/descheduler/pkg/utils"
	"sigs.k8s.io/descheduler/test"
)

func TestFindDuplicatePods(t *testing.T) {
	// nodes
	n1 := test.BuildTestNode("n1", 2000, 3000, 10)
	n2 := test.BuildTestNode("n2", 4000, 4000, 20)
	n3 := test.BuildTestNode("n3", 4000, 4000, 17)

	// first setup pods
	p1 := test.BuildTestPod("p1", 100, 0, n1.Name)
	p1.Namespace = "dev"
	p2 := test.BuildTestPod("p2", 100, 0, n1.Name)
	p2.Namespace = "dev"
	p3 := test.BuildTestPod("p3", 100, 0, n1.Name)
	p3.Namespace = "dev"
	p4 := test.BuildTestPod("p4", 100, 0, n1.Name)
	p5 := test.BuildTestPod("p5", 100, 0, n1.Name)
	p6 := test.BuildTestPod("p6", 100, 0, n1.Name)
	p7 := test.BuildTestPod("p7", 100, 0, n1.Name)
	p7.Namespace = "kube-system"
	p8 := test.BuildTestPod("p8", 100, 0, n1.Name)
	p8.Namespace = "test"
	p9 := test.BuildTestPod("p9", 100, 0, n1.Name)
	p9.Namespace = "test"
	p10 := test.BuildTestPod("p10", 100, 0, n1.Name)
	p10.Namespace = "test"

	// ### Evictable Pods ###

	// Three Pods in the "default" Namespace, bound to same ReplicaSet. 2 should be evicted.
	ownerRef1 := test.GetReplicaSetOwnerRefList()
	p1.ObjectMeta.OwnerReferences = ownerRef1
	p2.ObjectMeta.OwnerReferences = ownerRef1
	p3.ObjectMeta.OwnerReferences = ownerRef1

	// Three Pods in the "test" Namespace, bound to same ReplicaSet. 2 should be evicted.
	ownerRef2 := test.GetReplicaSetOwnerRefList()
	p8.ObjectMeta.OwnerReferences = ownerRef2
	p9.ObjectMeta.OwnerReferences = ownerRef2
	p10.ObjectMeta.OwnerReferences = ownerRef2

	// ### Non-evictable Pods ###

	// A DaemonSet.
	p4.ObjectMeta.OwnerReferences = test.GetDaemonSetOwnerRefList()

	// A Pod with local storage.
	p5.ObjectMeta.OwnerReferences = test.GetNormalPodOwnerRefList()
	p5.Spec.Volumes = []v1.Volume{
		{
			Name: "sample",
			VolumeSource: v1.VolumeSource{
				HostPath: &v1.HostPathVolumeSource{Path: "somePath"},
				EmptyDir: &v1.EmptyDirVolumeSource{
					SizeLimit: resource.NewQuantity(int64(10), resource.BinarySI)},
			},
		},
	}

	// A Mirror Pod.
	p6.Annotations = test.GetMirrorPodAnnotation()

	// A Critical Pod.
	priority := utils.SystemCriticalPriority
	p7.Spec.Priority = &priority

	// More pods than nodes
	pn2 := test.BuildTestPod("p21", 100, 0, n2.Name)
	pn2.ObjectMeta.OwnerReferences = ownerRef1
	pn2.Namespace = "dev"

	pn3 := test.BuildTestPod("p31", 100, 0, n3.Name)
	pn3.ObjectMeta.OwnerReferences = ownerRef1
	pn3.Namespace = "dev"

	testCases := []struct {
		description             string
		maxPodsToEvict          int
		pods                    []v1.Pod
		expectedEvictedPodCount int
		nodes                   []*v1.Node
	}{
		{
			description:             "0 pods in the `dev` Namespace, bound to same ReplicaSet. 0 should be evicted.",
			maxPodsToEvict:          5,
			pods:                    []v1.Pod{},
			expectedEvictedPodCount: 0,
			nodes:                   []*v1.Node{n1},
		},
		{
			description:             "One pod in the `dev` Namespace, bound to same ReplicaSet. 0 should be evicted.",
			maxPodsToEvict:          5,
			pods:                    []v1.Pod{*p1},
			expectedEvictedPodCount: 0,
			nodes:                   []*v1.Node{n1},
		},
		{
			description:             "One pod in the `dev` Namespace with 3 nodes, bound to same ReplicaSet. 0 should be evicted.",
			maxPodsToEvict:          5,
			pods:                    []v1.Pod{*p1},
			expectedEvictedPodCount: 0,
			nodes:                   []*v1.Node{n1, n2, n3},
		},
		{
			description:             "Pods are: part of DaemonSet, with local storage, mirror pod annotation, critical pod annotation - none should be evicted.",
			maxPodsToEvict:          2,
			pods:                    []v1.Pod{*p4, *p5, *p6, *p7},
			expectedEvictedPodCount: 0,
			nodes:                   []*v1.Node{n1},
		},
		{
			// before: node1 = 0, node2 = 8
			// after: node1 = 4, node2 = 4
			description:             "Ratio should be preserved when more pods than nodes",
			maxPodsToEvict:          0,
			pods:                    []v1.Pod{*pn2, *pn2, *pn2, *pn2, *pn2, *pn2, *pn2, *pn2},
			expectedEvictedPodCount: 4,
			nodes:                   []*v1.Node{n1, n2},
		},
		{
			// before: node1 = 3, node2 = 8
			// after: node1 = 5, node2 = 6
			description:             "Ratio should be preserved when more pods than nodes and both nodes have pods",
			maxPodsToEvict:          0,
			pods:                    []v1.Pod{*p1, *p1, *p1, *pn2, *pn2, *pn2, *pn2, *pn2, *pn2, *pn2, *pn2},
			expectedEvictedPodCount: 2,
			nodes:                   []*v1.Node{n1, n2},
		},
		{
			// node 1: 2, node 2: 6, node 3: 5
			// 13 pods / 3 nodes -> desire 4.33 (5) pods each.
			// evict 1 pods on node 2
			description:             "Ratio should be preserved when more pods than nodes and has remainders",
			maxPodsToEvict:          0,
			pods:                    []v1.Pod{*p1, *p1, *pn2, *pn2, *pn2, *pn2, *pn2, *pn2, *pn3, *pn3, *pn3, *pn3, *pn3},
			expectedEvictedPodCount: 1,
			nodes:                   []*v1.Node{n1, n2, n3},
		},
		{
			// node 1: 1, node 2: 1, node 3: 10
			// 10 pods / 3 nodes -> desire 3.33 (4) pods each.
			// evict 1 pods on node 2
			description:             "Ratio should be preserved when more pods than nodes and has remainders across several nodes",
			maxPodsToEvict:          0,
			pods:                    []v1.Pod{*p1, *pn2, *pn3, *pn3, *pn3, *pn3, *pn3, *pn3, *pn3, *pn3, *pn3, *pn3},
			expectedEvictedPodCount: 6,
			nodes:                   []*v1.Node{n1, n2, n3},
		},
		{
			// node 1: 3, node 2: 4, node 3: 4
			// 11 pods / 3 nodes -> desire 3.67 (4) pods each.
			description:             "Even numbers should not evict any",
			maxPodsToEvict:          0,
			pods:                    []v1.Pod{*p1, *p1, *p1, *pn2, *pn2, *pn2, *pn2, *pn3, *pn3, *pn3, *pn3},
			expectedEvictedPodCount: 0,
			nodes:                   []*v1.Node{n1, n2, n3},
		},
	}

	for _, testCase := range testCases {
		npe := utils.NodePodEvictedCount{}
		npe[n1] = 0
		fakeClient := &fake.Clientset{}

		fakeClient.Fake.AddReactor("list", "pods", func(action core.Action) (bool, runtime.Object, error) {
			obj := &v1.PodList{
				Items: []v1.Pod{},
			}
			for _, pod := range testCase.pods {
				podFieldSet := fields.Set(map[string]string{
					"spec.nodeName": pod.Spec.NodeName,
					"status.phase":  string(pod.Status.Phase),
				})
				match := action.(core.ListAction).GetListRestrictions().Fields.Matches(podFieldSet)
				if !match {
					continue
				}
				obj.Items = append(obj.Items, *pod.DeepCopy())
			}
			return true, obj, nil
		})

		fakeClient.Fake.AddReactor("get", "nodes", func(action core.Action) (bool, runtime.Object, error) {
			getAction := action.(core.GetAction)
			switch getAction.GetName() {
			case n1.Name:
				return true, n1, nil
			case n2.Name:
				return true, n2, nil
			case n3.Name:
				return true, n3, nil
			}
			return true, nil, fmt.Errorf("Wrong node: %v", getAction.GetName())
		})
		podsEvicted := deleteDuplicatePods(fakeClient, "v1", testCase.nodes, false, npe, testCase.maxPodsToEvict, false)
		if podsEvicted != testCase.expectedEvictedPodCount {
			t.Errorf("Test error for description: %s. Expected evicted pods count %v, got %v", testCase.description, testCase.expectedEvictedPodCount, podsEvicted)
		}
	}

}
