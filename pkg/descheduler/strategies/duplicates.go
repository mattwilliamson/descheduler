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
	"math"
	"strings"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog"

	"sigs.k8s.io/descheduler/cmd/descheduler/app/options"
	"sigs.k8s.io/descheduler/pkg/api"
	"sigs.k8s.io/descheduler/pkg/descheduler/evictions"
	podutil "sigs.k8s.io/descheduler/pkg/descheduler/pod"
	"sigs.k8s.io/descheduler/pkg/utils"
)

//type creator string
type DuplicatePodsMap map[string][]*v1.Pod
type DuplicateNodePodsMap map[*v1.Node]DuplicatePodsMap
type DuplicatePodCount struct {
	Total     int
	Nodes     int
	PerNode   float64
	Max       int
	Min       int
	Remainder int
}

func (dpc *DuplicatePodCount) Recalculate() {
	dpc.PerNode = float64(dpc.Total) / float64(dpc.Nodes)
	dpc.Max = int(math.Ceil(dpc.PerNode))
	dpc.Min = int(math.Floor(dpc.PerNode))
	dpc.Remainder = dpc.Max - dpc.Min
}

// RemoveDuplicatePods removes the duplicate pods on node. This strategy evicts all duplicate pods on node.
// A pod is said to be a duplicate of other if both of them are from same creator, kind and are within the same
// namespace. As of now, this strategy won't evict daemonsets, mirror pods, critical pods and pods with local storages.
func RemoveDuplicatePods(ds *options.DeschedulerServer, strategy api.DeschedulerStrategy, policyGroupVersion string, nodes []*v1.Node, nodepodCount utils.NodePodEvictedCount) {
	if !strategy.Enabled {
		return
	}
	deleteDuplicatePods(ds.Client, policyGroupVersion, nodes, ds.DryRun, nodepodCount, ds.MaxNoOfPodsToEvictPerNode, ds.EvictLocalStoragePods)
}

// deleteDuplicatePods evicts the pod from node and returns the count of evicted pods.
func deleteDuplicatePods(client clientset.Interface, policyGroupVersion string, nodes []*v1.Node, dryRun bool, nodepodCount utils.NodePodEvictedCount, maxPodsToEvict int, evictLocalStoragePods bool) int {
	podsEvicted := 0
	podCounts := map[string]*DuplicatePodCount{}
	nodePods := DuplicateNodePodsMap{}
	nodeCount := len(nodes)

	for _, node := range nodes {
		klog.V(1).Infof("Processing node: %#v", node.Name)
		dpm := ListDuplicatePodsOnANode(client, node, evictLocalStoragePods)
		nodePods[node] = dpm

		for creator, pods := range dpm {
			if _, ok := podCounts[creator]; !ok {
				podCounts[creator] = &DuplicatePodCount{}
			}
			podCounts[creator].Total += len(pods)
			podCounts[creator].Nodes = nodeCount
			podCounts[creator].Recalculate()
		}
	}

	for _, node := range nodes {
		klog.V(1).Infof("Processing node: %#v", node.Name)
		dpm := nodePods[node]

		for creator, pods := range dpm {
			pc := podCounts[creator]
			// podsToEvict := len(pods) - pc.Min
			podsToEvict := len(pods) - pc.Max

			if len(pods)-podsToEvict <= 0 || podsToEvict <= 0 {
				continue
			}

			// if len(pods)-podsToEvict-1 >= pc.Min && pc.Remainder > 0 {
			// 	fmt.Println("Remainder!:", pc.Remainder)
			// 	podsToEvict++
			// 	pc.Remainder--
			// }

			klog.V(1).Infof("%#v", creator)
			// i = 0 does not evict the first pod

			for i := 0; i < podsToEvict; i++ {
				if maxPodsToEvict > 0 && nodepodCount[node]+1 > maxPodsToEvict {
					break
				}
				success, err := evictions.EvictPod(client, pods[i], policyGroupVersion, dryRun)
				if !success {
					klog.Infof("Error when evicting pod: %#v (%#v)", pods[i].Name, err)
				} else {
					nodepodCount[node]++
					klog.V(1).Infof("Evicted pod: %#v (%#v)", pods[i].Name, err)
				}
			}
		}

		podsEvicted += nodepodCount[node]

	}

	return podsEvicted
}

// ListDuplicatePodsOnANode lists duplicate pods on a given node.
func ListDuplicatePodsOnANode(client clientset.Interface, node *v1.Node, evictLocalStoragePods bool) DuplicatePodsMap {
	pods, err := podutil.ListEvictablePodsOnNode(client, node, evictLocalStoragePods)
	if err != nil {
		return nil
	}
	return FindDuplicatePods(pods)
}

// FindDuplicatePods takes a list of pods and returns a duplicatePodsMap.
func FindDuplicatePods(pods []*v1.Pod) DuplicatePodsMap {
	dpm := DuplicatePodsMap{}
	// Ignoring the error here as in the ListDuplicatePodsOnNode function we call ListEvictablePodsOnNode which checks for error.
	for _, pod := range pods {
		ownerRefList := podutil.OwnerRef(pod)
		for _, ownerRef := range ownerRefList {
			// Namespace/Kind/Name should be unique for the cluster.
			s := strings.Join([]string{pod.ObjectMeta.Namespace, ownerRef.Kind, ownerRef.Name}, "/")
			dpm[s] = append(dpm[s], pod)
		}
	}
	return dpm
}
