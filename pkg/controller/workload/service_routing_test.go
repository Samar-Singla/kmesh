/*
 * Copyright The Kmesh Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package workload

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"kmesh.net/kmesh/api/v2/workloadapi"
	"kmesh.net/kmesh/pkg/controller/workload/bpfcache"
	"kmesh.net/kmesh/pkg/controller/workload/common"
	"kmesh.net/kmesh/pkg/nets"
)

func TestServiceRoutingPreferenceUpdate(t *testing.T) {
	closeScopes := []workloadapi.LoadBalancing_Scope{workloadapi.LoadBalancing_NETWORK, workloadapi.LoadBalancing_REGION, workloadapi.LoadBalancing_ZONE, workloadapi.LoadBalancing_SUBZONE}
	nodeScopes := []workloadapi.LoadBalancing_Scope{workloadapi.LoadBalancing_NODE}
	for _, tc := range []struct {
		name          string
		from, to      workloadapi.LoadBalancing_Mode
		before, after []workloadapi.LoadBalancing_Scope
		remoteOnly    bool
	}{
		{"prefer_close_to_local", workloadapi.LoadBalancing_FAILOVER, workloadapi.LoadBalancing_STRICT, closeScopes, nodeScopes, false},
		{"local_to_prefer_close", workloadapi.LoadBalancing_STRICT, workloadapi.LoadBalancing_FAILOVER, nodeScopes, closeScopes, false},
		{"strict_scope_change", workloadapi.LoadBalancing_STRICT, workloadapi.LoadBalancing_STRICT, closeScopes, nodeScopes, false},
		{"failover_scope_change", workloadapi.LoadBalancing_FAILOVER, workloadapi.LoadBalancing_FAILOVER, closeScopes, nodeScopes, false},
		{"no_local_endpoint", workloadapi.LoadBalancing_FAILOVER, workloadapi.LoadBalancing_STRICT, closeScopes, nodeScopes, true},
		{"mode_only_change", workloadapi.LoadBalancing_FAILOVER, workloadapi.LoadBalancing_STRICT, nodeScopes, nodeScopes, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NODE_NAME", "node-a")
			maps := bpfcache.NewFakeWorkloadMap(t)
			defer bpfcache.CleanupFakeWorkloadMap(maps)
			p := NewProcessor(maps)
			defer hashNameClean(p)
			loc := createLocality("region", "zone", "subzone")
			// Initialize locality before adding any endpoint to isolate service
			// updates from the separate locality-initialization behavior.
			p.locality.SetLocality("node-a", "cluster0", "testnetwork", loc)
			svc := common.CreateFakeService("svc1", "10.240.10.1", "", createLoadBalancing(tc.from, tc.before))
			require.NoError(t, p.handleService(svc))
			workloads := []*workloadapi.Workload{
				createWorkload("remote1", "10.244.0.2", "node-b", workloadapi.NetworkMode_STANDARD, loc, "svc1"),
				createWorkload("remote2", "10.244.0.3", "node-c", workloadapi.NetworkMode_STANDARD, loc, "svc1"),
			}
			if !tc.remoteOnly {
				workloads = append(workloads, createWorkload("local", "10.244.0.1", "node-a", workloadapi.NetworkMode_STANDARD, loc, "svc1"))
			}
			for _, wl := range workloads {
				require.NoError(t, p.handleWorkload(wl))
			}
			next := proto.Clone(svc).(*workloadapi.Service)
			next.LoadBalancing = createLoadBalancing(tc.to, tc.after)
			next.Ports[0].TargetPort = 9090
			// Repeating the update must preserve both the groups and counts.
			for range 2 {
				require.NoError(t, p.handleService(next))
				sk := bpfcache.ServiceKey{ServiceId: p.hashName.Hash(svc.ResourceName())}
				var value bpfcache.ServiceValue
				require.NoError(t, p.bpf.ServiceLookup(&sk, &value))
				require.Equal(t, uint32(tc.to), value.LbPolicy)
				require.Equal(t, nets.ConvertPortToBigEndian(9090), value.TargetPort[0])
				expectedCounts := value.EndpointCount
				clear(expectedCounts[:])
				for _, wl := range workloads {
					prio := p.locality.CalcLocalityLBPrio(wl, tc.after)
					expectedCounts[prio]++
					uid := p.hashName.Hash(wl.Uid)
					ep, ok := p.EndpointCache.List(sk.ServiceId)[uid]
					require.True(t, ok)
					require.Equal(t, prio, ep.Prio)
					var backend bpfcache.EndpointValue
					require.NoError(t, p.bpf.EndpointLookup(&bpfcache.EndpointKey{ServiceId: sk.ServiceId, Prio: prio, BackendIndex: ep.BackendIndex}, &backend))
					require.Equal(t, uid, backend.BackendUid)
				}
				require.Equal(t, expectedCounts, value.EndpointCount)
				require.Equal(t, len(workloads), p.bpf.EndpointCount())
			}
		})
	}
}
