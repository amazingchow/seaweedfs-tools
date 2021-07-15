package pkg

import (
	"github.com/chrislusf/seaweedfs/weed/pb/master_pb"
)

func CollectVolumeInfo(topo *master_pb.TopologyInfo, canWriteOnly bool) map[string][]uint32 {
	m := make(map[string][]uint32)
	for _, dc := range topo.DataCenterInfos {
		for _, r := range dc.RackInfos {
			for _, dn := range r.DataNodeInfos {
				for _, v := range dn.VolumeInfos {
					if canWriteOnly {
						if !v.ReadOnly {
							_, ok := m[v.Collection]
							if !ok {
								m[v.Collection] = make([]uint32, 0)
							}
							m[v.Collection] = append(m[v.Collection], v.Id)
						}
					} else {
						_, ok := m[v.Collection]
						if !ok {
							m[v.Collection] = make([]uint32, 0)
						}
						m[v.Collection] = append(m[v.Collection], v.Id)
					}
				}
			}
		}
	}
	return m
}
