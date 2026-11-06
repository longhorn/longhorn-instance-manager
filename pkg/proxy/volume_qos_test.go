package proxy

import (
	"context"
	"testing"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

func TestVolumeQosLimitSetRejectsUnsupportedDataEngines(t *testing.T) {
	p := &Proxy{
		ops: map[rpc.DataEngine]ProxyOps{
			rpc.DataEngine_DATA_ENGINE_V1: V1DataEngineProxyOps{},
		},
	}

	tests := []struct {
		name       string
		dataEngine rpc.DataEngine
	}{
		{
			name:       "v1 volumes do not support QoS limits",
			dataEngine: rpc.DataEngine_DATA_ENGINE_V1,
		},
		{
			name:       "data engine without proxy ops",
			dataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := p.VolumeQosLimitSet(context.Background(), &rpc.EngineVolumeQosLimitSetRequest{
				ProxyEngineRequest: &rpc.ProxyEngineRequest{
					EngineName: "vol-e",
					VolumeName: "vol",
					DataEngine: tc.dataEngine,
				},
				QosLimit: &rpc.VolumeQosLimit{RwIosPerSec: 1000, RwMbytesPerSec: 100},
			})
			if grpcstatus.Code(err) != grpccodes.Unimplemented {
				t.Fatalf("expected Unimplemented, got %v", err)
			}
		})
	}
}
