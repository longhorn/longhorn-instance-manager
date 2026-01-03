package health

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/longhorn/longhorn-instance-manager/pkg/disk"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

type CheckDiskServer struct {
	server *disk.Server
}

func NewDiskHealthCheckServer(server *disk.Server) *CheckDiskServer {
	return &CheckDiskServer{
		server: server,
	}
}

func (hc *CheckDiskServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if hc.server != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, fmt.Errorf("server or instance manager is not running")
}

func (hc *CheckDiskServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	for {
		if hc.server != nil {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for %s",
					healthpb.HealthCheckResponse_SERVING, types.DiskGrpcService)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for %s",
					healthpb.HealthCheckResponse_NOT_SERVING, types.DiskGrpcService)
			}

		}
		time.Sleep(time.Second)
	}
}

func (hc *CheckDiskServer) List(context.Context, *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	return &healthpb.HealthListResponse{
		Statuses: map[string]*healthpb.HealthCheckResponse{
			"grpc": {
				Status: healthpb.HealthCheckResponse_SERVING,
			},
		},
	}, nil
}
