package health

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/longhorn/longhorn-instance-manager/pkg/instance"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

type CheckInstanceServer struct {
	server *instance.Server
}

func NewInstanceHealthCheckServer(server *instance.Server) *CheckInstanceServer {
	return &CheckInstanceServer{
		server: server,
	}
}

func (hc *CheckInstanceServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if hc.server != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, fmt.Errorf("server or instance manager is not running")
}

func (hc *CheckInstanceServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	for {
		if hc.server != nil {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for %s",
					healthpb.HealthCheckResponse_SERVING, types.InstanceGrpcService)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for %s",
					healthpb.HealthCheckResponse_NOT_SERVING, types.InstanceGrpcService)
			}

		}
		time.Sleep(time.Second)
	}
}

func (hc *CheckInstanceServer) List(context.Context, *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	return &healthpb.HealthListResponse{
		Statuses: map[string]*healthpb.HealthCheckResponse{
			"grpc": {
				Status: healthpb.HealthCheckResponse_SERVING,
			},
		},
	}, nil
}
