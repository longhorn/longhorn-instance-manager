package health

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/longhorn/longhorn-instance-manager/pkg/process"
)

type CheckServer struct {
	pl *process.Manager
}

func NewHealthCheckServer(pl *process.Manager) *CheckServer {
	return &CheckServer{
		pl: pl,
	}
}

func (hc *CheckServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if hc.pl != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, fmt.Errorf("Engine Manager or Process Manager or Instance Manager is not running")
}

func (hc *CheckServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	for {
		if hc.pl != nil {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for gRPC process management server",
					healthpb.HealthCheckResponse_SERVING)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for gRPC process management server",
					healthpb.HealthCheckResponse_NOT_SERVING)
			}
		}
		time.Sleep(time.Second)
	}
}

func (hc *CheckServer) List(context.Context, *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	return &healthpb.HealthListResponse{
		Statuses: map[string]*healthpb.HealthCheckResponse{
			"grpc": {
				Status: healthpb.HealthCheckResponse_SERVING,
			},
		},
	}, nil
}
