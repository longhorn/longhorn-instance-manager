package health

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	spdk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

type CheckSPDKServer struct {
	server *spdk.Server
}

func NewSPDKHealthCheckServer(server *spdk.Server) *CheckSPDKServer {
	return &CheckSPDKServer{
		server: server,
	}
}

func (hc *CheckSPDKServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if hc.server != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, fmt.Errorf("server or instance manager is not running")
}

func (hc *CheckSPDKServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	for {
		if hc.server != nil {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for %s",
					healthpb.HealthCheckResponse_SERVING, types.SpdkGrpcService)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for %s",
					healthpb.HealthCheckResponse_NOT_SERVING, types.SpdkGrpcService)
			}

		}
		time.Sleep(time.Second)
	}
}

func (hc *CheckSPDKServer) List(context.Context, *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	return &healthpb.HealthListResponse{
		Statuses: map[string]*healthpb.HealthCheckResponse{
			"grpc": {
				Status: healthpb.HealthCheckResponse_SERVING,
			},
		},
	}, nil
}
