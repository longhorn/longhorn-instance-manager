package health

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/longhorn/longhorn-instance-manager/pkg/proxy"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

type CheckProxyServer struct {
	proxy *proxy.Proxy
}

func NewProxyHealthCheckServer(proxy *proxy.Proxy) *CheckProxyServer {
	return &CheckProxyServer{
		proxy: proxy,
	}
}

func (hc *CheckProxyServer) Check(context.Context, *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if hc.proxy != nil {
		return &healthpb.HealthCheckResponse{
			Status: healthpb.HealthCheckResponse_SERVING,
		}, nil
	}

	return &healthpb.HealthCheckResponse{
		Status: healthpb.HealthCheckResponse_NOT_SERVING,
	}, fmt.Errorf("proxy or instance manager is not running")
}

func (hc *CheckProxyServer) Watch(req *healthpb.HealthCheckRequest, ws healthpb.Health_WatchServer) error {
	for {
		if hc.proxy != nil {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for %s",
					healthpb.HealthCheckResponse_SERVING, types.ProxyGRPCService)
			}
		} else {
			if err := ws.Send(&healthpb.HealthCheckResponse{
				Status: healthpb.HealthCheckResponse_NOT_SERVING,
			}); err != nil {
				logrus.WithError(err).Errorf("Failed to send health check result %v for %s",
					healthpb.HealthCheckResponse_NOT_SERVING, types.ProxyGRPCService)
			}

		}
		time.Sleep(time.Second)
	}
}

func (hc *CheckProxyServer) List(context.Context, *healthpb.HealthListRequest) (*healthpb.HealthListResponse, error) {
	return &healthpb.HealthListResponse{
		Statuses: map[string]*healthpb.HealthCheckResponse{
			"grpc": {
				Status: healthpb.HealthCheckResponse_SERVING,
			},
		},
	}, nil
}
