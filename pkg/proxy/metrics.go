package proxy

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/enginerpc"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

func (p *Proxy) MetricsGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineMetricsGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"volume":     req.VolumeName,
		"instance":   req.EngineName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Getting metrics")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.MetricsGet(ctx, req)
}

func (ops V1DataEngineProxyOps) MetricsGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineMetricsGetProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.Address,
				"volume":     req.VolumeName,
				"instance":   req.EngineName,
				"dataEngine": req.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	metrics, err := c.MetricsGet()
	if err != nil {
		return nil, err
	}

	return &rpc.EngineMetricsGetProxyResponse{
		Metrics: &enginerpc.Metrics{
			ReadThroughput:  metrics.Throughput.Read,
			WriteThroughput: metrics.Throughput.Write,
			ReadLatency:     metrics.TotalLatency.Read,
			WriteLatency:    metrics.TotalLatency.Write,
			ReadIOPS:        metrics.IOPS.Read,
			WriteIOPS:       metrics.IOPS.Write,
		},
	}, nil
}

func (ops V2DataEngineProxyOps) MetricsGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineMetricsGetProxyResponse, err error) {
	c, err := getSPDKClientFromAddress(req.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.Address,
				"volume":     req.VolumeName,
				"instance":   req.EngineName,
				"dataEngine": req.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	metrics, err := c.MetricsGet(req.EngineName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get engine %v: %v", req.EngineName, err)
	}

	return &rpc.EngineMetricsGetProxyResponse{
		Metrics: &enginerpc.Metrics{
			ReadThroughput:  metrics.ReadThroughput,
			WriteThroughput: metrics.WriteThroughput,
			ReadLatency:     metrics.ReadLatency,
			WriteLatency:    metrics.WriteLatency,
			ReadIOPS:        metrics.ReadIOPS,
			WriteIOPS:       metrics.WriteIOPS,
		},
	}, nil
}
