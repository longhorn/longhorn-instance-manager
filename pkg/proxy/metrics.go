package proxy

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	"github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (p *Proxy) MetricsGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineMetricsGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Trace("Getting metrics")

	c, err := eclient.NewControllerClient(req.Address)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	metrics, err := c.MetricsGet()
	if err != nil {
		return nil, err
	}

	return &rpc.EngineMetricsGetProxyResponse{
		Metrics: &ptypes.Metrics{
			ReadThroughput:  metrics.Throughput.Read,
			WriteThroughput: metrics.Throughput.Write,
			ReadLatency:     metrics.TotalLatency.Read,
			WriteLatency:    metrics.TotalLatency.Write,
			ReadIOPS:        metrics.IOPS.Read,
			WriteIOPS:       metrics.IOPS.Write,
		},
	}, nil
}
