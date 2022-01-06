package proxy

import (
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

type Proxy struct {
	logsDir       string
	shutdownCh    chan error
	HealthChecker HealthChecker
}

func NewProxy(logsDir string, shutdownCh chan error) (*Proxy, error) {
	p := &Proxy{
		logsDir:       logsDir,
		shutdownCh:    shutdownCh,
		HealthChecker: &GRPCHealthChecker{},
	}

	go p.startMonitoring()

	return p, nil
}

func (p *Proxy) startMonitoring() {
	done := false
	for {
		select {
		case <-p.shutdownCh:
			logrus.Infof("Proxy Server is shutting down")
			done = true
			break
		}
		if done {
			break
		}
	}
}

func (p *Proxy) Ping(ctx context.Context, req *empty.Empty) (resp *empty.Empty, err error) {
	return &empty.Empty{}, nil
}

func (p *Proxy) ServerVersionGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVersionProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Debug("Getting server version")

	resp = &rpc.EngineVersionProxyResponse{
		Version: &eptypes.VersionOutput{},
	}

	// TODO

	return resp, nil
}
