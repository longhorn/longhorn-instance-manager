package proxy

import (
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

type HealthChecker interface {
	IsRunning(address string) bool
	WaitForRunning(address, name string, stopCh chan struct{}) bool
}

type GRPCHealthChecker struct{}

func (c *GRPCHealthChecker) IsRunning(address string) bool {
	return util.GRPCServiceReadinessProbe(address)
}

func (c *GRPCHealthChecker) WaitForRunning(address, name string, stopCh chan struct{}) bool {
	ticker := time.NewTicker(types.WaitInterval)
	defer ticker.Stop()

	for i := 0; i < types.WaitCount; i++ {
		select {
		case <-stopCh:
			logrus.Infof("stop waiting for gRPC service of proxy %v to start at %v", name, address)
			return false

		case <-ticker.C:
			if c.IsRunning(address) {
				logrus.Infof("Proxy %v has started at %v", name, address)
				return true
			}
			logrus.Infof("wait for gRPC service of proxy %v to start at %v", name, address)
		}
	}

	return false
}
