package client

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
)

func (c *ProxyClient) VolumeGet(serviceAddress string) (info *etypes.VolumeInfo, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get volume")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting volume via proxy")

	return info, nil
}

func (c *ProxyClient) VolumeExpand(serviceAddress string, size int64) (err error) {
	if serviceAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to expand volume")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Expanding volume via proxy")

	return nil
}

func (c *ProxyClient) VolumeFrontendStart(serviceAddress, frontendName string) (err error) {
	if serviceAddress == "" || frontendName == "" {
		return errors.Wrapf(ErrParameter, "failed to start volume frontend")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Starting volume frontend %v via proxy", frontendName)

	return nil
}

func (c *ProxyClient) VolumeFrontendShutdown(serviceAddress string) (err error) {
	if serviceAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to shutdown volume frontend")
	}
	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Shutting down volume frontend via proxy")

	return nil
}
