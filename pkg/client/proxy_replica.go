package client

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
)

func (c *ProxyClient) ReplicaAdd(serviceAddress, replicaAddress string, restore bool) (err error) {
	if serviceAddress == "" || replicaAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to add replica")
	}

	log := logrus.WithFields(logrus.Fields{
		"serviceURL": c.ServiceURL,
		"restore":    restore,
	})
	log.Debugf("Adding replica %v via proxy", replicaAddress)

	return nil
}

func (c *ProxyClient) ReplicaList(serviceAddress string) (rInfoList []*etypes.ControllerReplicaInfo, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to list replicas")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Listing replicas via proxy")

	return rInfoList, nil
}

func (c *ProxyClient) ReplicaRebuildingStatus(serviceAddress string) (status map[string]*ReplicaRebuildStatus, err error) {
	if serviceAddress == "" {
		return status, errors.Wrapf(ErrParameter, "failed to get replica rebuilding status")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting replica rebuilding status via proxy")

	return status, nil
}

func (c *ProxyClient) ReplicaVerifyRebuild(serviceAddress, replicaAddress string) (err error) {
	if serviceAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to verify replica rebuild")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Verifying replica rebuild via proxy")

	return nil
}

func (c *ProxyClient) ReplicaRemove(serviceAddress, replicaAddress string) (err error) {
	if serviceAddress == "" || replicaAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to remove replica")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Removing replica %v via proxy", replicaAddress)

	return nil
}
