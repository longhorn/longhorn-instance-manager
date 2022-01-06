package client

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
)

func (c *ProxyClient) VolumeSnapshot(serviceAddress, volumeName string, labels map[string]string) (snapshotName string, err error) {
	if serviceAddress == "" {
		return "", errors.Wrapf(ErrParameter, "failed to snapshot volume")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Snapshotting volume via proxy")

	return snapshotName, nil
}

func (c *ProxyClient) SnapshotList(serviceAddress string) (snapshotDiskInfo map[string]*etypes.DiskInfo, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to list snapshots")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Listing snapshots via proxy")

	return snapshotDiskInfo, nil
}

func (c *ProxyClient) SnapshotClone(serviceAddress, name, fromController string) (err error) {
	if serviceAddress == "" || name == "" || fromController == "" {
		return errors.Wrapf(ErrParameter, "failed to clone snapshot")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Cloning snapshot %v from %v via proxy", name, fromController)

	return err
}

func (c *ProxyClient) SnapshotCloneStatus(serviceAddress string) (status map[string]*SnapshotCloneStatus, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed get snapshot clone status")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting snapshot clone status via proxy")

	return status, err
}

func (c *ProxyClient) SnapshotRevert(serviceAddress string, name string) (err error) {
	if serviceAddress == "" || name == "" {
		return errors.Wrapf(ErrParameter, "failed to revert volume to snapshot %v", name)
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Reverting snapshot %v via proxy", name)

	return nil
}

func (c *ProxyClient) SnapshotPurge(serviceAddress string, skipIfInProgress bool) (err error) {
	if serviceAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to purge snapshots")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Purging snapshots via proxy")

	return nil
}

func (c *ProxyClient) SnapshotPurgeStatus(serviceAddress string) (status map[string]*SnapshotPurgeStatus, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get snapshot purge status")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting snapshot purge status via proxy")

	return status, nil
}

func (c *ProxyClient) SnapshotRemove(serviceAddress string, names []string) (err error) {
	if serviceAddress == "" {
		return errors.Wrapf(ErrParameter, "failed to remove snapshots")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Removing snapshot %v via proxy", names)

	return nil
}
