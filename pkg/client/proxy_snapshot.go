package client

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eutil "github.com/longhorn/longhorn-engine/pkg/util"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (c *ProxyClient) VolumeSnapshot(serviceAddress, volumeName string, labels map[string]string) (snapshotName string, err error) {
	if serviceAddress == "" {
		return "", errors.Wrapf(ErrParameter, "failed to snapshot volume")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Snapshotting volume via proxy")

	for key, value := range labels {
		if errList := eutil.IsQualifiedName(key); len(errList) > 0 {
			return "", errors.Errorf("invalid key %v for label: %v", key, errList[0])
		}

		// We don't need to validate the Label value since we're allowing for any form of data to be stored, similar
		// to Kubernetes Annotations. Of course, we should make sure it isn't empty.
		if value == "" {
			return "", errors.Errorf("invalid empty value for label with key %v", key)
		}
	}

	req := &rpc.EngineVolumeSnapshotRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		SnapshotVolume: &eptypes.VolumeSnapshotRequest{
			Name:   volumeName,
			Labels: labels,
		},
	}
	recv, err := c.service.VolumeSnapshot(c.ctx, req)
	if err != nil {
		return "", errors.Wrapf(err, "failed to snapshot volume via proxy %v to %v", c.ServiceURL, serviceAddress)
	}

	return recv.Snapshot.Name, nil
}

func (c *ProxyClient) SnapshotList(serviceAddress string) (snapshotDiskInfo map[string]*etypes.DiskInfo, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to list snapshots")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Listing snapshots via proxy")

	req := &rpc.ProxyEngineRequest{
		Address: serviceAddress,
	}
	resp, err := c.service.SnapshotList(c.ctx, req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list replicas for volume via proxy %v to %v", c.ServiceURL, serviceAddress)
	}

	snapshotDiskInfo = map[string]*etypes.DiskInfo{}
	for k, v := range resp.Disks {
		if v.Children == nil {
			v.Children = map[string]bool{}
		}
		if v.Labels == nil {
			v.Labels = map[string]string{}
		}
		snapshotDiskInfo[k] = &etypes.DiskInfo{
			Name:        v.Name,
			Parent:      v.Parent,
			Children:    v.Children,
			Removed:     v.Removed,
			UserCreated: v.UserCreated,
			Created:     v.Created,
			Size:        v.Size,
			Labels:      v.Labels,
		}
	}
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

	req := &rpc.EngineSnapshotRemoveRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address: serviceAddress,
		},
		Names: names,
	}
	_, err = c.service.SnapshotRemove(c.ctx, req)
	if err != nil {
		return errors.Wrapf(err, "failed to remove snapshot %v via proxy %v to %v", names, c.ServiceURL, serviceAddress)
	}

	return nil
}
