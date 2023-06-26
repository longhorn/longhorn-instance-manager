package client

import (
	"github.com/pkg/errors"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eutil "github.com/longhorn/longhorn-engine/pkg/util"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
)

<<<<<<< HEAD
func (c *ProxyClient) VolumeSnapshot(serviceAddress, volumeSnapshotName string, labels map[string]string) (snapshotName string, err error) {
	input := map[string]string{
=======
func (c *ProxyClient) VolumeSnapshot(backendStoreDriver, engineName, volumeName, serviceAddress,
	volumeSnapshotName string, labels map[string]string) (snapshotName string, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return "", errors.Wrap(err, "failed to snapshot volume")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to snapshot volume", c.getProxyErrorPrefix(serviceAddress))
	}()

	for key, value := range labels {
		if errList := eutil.IsQualifiedName(key); len(errList) > 0 {
			err = errors.Errorf("invalid key %v for label: %v", key, errList[0])
			return "", err
		}

		// We don't need to validate the Label value since we're allowing for any form of data to be stored, similar
		// to Kubernetes Annotations. Of course, we should make sure it isn't empty.
		if value == "" {
			err = errors.Errorf("invalid empty value for label with key %v", key)
			return "", err
		}
	}

	req := &rpc.EngineVolumeSnapshotRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
<<<<<<< HEAD
			Address: serviceAddress,
=======
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		},
		SnapshotVolume: &eptypes.VolumeSnapshotRequest{
			Name:   volumeSnapshotName,
			Labels: labels,
		},
	}
	recv, err := c.service.VolumeSnapshot(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return "", err
	}

	return recv.Snapshot.Name, nil
}

<<<<<<< HEAD
func (c *ProxyClient) SnapshotList(serviceAddress string) (snapshotDiskInfo map[string]*etypes.DiskInfo, err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotList(backendStoreDriver, engineName, volumeName,
	serviceAddress string) (snapshotDiskInfo map[string]*etypes.DiskInfo, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to list snapshots")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to list snapshots", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
<<<<<<< HEAD
		Address: serviceAddress,
=======
		Address:            serviceAddress,
		EngineName:         engineName,
		BackendStoreDriver: rpc.BackendStoreDriver(driver),
		VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
	}
	resp, err := c.service.SnapshotList(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
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

<<<<<<< HEAD
func (c *ProxyClient) SnapshotClone(serviceAddress, name, fromController string, fileSyncHTTPClientTimeout int) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotClone(backendStoreDriver, engineName, volumeName, serviceAddress, snapshotName,
	fromController string, fileSyncHTTPClientTimeout int) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
		"name":           name,
		"fromController": fromController,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to clone snapshot")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to clone snapshot %v from %v", c.getProxyErrorPrefix(serviceAddress), name, fromController)
	}()

	req := &rpc.EngineSnapshotCloneRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
<<<<<<< HEAD
			Address: serviceAddress,
=======
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		},
		FromController:            fromController,
		SnapshotName:              name,
		ExportBackingImageIfExist: false,
		FileSyncHttpClientTimeout: int32(fileSyncHTTPClientTimeout),
	}
	_, err = c.service.SnapshotClone(getContextWithGRPCLongTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
func (c *ProxyClient) SnapshotCloneStatus(serviceAddress string) (status map[string]*SnapshotCloneStatus, err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotCloneStatus(backendStoreDriver, engineName, volumeName, serviceAddress string) (status map[string]*SnapshotCloneStatus, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot clone status")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get snapshot clone status", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
<<<<<<< HEAD
		Address: serviceAddress,
=======
		Address:            serviceAddress,
		EngineName:         engineName,
		BackendStoreDriver: rpc.BackendStoreDriver(driver),
		VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
	}
	recv, err := c.service.SnapshotCloneStatus(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	status = map[string]*SnapshotCloneStatus{}
	for k, v := range recv.Status {
		status[k] = &SnapshotCloneStatus{
			IsCloning:          v.IsCloning,
			Error:              v.Error,
			Progress:           int(v.Progress),
			State:              v.State,
			FromReplicaAddress: v.FromReplicaAddress,
			SnapshotName:       v.SnapshotName,
		}
	}
	return status, nil
}

<<<<<<< HEAD
func (c *ProxyClient) SnapshotRevert(serviceAddress string, name string) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotRevert(backendStoreDriver, engineName, volumeName, serviceAddress string,
	name string) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
		"name":           name,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to revert volume to snapshot")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to revert volume to snapshot %v", c.getProxyErrorPrefix(serviceAddress), name)
	}()

	if name == etypes.VolumeHeadName {
		err = errors.Errorf("invalid operation: cannot revert to %v", etypes.VolumeHeadName)
		return err
	}

	req := &rpc.EngineSnapshotRevertRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
<<<<<<< HEAD
			Address: serviceAddress,
=======
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		},
		Name: name,
	}
	_, err = c.service.SnapshotRevert(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
func (c *ProxyClient) SnapshotPurge(serviceAddress string, skipIfInProgress bool) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotPurge(backendStoreDriver, engineName, volumeName, serviceAddress string,
	skipIfInProgress bool) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to purge snapshots")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to purge snapshots", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineSnapshotPurgeRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
<<<<<<< HEAD
			Address: serviceAddress,
=======
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		},
		SkipIfInProgress: skipIfInProgress,
	}
	_, err = c.service.SnapshotPurge(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
func (c *ProxyClient) SnapshotPurgeStatus(serviceAddress string) (status map[string]*SnapshotPurgeStatus, err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotPurgeStatus(backendStoreDriver, engineName, volumeName, serviceAddress string) (status map[string]*SnapshotPurgeStatus, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot purge status")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get snapshot purge status", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.ProxyEngineRequest{
<<<<<<< HEAD
		Address: serviceAddress,
=======
		Address:            serviceAddress,
		EngineName:         engineName,
		BackendStoreDriver: rpc.BackendStoreDriver(driver),
		VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
	}

	recv, err := c.service.SnapshotPurgeStatus(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	status = make(map[string]*SnapshotPurgeStatus)
	for k, v := range recv.Status {
		status[k] = &SnapshotPurgeStatus{
			Error:     v.Error,
			IsPurging: v.IsPurging,
			Progress:  int(v.Progress),
			State:     v.State,
		}
	}
	return status, nil
}

<<<<<<< HEAD
func (c *ProxyClient) SnapshotRemove(serviceAddress string, names []string) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotRemove(backendStoreDriver, engineName, volumeName, serviceAddress string,
	names []string) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrapf(err, "failed to remove snapshot %v", names)
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to remove snapshot %v", c.getProxyErrorPrefix(serviceAddress), names)
	}()

	req := &rpc.EngineSnapshotRemoveRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
<<<<<<< HEAD
			Address: serviceAddress,
=======
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		},
		Names: names,
	}
	_, err = c.service.SnapshotRemove(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
func (c *ProxyClient) SnapshotHash(serviceAddress string, snapshotName string, rehash bool) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotHash(backendStoreDriver, engineName, volumeName, serviceAddress string,
	snapshotName string, rehash bool) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to hash snapshot")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to hash snapshot", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineSnapshotHashRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
<<<<<<< HEAD
			Address: serviceAddress,
=======
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		},
		SnapshotName: snapshotName,
		Rehash:       rehash,
	}
	_, err = c.service.SnapshotHash(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
func (c *ProxyClient) SnapshotHashStatus(serviceAddress, snapshotName string) (status map[string]*SnapshotHashStatus, err error) {
	input := map[string]string{
=======
func (c *ProxyClient) SnapshotHashStatus(backendStoreDriver, engineName, volumeName, serviceAddress,
	snapshotName string) (status map[string]*SnapshotHashStatus, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get snapshot hash status")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get snapshot hash status", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineSnapshotHashStatusRequest{
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
<<<<<<< HEAD
			Address: serviceAddress,
=======
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			VolumeName:         volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		},
		SnapshotName: snapshotName,
	}

	recv, err := c.service.SnapshotHashStatus(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	status = make(map[string]*SnapshotHashStatus)
	for k, v := range recv.Status {
		status[k] = &SnapshotHashStatus{
			State:             v.State,
			Checksum:          v.Checksum,
			Error:             v.Error,
			SilentlyCorrupted: v.SilentlyCorrupted,
		}
	}

	return status, nil
}
