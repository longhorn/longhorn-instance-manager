package client

import (
	"github.com/pkg/errors"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

<<<<<<< HEAD
func (c *ProxyClient) VolumeGet(serviceAddress string) (info *etypes.VolumeInfo, err error) {
	input := map[string]string{
=======
func (c *ProxyClient) VolumeGet(backendStoreDriver, engineName, volumeName, serviceAddress string) (info *etypes.VolumeInfo, err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return nil, errors.Wrap(err, "failed to get volume")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to get volume", c.getProxyErrorPrefix(serviceAddress))
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
	resp, err := c.service.VolumeGet(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return nil, err
	}

	info = &etypes.VolumeInfo{
		Name:                      resp.Volume.Name,
		Size:                      resp.Volume.Size,
		ReplicaCount:              int(resp.Volume.ReplicaCount),
		Endpoint:                  resp.Volume.Endpoint,
		Frontend:                  resp.Volume.Frontend,
		FrontendState:             resp.Volume.FrontendState,
		IsExpanding:               resp.Volume.IsExpanding,
		LastExpansionError:        resp.Volume.LastExpansionError,
		LastExpansionFailedAt:     resp.Volume.LastExpansionFailedAt,
		UnmapMarkSnapChainRemoved: resp.Volume.UnmapMarkSnapChainRemoved,
	}
	return info, nil
}

<<<<<<< HEAD
func (c *ProxyClient) VolumeExpand(serviceAddress string, size int64) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) VolumeExpand(backendStoreDriver, engineName, volumeName, serviceAddress string,
	size int64) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to expand volume")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to expand volume", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineVolumeExpandRequest{
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
		Expand: &eptypes.VolumeExpandRequest{
			Size: size,
		},
	}
	_, err = c.service.VolumeExpand(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
func (c *ProxyClient) VolumeFrontendStart(serviceAddress, frontendName string) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) VolumeFrontendStart(backendStoreDriver, engineName, volumeName, serviceAddress, frontendName string) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
		"frontendName":   frontendName,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to start volume frontend")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to start volume frontend %v", c.getProxyErrorPrefix(serviceAddress), frontendName)
	}()

	req := &rpc.EngineVolumeFrontendStartRequest{
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
		FrontendStart: &eptypes.VolumeFrontendStartRequest{
			Frontend: frontendName,
		},
	}
	_, err = c.service.VolumeFrontendStart(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
func (c *ProxyClient) VolumeFrontendShutdown(serviceAddress string) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) VolumeFrontendShutdown(backendStoreDriver, engineName, volumeName,
	serviceAddress string) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to shutdown volume frontend")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to shutdown volume frontend", c.getProxyErrorPrefix(serviceAddress))
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
	_, err = c.service.VolumeFrontendShutdown(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}

<<<<<<< HEAD
func (c *ProxyClient) VolumeUnmapMarkSnapChainRemovedSet(serviceAddress string, enabled bool) (err error) {
	input := map[string]string{
=======
func (c *ProxyClient) VolumeUnmapMarkSnapChainRemovedSet(backendStoreDriver, engineName, volumeName,
	serviceAddress string, enabled bool) (err error) {
	input := map[string]string{
		"engineName":     engineName,
		"volumeName":     volumeName,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
		"serviceAddress": serviceAddress,
	}
	if err := validateProxyMethodParameters(input); err != nil {
		return errors.Wrap(err, "failed to set volume flag UnmapMarkSnapChainRemoved")
	}

	defer func() {
		err = errors.Wrapf(err, "%v failed to set UnmapMarkSnapChainRemoved", c.getProxyErrorPrefix(serviceAddress))
	}()

	req := &rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest{
<<<<<<< HEAD
		ProxyEngineRequest: &rpc.ProxyEngineRequest{Address: serviceAddress},
		UnmapMarkSnap:      &eptypes.VolumeUnmapMarkSnapChainRemovedSetRequest{Enabled: enabled},
=======
		ProxyEngineRequest: &rpc.ProxyEngineRequest{
			Address:            serviceAddress,
			EngineName:         engineName,
			BackendStoreDriver: rpc.BackendStoreDriver(driver),
			VolumeName:         volumeName,
		},
		UnmapMarkSnap: &eptypes.VolumeUnmapMarkSnapChainRemovedSetRequest{Enabled: enabled},
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
	}
	_, err = c.service.VolumeUnmapMarkSnapChainRemovedSet(getContextWithGRPCTimeout(c.ctx), req)
	if err != nil {
		return err
	}

	return nil
}
