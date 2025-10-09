package proxy

import (
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/enginerpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	spdktypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

func (p *Proxy) VolumeSnapshot(ctx context.Context, req *rpc.EngineVolumeSnapshotRequest) (resp *rpc.EngineVolumeSnapshotProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Snapshotting volume: snapshot %v", req.SnapshotVolume.Name)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.VolumeSnapshot(ctx, req)
}

func (ops V1DataEngineProxyOps) VolumeSnapshot(ctx context.Context, req *rpc.EngineVolumeSnapshotRequest) (resp *rpc.EngineVolumeSnapshotProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	recv, err := c.VolumeSnapshot(req.SnapshotVolume.Name, req.SnapshotVolume.Labels, req.SnapshotVolume.FreezeFilesystem)
	if err != nil {
		return nil, err
	}

	return &rpc.EngineVolumeSnapshotProxyResponse{
		Snapshot: &enginerpc.VolumeSnapshotReply{
			Name: recv,
		},
	}, nil
}

func (ops V2DataEngineProxyOps) VolumeSnapshot(ctx context.Context, req *rpc.EngineVolumeSnapshotRequest) (resp *rpc.EngineVolumeSnapshotProxyResponse, err error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	snapshotName := req.SnapshotVolume.Name
	if snapshotName == "" {
		snapshotName = util.UUID()
	}

	_, err = c.EngineSnapshotCreate(req.ProxyEngineRequest.EngineName, snapshotName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to create snapshot %v: %v", snapshotName, err)
	}
	return &rpc.EngineVolumeSnapshotProxyResponse{
		Snapshot: &enginerpc.VolumeSnapshotReply{
			Name: snapshotName,
		},
	}, nil
}

func (p *Proxy) SnapshotList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotListProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Listing snapshots")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.SnapshotList(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotListProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.Address,
				"engineName": req.EngineName,
				"volumeName": req.VolumeName,
				"dataEngine": req.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	recv, err := c.ReplicaList()
	if err != nil {
		return nil, err
	}

	snapshotsDiskInfo, err := esync.GetSnapshotsInfo(recv, req.VolumeName)
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineSnapshotListProxyResponse{
		Disks: map[string]*rpc.EngineSnapshotDiskInfo{},
	}
	for k, v := range snapshotsDiskInfo {
		resp.Disks[k] = &rpc.EngineSnapshotDiskInfo{
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

	return resp, nil
}

func (ops V2DataEngineProxyOps) SnapshotList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotListProxyResponse, err error) {
	c, err := getSPDKClientFromAddress(req.Address)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK client from engine address %v", req.Address)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.Address,
				"engineName": req.EngineName,
				"volumeName": req.VolumeName,
				"dataEngine": req.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	engine, err := c.EngineGet(req.EngineName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get engine %v", req.EngineName)
	}
	disks := engine.Snapshots
	if engine.Head != nil {
		disks[engine.Head.Name] = engine.Head
	}

	resp = &rpc.EngineSnapshotListProxyResponse{
		Disks: map[string]*rpc.EngineSnapshotDiskInfo{},
	}
	for snapshotName, snapshot := range disks {
		/*
		 * If the snapshot was created before the introduction of the new attribute SnapshotTimestamp,
		 * and so this one is not available, do fallback over the old one CreationTime.
		 */
		snapshotTime := snapshot.SnapshotTimestamp
		if snapshotTime == "" {
			snapshotTime = snapshot.CreationTime
		}
		resp.Disks[snapshotName] = &rpc.EngineSnapshotDiskInfo{
			Name:        snapshot.Name,
			Parent:      snapshot.Parent,
			Children:    snapshot.Children,
			Removed:     false,
			UserCreated: snapshot.UserCreated,
			Created:     snapshotTime,
			Size:        strconv.FormatUint(snapshot.ActualSize, 10),
			Labels:      map[string]string{},
		}
	}
	return resp, nil
}

func (p *Proxy) SnapshotClone(ctx context.Context, req *rpc.EngineSnapshotCloneRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Cloning snapshot from %v to %v", req.FromEngineAddress, req.ProxyEngineRequest.Address)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.SnapshotClone(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotClone(ctx context.Context, req *rpc.EngineSnapshotCloneRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})

	cFrom, err := eclient.NewControllerClient(req.FromEngineAddress, req.FromVolumeName, req.FromEngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := cFrom.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	cTo, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := cTo.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	err = esync.CloneSnapshot(cTo, cFrom, req.ProxyEngineRequest.VolumeName, req.FromVolumeName, req.SnapshotName,
		req.ExportBackingImageIfExist, int(req.FileSyncHttpClientTimeout), req.GrpcTimeoutSeconds)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) SnapshotClone(ctx context.Context, req *rpc.EngineSnapshotCloneRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":        req.ProxyEngineRequest.Address,
		"engineName":        req.ProxyEngineRequest.EngineName,
		"volumeName":        req.ProxyEngineRequest.VolumeName,
		"dataEngine":        req.ProxyEngineRequest.DataEngine,
		"fromEngineAddress": req.FromEngineAddress,
		"fromEngineName":    req.FromEngineName,
		"fromVolumeName":    req.FromVolumeName,
		"snapshotName":      req.SnapshotName,
	})

	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	err = c.EngineSnapshotClone(req.ProxyEngineRequest.EngineName, req.SnapshotName, req.FromEngineName, req.FromEngineAddress, spdkrpc.CloneMode(req.CloneMode))
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to do clone snapshot %v: %v", req.SnapshotName, err)
	}

	return &emptypb.Empty{}, nil
}

func (p *Proxy) SnapshotCloneStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotCloneStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Getting snapshot clone status")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.SnapshotCloneStatus(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotCloneStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotCloneStatusProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.Address,
				"engineName": req.EngineName,
				"volumeName": req.VolumeName,
				"dataEngine": req.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	recv, err := esync.CloneStatus(c, req.VolumeName)
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineSnapshotCloneStatusProxyResponse{
		Status: map[string]*enginerpc.SnapshotCloneStatusResponse{},
	}
	for k, v := range recv {
		resp.Status[k] = &enginerpc.SnapshotCloneStatusResponse{
			IsCloning:          v.IsCloning,
			Error:              v.Error,
			Progress:           int32(v.Progress),
			State:              v.State,
			FromReplicaAddress: v.FromReplicaAddress,
			SnapshotName:       v.SnapshotName,
		}
	}

	return resp, nil
}

func (ops V2DataEngineProxyOps) SnapshotCloneStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotCloneStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	c, err := getSPDKClientFromAddress(req.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	recv, err := c.EngineGet(req.EngineName)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to get engine %v", req.EngineName).Error())
	}

	replicaName, replicaAddress := "", ""
	for rName, mode := range recv.ReplicaModeMap {
		if mode != spdktypes.ModeRW {
			continue
		}
		address, ok := recv.ReplicaAddressMap[rName]
		if !ok {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get replica address for %v", replicaName)
		}
		replicaName = rName
		replicaAddress = address
		break
	}
	if replicaName == "" || replicaAddress == "" {
		return nil, grpcstatus.Error(grpccodes.Internal, "cannot find a RW replica")
	}

	replicaClient, err := getSPDKClientFromAddress(replicaAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "cannot ger client for replica %v", replicaName)
	}
	defer func() {
		if closeErr := replicaClient.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	status, err := replicaClient.ReplicaSnapshotCloneDstStatusCheck(replicaName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get clone status for %v: %v", replicaName, err)
	}
	resp = &rpc.EngineSnapshotCloneStatusProxyResponse{
		Status: map[string]*enginerpc.SnapshotCloneStatusResponse{},
	}
	tcpReplicaAddress := types.AddTcpPrefixForAddress(replicaAddress)
	resp.Status[tcpReplicaAddress] = &enginerpc.SnapshotCloneStatusResponse{
		IsCloning:          status.IsCloning,
		Error:              status.Error,
		Progress:           int32(status.Progress),
		State:              status.State,
		FromReplicaAddress: status.SrcReplicaAddress,
		SnapshotName:       status.SnapshotName,
	}

	return resp, nil
}

func (p *Proxy) SnapshotRevert(ctx context.Context, req *rpc.EngineSnapshotRevertRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Reverting snapshot %v", req.Name)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.SnapshotRevert(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotRevert(ctx context.Context, req *rpc.EngineSnapshotRevertRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	if err := c.VolumeRevert(req.Name); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) SnapshotRevert(ctx context.Context, req *rpc.EngineSnapshotRevertRequest) (resp *emptypb.Empty, err error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	err = c.EngineSnapshotRevert(req.ProxyEngineRequest.EngineName, req.Name)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to create snapshot %v: %v", req.Name, err)
	}

	return &emptypb.Empty{}, nil
}

func (p *Proxy) SnapshotPurge(ctx context.Context, req *rpc.EngineSnapshotPurgeRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Info("Purging snapshots")

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.SnapshotPurge(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotPurge(ctx context.Context, req *rpc.EngineSnapshotPurgeRequest) (resp *emptypb.Empty, err error) {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}

	if err := task.PurgeSnapshots(req.SkipIfInProgress); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) SnapshotPurge(ctx context.Context, req *rpc.EngineSnapshotPurgeRequest) (resp *emptypb.Empty, err error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	// For v2 Data Engine, snapshot purge is no longer a time-consuming operation
	err = c.EngineSnapshotPurge(req.ProxyEngineRequest.EngineName)
	return &emptypb.Empty{}, nil
}

func (p *Proxy) SnapshotPurgeStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotPurgeStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Getting snapshot purge status")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.SnapshotPurgeStatus(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotPurgeStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotPurgeStatusProxyResponse, err error) {
	task, err := esync.NewTask(ctx, req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}

	recv, err := task.PurgeSnapshotStatus()
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineSnapshotPurgeStatusProxyResponse{
		Status: map[string]*enginerpc.SnapshotPurgeStatusResponse{},
	}
	for k, v := range recv {
		resp.Status[k] = &enginerpc.SnapshotPurgeStatusResponse{
			IsPurging: v.IsPurging,
			Error:     v.Error,
			Progress:  int32(v.Progress),
			State:     v.State,
		}
	}

	return resp, nil
}

func (ops V2DataEngineProxyOps) SnapshotPurgeStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineSnapshotPurgeStatusProxyResponse, err error) {
	/* TODO: implement this */
	return &rpc.EngineSnapshotPurgeStatusProxyResponse{
		Status: map[string]*enginerpc.SnapshotPurgeStatusResponse{},
	}, nil
}

func (p *Proxy) SnapshotRemove(ctx context.Context, req *rpc.EngineSnapshotRemoveRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Removing snapshots %v", req.Names)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.SnapshotRemove(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotRemove(ctx context.Context, req *rpc.EngineSnapshotRemoveRequest) (resp *emptypb.Empty, err error) {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for _, name := range req.Names {
		if err := task.DeleteSnapshot(name); err != nil {
			if err != nil {
				lastErr = err
				logrus.WithError(err).Warnf("Failed to delete snapshot %s", name)
			}
		}
	}

	return &emptypb.Empty{}, lastErr
}

func (ops V2DataEngineProxyOps) SnapshotRemove(ctx context.Context, req *rpc.EngineSnapshotRemoveRequest) (resp *emptypb.Empty, err error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	var lastErr error
	for _, name := range req.Names {
		err = c.EngineSnapshotDelete(req.ProxyEngineRequest.EngineName, name)
		if err != nil {
			lastErr = err
			logrus.WithError(err).Warnf("Failed to delete snapshot %s", name)
		}
	}

	return &emptypb.Empty{}, lastErr
}

func (p *Proxy) SnapshotHash(ctx context.Context, req *rpc.EngineSnapshotHashRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Hashing snapshot %v with rehash %v", req.SnapshotName, req.Rehash)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.SnapshotHash(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotHash(ctx context.Context, req *rpc.EngineSnapshotHashRequest) (resp *emptypb.Empty, err error) {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}

	if err := task.HashSnapshot(req.SnapshotName, req.Rehash); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) SnapshotHash(ctx context.Context, req *rpc.EngineSnapshotHashRequest) (resp *emptypb.Empty, err error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	err = c.EngineSnapshotHash(req.ProxyEngineRequest.EngineName, req.SnapshotName, req.Rehash)
	return &emptypb.Empty{}, err
}

func (p *Proxy) SnapshotHashStatus(ctx context.Context, req *rpc.EngineSnapshotHashStatusRequest) (resp *rpc.EngineSnapshotHashStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Trace("Getting snapshot hash status")

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.SnapshotHashStatus(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotHashStatus(ctx context.Context, req *rpc.EngineSnapshotHashStatusRequest) (resp *rpc.EngineSnapshotHashStatusProxyResponse, err error) {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}

	recv, err := task.HashSnapshotStatus(req.SnapshotName)
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineSnapshotHashStatusProxyResponse{
		Status: map[string]*enginerpc.SnapshotHashStatusResponse{},
	}
	for k, v := range recv {
		resp.Status[k] = &enginerpc.SnapshotHashStatusResponse{
			State:             v.State,
			Checksum:          v.Checksum,
			Error:             v.Error,
			SilentlyCorrupted: v.SilentlyCorrupted,
		}
	}

	return resp, nil
}

func (ops V2DataEngineProxyOps) SnapshotHashStatus(ctx context.Context, req *rpc.EngineSnapshotHashStatusRequest) (resp *rpc.EngineSnapshotHashStatusProxyResponse, err error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	recv, err := c.EngineSnapshotHashStatus(req.ProxyEngineRequest.EngineName, req.SnapshotName)
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineSnapshotHashStatusProxyResponse{
		Status: map[string]*enginerpc.SnapshotHashStatusResponse{},
	}
	for k, v := range recv.Status {
		resp.Status[k] = &enginerpc.SnapshotHashStatusResponse{
			State:             v.State,
			Checksum:          v.Checksum,
			Error:             v.Error,
			SilentlyCorrupted: v.SilentlyCorrupted,
		}
	}

	return resp, nil
}
