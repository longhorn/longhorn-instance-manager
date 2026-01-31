package proxy

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/enginerpc"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	spdktypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

func (p *Proxy) ReplicaAdd(ctx context.Context, req *rpc.EngineReplicaAddRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":     req.ProxyEngineRequest.Address,
		"engineName":     req.ProxyEngineRequest.EngineName,
		"volumeName":     req.ProxyEngineRequest.VolumeName,
		"replicaName":    req.ReplicaName,
		"replicaAddress": req.ReplicaAddress,
		"restore":        req.Restore,
		"size":           req.Size,
		"currentSize":    req.CurrentSize,
		"fastSync":       req.FastSync,
		"localSync":      req.LocalSync,
	})
	log.Info("Adding replica")

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.ReplicaAdd(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaAdd(ctx context.Context, req *rpc.EngineReplicaAddRequest) (resp *emptypb.Empty, err error) {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}

	if req.Restore {
		if err := task.AddRestoreReplica(req.Size, req.CurrentSize, req.ReplicaAddress, req.ReplicaName); err != nil {
			return nil, err
		}
	} else {
		var localSync *etypes.FileLocalSync
		if req.LocalSync != nil {
			localSync = &etypes.FileLocalSync{
				SourcePath: req.LocalSync.SourcePath,
				TargetPath: req.LocalSync.TargetPath,
			}
		}
		if err := task.AddReplica(req.Size, req.CurrentSize, req.ReplicaAddress, req.ReplicaName,
			int(req.FileSyncHttpClientTimeout), req.FastSync, localSync, req.GrpcTimeoutSeconds); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) ReplicaAdd(ctx context.Context, req *rpc.EngineReplicaAddRequest) (resp *emptypb.Empty, err error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL":     req.ProxyEngineRequest.Address,
				"engineName":     req.ProxyEngineRequest.EngineName,
				"volumeName":     req.ProxyEngineRequest.VolumeName,
				"replicaName":    req.ReplicaName,
				"replicaAddress": req.ReplicaAddress,
				"restore":        req.Restore,
				"size":           req.Size,
				"currentSize":    req.CurrentSize,
				"fastSync":       req.FastSync,
				"localSync":      req.LocalSync,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	replicaAddress := strings.TrimPrefix(req.ReplicaAddress, "tcp://")

	err = c.EngineReplicaAdd(req.ProxyEngineRequest.EngineName, req.ReplicaName, replicaAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to add replica %v: %v", replicaAddress, err)
	}
	return &emptypb.Empty{}, nil
}

func (p *Proxy) ReplicaList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaListProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Listing replicas")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.ReplicaList(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaListProxyResponse, err error) {
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

	replicas := []*enginerpc.ControllerReplica{}
	for _, r := range recv {
		replica := &enginerpc.ControllerReplica{
			Address: &enginerpc.ReplicaAddress{
				Address: r.Address,
			},
			Mode: etypes.ReplicaModeToGRPCReplicaMode(r.Mode),
		}
		replicas = append(replicas, replica)
	}

	return &rpc.EngineReplicaListProxyResponse{
		ReplicaList: &enginerpc.ReplicaListReply{
			Replicas: replicas,
		},
	}, nil
}

func replicaModeToGRPCReplicaMode(mode spdktypes.Mode) enginerpc.ReplicaMode {
	switch mode {
	case spdktypes.ModeWO:
		return enginerpc.ReplicaMode_WO
	case spdktypes.ModeRW:
		return enginerpc.ReplicaMode_RW
	case spdktypes.ModeERR:
		return enginerpc.ReplicaMode_ERR
	}
	return enginerpc.ReplicaMode_ERR
}

func (ops V2DataEngineProxyOps) ReplicaList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaListProxyResponse, err error) {
	c, err := getSPDKClientFromAddress(req.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.Address, err)
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

	recv, err := c.EngineGet(req.EngineName)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to get engine %v", req.EngineName).Error())
	}

	replicas := []*enginerpc.ControllerReplica{}
	for replicaName, mode := range recv.ReplicaModeMap {
		address, ok := recv.ReplicaAddressMap[replicaName]
		if !ok {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get replica address for %v", replicaName)
		}
		replica := &enginerpc.ControllerReplica{
			Address: &enginerpc.ReplicaAddress{
				Address: types.AddTcpPrefixForAddress(address),
			},
			Mode: replicaModeToGRPCReplicaMode(mode),
		}
		replicas = append(replicas, replica)
	}

	return &rpc.EngineReplicaListProxyResponse{
		ReplicaList: &enginerpc.ReplicaListReply{
			Replicas: replicas,
		},
	}, nil
}

func (p *Proxy) ReplicaRebuildingStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Getting replica rebuilding status")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.ReplicaRebuildingStatus(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaRebuildingStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildStatusProxyResponse, err error) {
	task, err := esync.NewTask(ctx, req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}

	recv, err := task.RebuildStatus()
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineReplicaRebuildStatusProxyResponse{
		Status: make(map[string]*enginerpc.ReplicaRebuildStatusResponse),
	}
	for k, v := range recv {
		resp.Status[k] = &enginerpc.ReplicaRebuildStatusResponse{
			Error:                  v.Error,
			IsRebuilding:           v.IsRebuilding,
			Progress:               int32(v.Progress),
			State:                  v.State,
			FromReplicaAddressList: v.FromReplicaAddressList,
		}
	}

	return resp, nil
}

func (ops V2DataEngineProxyOps) ReplicaRebuildingStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})

	engineCli, err := getSPDKClientFromAddress(req.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.Address, err)
	}
	defer func() {
		if closeErr := engineCli.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	e, err := engineCli.EngineGet(req.EngineName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get engine %v: %v", req.EngineName, err)
	}

	// TODO: By design, there is one rebuilding replica at most for each volume; hence no need to return a map for rebuilding status.
	resp = &rpc.EngineReplicaRebuildStatusProxyResponse{
		Status: make(map[string]*enginerpc.ReplicaRebuildStatusResponse),
	}
	for replicaName, mode := range e.ReplicaModeMap {
		if mode != spdktypes.ModeWO {
			continue
		}
		replicaAddress := e.ReplicaAddressMap[replicaName]
		if replicaAddress == "" {
			continue
		}
		// TODO: Need to unify the replica address format for v1 and v2 engine
		tcpReplicaAddress := types.AddTcpPrefixForAddress(replicaAddress)
		replicaCli, err := getSPDKClientFromAddress(replicaAddress)
		if err != nil {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from replica address %v: %v", replicaAddress, err)
		}
		defer func() {
			if closeErr := replicaCli.Close(); closeErr != nil {
				log.WithError(closeErr).Warn("Failed to close SPDK client")
			}
		}()

		shallowCopyResp, err := replicaCli.ReplicaRebuildingDstShallowCopyCheck(replicaName)
		if err != nil {
			// Let the upper layer to handle this error rather than considering it as the error message of a rebuilding failure
			return nil, err
		}
		resp.Status[tcpReplicaAddress] = &enginerpc.ReplicaRebuildStatusResponse{
			Error:                  shallowCopyResp.Error,
			IsRebuilding:           shallowCopyResp.TotalState == spdktypes.ProgressStateInProgress,
			Progress:               int32(shallowCopyResp.TotalProgress),
			State:                  shallowCopyResp.TotalState,
			FromReplicaAddressList: []string{types.AddTcpPrefixForAddress(shallowCopyResp.SrcReplicaAddress)},
		}
	}

	return resp, nil
}

func (p *Proxy) ReplicaRebuildingQosSet(ctx context.Context, req *rpc.EngineReplicaRebuildingQosSetRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":   req.ProxyEngineRequest.Address,
		"engineName":   req.ProxyEngineRequest.EngineName,
		"volumeName":   req.ProxyEngineRequest.VolumeName,
		"dataEngine":   req.ProxyEngineRequest.DataEngine,
		"qosLimitMbps": req.QosLimitMbps,
	})
	log.Trace("Setting qos on replica rebuilding")

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.ReplicaRebuildingQosSet(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaRebuildingQosSet(ctx context.Context, req *rpc.EngineReplicaRebuildingQosSetRequest) (resp *emptypb.Empty, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
}

func (ops V2DataEngineProxyOps) ReplicaRebuildingQosSet(ctx context.Context, req *rpc.EngineReplicaRebuildingQosSetRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":   req.ProxyEngineRequest.Address,
		"engineName":   req.ProxyEngineRequest.EngineName,
		"volumeName":   req.ProxyEngineRequest.VolumeName,
		"dataEngine":   req.ProxyEngineRequest.DataEngine,
		"qosLimitMbps": req.QosLimitMbps,
	})

	engineCli, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := engineCli.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close SPDK engine client")
		}
	}()

	engine, err := engineCli.EngineGet(req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get engine %v: %v", req.ProxyEngineRequest.EngineName, err)
	}

	for replicaName, mode := range engine.ReplicaModeMap {
		if mode != spdktypes.ModeWO {
			continue
		}

		replicaAddress := engine.ReplicaAddressMap[replicaName]
		if replicaAddress == "" {
			log.WithField("replicaName", replicaName).Warn("Empty replica address, skipping QoS set")
			continue
		}

		replicaCli, err := getSPDKClientFromAddress(replicaAddress)
		if err != nil {
			log.WithError(err).WithField("replicaAddress", replicaAddress).
				Warn("Failed to get SPDK client from replica address")
			continue
		}
		defer func() {
			if closeErr := replicaCli.Close(); closeErr != nil {
				log.WithError(closeErr).Warn("Failed to close SPDK replica client")
			}
		}()

		if err := replicaCli.ReplicaRebuildingDstSetQosLimit(replicaName, req.QosLimitMbps); err != nil {
			log.WithError(err).WithFields(logrus.Fields{
				"replicaName":  replicaName,
				"replicaAddr":  replicaAddress,
				"qosLimitMbps": req.QosLimitMbps,
			}).Warn("Failed to set QoS on replica")
			continue
		}

		log.WithFields(logrus.Fields{
			"replicaName":  replicaName,
			"replicaAddr":  replicaAddress,
			"qosLimitMbps": req.QosLimitMbps,
		}).Trace("Successfully set QoS on replica")
	}

	return &emptypb.Empty{}, nil
}

func (p *Proxy) ReplicaVerifyRebuild(ctx context.Context, req *rpc.EngineReplicaVerifyRebuildRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
	})
	log.Infof("Verifying replica %v rebuild", req.ReplicaAddress)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.ReplicaVerifyRebuild(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaVerifyRebuild(ctx context.Context, req *rpc.EngineReplicaVerifyRebuildRequest) (resp *emptypb.Empty, err error) {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}

	err = task.VerifyRebuildReplica(req.ReplicaAddress, req.ReplicaName)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) ReplicaVerifyRebuild(ctx context.Context, req *rpc.EngineReplicaVerifyRebuildRequest) (resp *emptypb.Empty, err error) {
	/* TODO: implement this */
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}

func (p *Proxy) ReplicaRemove(ctx context.Context, req *rpc.EngineReplicaRemoveRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":     req.ProxyEngineRequest.Address,
		"engineName":     req.ProxyEngineRequest.EngineName,
		"volumeName":     req.ProxyEngineRequest.VolumeName,
		"replicaName":    req.ReplicaName,
		"replicaAddress": req.ReplicaAddress,
	})
	log.Info("Removing replica")

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.ReplicaRemove(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaRemove(ctx context.Context, req *rpc.EngineReplicaRemoveRequest) (*emptypb.Empty, error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL":     req.ProxyEngineRequest.Address,
				"engineName":     req.ProxyEngineRequest.EngineName,
				"volumeName":     req.ProxyEngineRequest.VolumeName,
				"replicaName":    req.ReplicaName,
				"replicaAddress": req.ReplicaAddress,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	err = ops.cancelSnapshotHashesForReplica(ctx, c, req.ReplicaAddress, req.ProxyEngineRequest)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"serviceURL":     req.ProxyEngineRequest.Address,
			"engineName":     req.ProxyEngineRequest.EngineName,
			"volumeName":     req.ProxyEngineRequest.VolumeName,
			"replicaName":    req.ReplicaName,
			"replicaAddress": req.ReplicaAddress,
		}).WithError(err).Warn("Failed to stop snapshot hash")
	}

	return nil, c.ReplicaDelete(req.ReplicaAddress)
}

func (ops V1DataEngineProxyOps) cancelSnapshotHashesForReplica(ctx context.Context, c *eclient.ControllerClient, replicaAddress string, proxyEngineRequest *rpc.ProxyEngineRequest) error {
	recv, err := c.ReplicaList()
	if err != nil {
		return err
	}

	snapshotsDiskInfo, err := esync.GetSnapshotsInfo(recv, proxyEngineRequest.VolumeName)
	if err != nil {
		return err
	}
	snapshots := []string{}
	for _, snapshotInfo := range snapshotsDiskInfo {
		snapshots = append(snapshots, snapshotInfo.Name)
	}

	task, err := esync.NewTask(ctx, proxyEngineRequest.Address, proxyEngineRequest.VolumeName, proxyEngineRequest.EngineName)
	if err != nil {
		return err
	}

	if err := task.CancelSnapshotHashJob(replicaAddress, snapshots); err != nil {
		return err
	}

	return nil
}

func (ops V2DataEngineProxyOps) ReplicaRemove(ctx context.Context, req *rpc.EngineReplicaRemoveRequest) (*emptypb.Empty, error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL":     req.ProxyEngineRequest.Address,
				"engineName":     req.ProxyEngineRequest.EngineName,
				"volumeName":     req.ProxyEngineRequest.VolumeName,
				"replicaName":    req.ReplicaName,
				"replicaAddress": req.ReplicaAddress,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	replicaAddress := strings.TrimPrefix(req.ReplicaAddress, "tcp://")

	return nil, c.EngineReplicaDelete(req.ProxyEngineRequest.EngineName, req.ReplicaName, replicaAddress)
}

func (p *Proxy) ReplicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
	})
	log.Infof("Updating replica mode to %v", req.Mode)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}

	return ops.ReplicaModeUpdate(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Infof("Updating replica mode to %v", req.Mode)

	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	if _, err = c.ReplicaUpdate(req.ReplicaAddress, etypes.GRPCReplicaModeToReplicaMode(req.Mode)); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) ReplicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *emptypb.Empty, err error) {
	/* TODO: implement this */
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}

func (p *Proxy) ReplicaRebuildConcurrentSyncLimitSet(ctx context.Context, req *rpc.EngineReplicaRebuildConcurrentSyncLimitSetRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
	})
	log.Infof("Updating replica rebuild concurrent sync limit to %d", req.Limit)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}

	return ops.ReplicaRebuildConcurrentSyncLimitSet(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaRebuildConcurrentSyncLimitSet(ctx context.Context, req *rpc.EngineReplicaRebuildConcurrentSyncLimitSetRequest) (resp *emptypb.Empty, err error) {
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

	if err = c.ReplicaRebuildConcurrentSyncLimitSet(int(req.Limit)); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) ReplicaRebuildConcurrentSyncLimitSet(ctx context.Context, req *rpc.EngineReplicaRebuildConcurrentSyncLimitSetRequest) (resp *emptypb.Empty, err error) {
	/* TODO: implement this */
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}

func (p *Proxy) ReplicaRebuildConcurrentSyncLimitGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildConcurrentSyncLimitGetResponse, err error) {
	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}

	return ops.ReplicaRebuildConcurrentSyncLimitGet(ctx, req)
}

func (ops V1DataEngineProxyOps) ReplicaRebuildConcurrentSyncLimitGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildConcurrentSyncLimitGetResponse, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName,
		req.EngineName)
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

	limit, err := c.ReplicaRebuildConcurrentSyncLimitGet()
	if err != nil {
		return nil, err
	}

	return &rpc.EngineReplicaRebuildConcurrentSyncLimitGetResponse{
		Limit: int32(limit),
	}, nil
}

func (ops V2DataEngineProxyOps) ReplicaRebuildConcurrentSyncLimitGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildConcurrentSyncLimitGetResponse, err error) {
	/* TODO: implement this */
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}
