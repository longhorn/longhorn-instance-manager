package proxy

import (
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
	spdktypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
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
		if err := task.AddReplica(req.Size, req.CurrentSize, req.ReplicaAddress, req.ReplicaName,
			int(req.FileSyncHttpClientTimeout), req.FastSync); err != nil {
			return nil, err
		}
	}
	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) ReplicaAdd(ctx context.Context, req *rpc.EngineReplicaAddRequest) (resp *emptypb.Empty, err error) {
	c, err := getSPDKClientFromEngineAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer c.Close()

	replicaAddress := strings.TrimPrefix(req.ReplicaAddress, "tcp://")

	err = c.EngineReplicaAdd(req.ProxyEngineRequest.EngineName, req.ReplicaName, replicaAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to add replica %v", replicaAddress).Error())
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
	defer c.Close()

	recv, err := c.ReplicaList()
	if err != nil {
		return nil, err
	}

	replicas := []*eptypes.ControllerReplica{}
	for _, r := range recv {
		replica := &eptypes.ControllerReplica{
			Address: &eptypes.ReplicaAddress{
				Address: r.Address,
			},
			Mode: eptypes.ReplicaModeToGRPCReplicaMode(r.Mode),
		}
		replicas = append(replicas, replica)
	}

	return &rpc.EngineReplicaListProxyResponse{
		ReplicaList: &eptypes.ReplicaListReply{
			Replicas: replicas,
		},
	}, nil
}

func replicaModeToGRPCReplicaMode(mode spdktypes.Mode) eptypes.ReplicaMode {
	switch mode {
	case spdktypes.ModeWO:
		return eptypes.ReplicaMode_WO
	case spdktypes.ModeRW:
		return eptypes.ReplicaMode_RW
	case spdktypes.ModeERR:
		return eptypes.ReplicaMode_ERR
	}
	return eptypes.ReplicaMode_ERR
}

func (ops V2DataEngineProxyOps) ReplicaList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaListProxyResponse, err error) {
	c, err := getSPDKClientFromEngineAddress(req.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get SPDK client from engine address %v", req.Address).Error())
	}
	defer c.Close()

	recv, err := c.EngineGet(req.EngineName)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to get engine %v", req.EngineName).Error())
	}

	replicas := []*eptypes.ControllerReplica{}
	for replicaName, mode := range recv.ReplicaModeMap {
		address, ok := recv.ReplicaAddressMap[replicaName]
		if !ok {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get replica address for %v", replicaName)
		}
		replica := &eptypes.ControllerReplica{
			Address: &eptypes.ReplicaAddress{
				Address: address,
			},
			Mode: replicaModeToGRPCReplicaMode(mode),
		}
		replicas = append(replicas, replica)
	}

	return &rpc.EngineReplicaListProxyResponse{
		ReplicaList: &eptypes.ReplicaListReply{
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
		Status: make(map[string]*eptypes.ReplicaRebuildStatusResponse),
	}
	for k, v := range recv {
		resp.Status[k] = &eptypes.ReplicaRebuildStatusResponse{
			Error:              v.Error,
			IsRebuilding:       v.IsRebuilding,
			Progress:           int32(v.Progress),
			State:              v.State,
			FromReplicaAddress: v.FromReplicaAddress,
		}
	}

	return resp, nil
}

func (ops V2DataEngineProxyOps) ReplicaRebuildingStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildStatusProxyResponse, err error) {
	/* TODO: implement this */
	return &rpc.EngineReplicaRebuildStatusProxyResponse{
		Status: make(map[string]*eptypes.ReplicaRebuildStatusResponse),
	}, nil
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
	defer c.Close()

	return nil, c.ReplicaDelete(req.ReplicaAddress)
}

func (ops V2DataEngineProxyOps) ReplicaRemove(ctx context.Context, req *rpc.EngineReplicaRemoveRequest) (*emptypb.Empty, error) {
	c, err := getSPDKClientFromEngineAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer c.Close()

	return nil, c.EngineReplicaDelete(req.ProxyEngineRequest.EngineName, req.ReplicaName, req.ReplicaAddress)
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
	defer c.Close()

	if _, err = c.ReplicaUpdate(req.ReplicaAddress, eptypes.GRPCReplicaModeToReplicaMode(req.Mode)); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) ReplicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *emptypb.Empty, err error) {
	/* TODO: implement this */
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}
