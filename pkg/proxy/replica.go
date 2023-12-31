package proxy

import (
	"strings"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"
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

	v, err := executeProxyOp(ctx, ProxyOpsReplicaAdd, req.ProxyEngineRequest.DataEngine, req)
	return v.(*emptypb.Empty), err
}

func replicaAdd(ctx context.Context, req *rpc.EngineReplicaAddRequest) (resp *emptypb.Empty, err error) {
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

func spdkReplicaAdd(ctx context.Context, req *rpc.EngineReplicaAddRequest, spdkServiceAddress string) (resp *emptypb.Empty, err error) {
	c, err := spdkclient.NewSPDKClient(spdkServiceAddress)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	replicaAddress := strings.TrimPrefix(req.ReplicaAddress, "tcp://")

	err = c.EngineReplicaAdd(req.ProxyEngineRequest.EngineName, req.ReplicaName, replicaAddress)
	if err != nil {
		return nil, err
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

	v, err := executeProxyOp(ctx, ProxyOpsReplicaList, req.DataEngine, req)
	return v.(*rpc.EngineReplicaListProxyResponse), err
}

func replicaList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaListProxyResponse, err error) {
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

func spdkReplicaList(ctx context.Context, req *rpc.ProxyEngineRequest, spdkServiceAddress string) (resp *rpc.EngineReplicaListProxyResponse, err error) {
	c, err := spdkclient.NewSPDKClient(spdkServiceAddress)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	recv, err := c.EngineGet(req.EngineName)
	if err != nil {
		return nil, err
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

	v, err := executeProxyOp(ctx, ProxyOpsReplicaRebuildingStatus, req.DataEngine, req)
	return v.(*rpc.EngineReplicaRebuildStatusProxyResponse), err
}

func replicaRebuildingStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildStatusProxyResponse, err error) {
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

func spdkReplicaRebuildingStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildStatusProxyResponse, err error) {
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

	v, err := executeProxyOp(ctx, ProxyOpsReplicaVerifyRebuild, req.ProxyEngineRequest.DataEngine, req)
	return v.(*emptypb.Empty), err
}

func replicaVerifyRebuild(ctx context.Context, req *rpc.EngineReplicaVerifyRebuildRequest) (resp *emptypb.Empty, err error) {
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

func spdkReplicaVerifyRebuild(ctx context.Context, req *rpc.EngineReplicaVerifyRebuildRequest) (resp *emptypb.Empty, err error) {
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

	v, err := executeProxyOp(ctx, ProxyOpsReplicaRemove, req.ProxyEngineRequest.DataEngine, req)
	return v.(*emptypb.Empty), err
}

func replicaRemove(ctx context.Context, req *rpc.EngineReplicaRemoveRequest) (*emptypb.Empty, error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	return nil, c.ReplicaDelete(req.ReplicaAddress)
}

func spdkReplicaRemove(ctx context.Context, req *rpc.EngineReplicaRemoveRequest, spdkServiceAddress string) (*emptypb.Empty, error) {
	c, err := spdkclient.NewSPDKClient(spdkServiceAddress)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	return nil, c.EngineReplicaDelete(req.ProxyEngineRequest.EngineName, req.ReplicaName, req.ReplicaAddress)
}

func (p *Proxy) ReplicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
	})
	log.Infof("Updating replica mode to %v", req.Mode)

	v, err := executeProxyOp(ctx, ProxyOpsReplicaModeUpdate, req.ProxyEngineRequest.DataEngine, req)
	return v.(*emptypb.Empty), err
}

func replicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *emptypb.Empty, err error) {
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

func spdkReplicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *emptypb.Empty, err error) {
	/* TODO: implement this */
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}
