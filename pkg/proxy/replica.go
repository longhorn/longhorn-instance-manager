package proxy

import (
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (p *Proxy) ReplicaAdd(ctx context.Context, req *rpc.EngineReplicaAddRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":  req.ProxyEngineRequest.Address,
		"restore":     req.Restore,
		"size":        req.Size,
		"currentSize": req.CurrentSize,
		"fastSync":    req.FastSync,
	})
	log.Debugf("Adding replica %v", req.ReplicaAddress)

	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}

	if req.Restore {
		if err := task.AddRestoreReplica(req.Size, req.CurrentSize, req.ReplicaAddress); err != nil {
			return nil, err
		}
	} else {
		if err := task.AddReplica(req.Size, req.CurrentSize, req.ReplicaAddress, req.FastSync); err != nil {
			return nil, err
		}
	}

	return &empty.Empty{}, nil
}

func (p *Proxy) ReplicaList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaListProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Debug("Listing replicas")

	c, err := eclient.NewControllerClient(req.Address)
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

func (p *Proxy) ReplicaRebuildingStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Debug("Getting replica rebuilding status")

	task, err := esync.NewTask(ctx, req.Address)
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

func (p *Proxy) ReplicaVerifyRebuild(ctx context.Context, req *rpc.EngineReplicaVerifyRebuildRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Debugf("Verifying replica %v rebuild", req.ReplicaAddress)

	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}

	err = task.VerifyRebuildReplica(req.ReplicaAddress)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (p *Proxy) ReplicaRemove(ctx context.Context, req *rpc.EngineReplicaRemoveRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Debug("Removing replica")

	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	if err = c.ReplicaDelete(req.ReplicaAddress); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (p *Proxy) ReplicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Debug("Updating replica mode")

	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	if _, err = c.ReplicaUpdate(req.ReplicaAddress, eptypes.GRPCReplicaModeToReplicaMode(req.Mode)); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}
