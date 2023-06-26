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
<<<<<<< HEAD
		"serviceURL":  req.ProxyEngineRequest.Address,
		"restore":     req.Restore,
		"size":        req.Size,
		"currentSize": req.CurrentSize,
		"fastSync":    req.FastSync,
=======
		"serviceURL":     req.ProxyEngineRequest.Address,
		"engineName":     req.ProxyEngineRequest.EngineName,
		"volumeName":     req.ProxyEngineRequest.VolumeName,
		"replicaName":    req.ReplicaName,
		"replicaAddress": req.ReplicaAddress,
		"restore":        req.Restore,
		"size":           req.Size,
		"currentSize":    req.CurrentSize,
		"fastSync":       req.FastSync,
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
	})
	log.Infof("Adding replica %v", req.ReplicaAddress)

<<<<<<< HEAD
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address)
=======
	switch req.ProxyEngineRequest.BackendStoreDriver {
	case rpc.BackendStoreDriver_v1:
		return p.replicaAdd(ctx, req)
	case rpc.BackendStoreDriver_v2:
		return p.spdkReplicaAdd(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown backend store driver %v", req.ProxyEngineRequest.BackendStoreDriver)
	}
}

func (p *Proxy) replicaAdd(ctx context.Context, req *rpc.EngineReplicaAddRequest) (resp *empty.Empty, err error) {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
	if err != nil {
		return nil, err
	}

	if req.Restore {
		if err := task.AddRestoreReplica(req.Size, req.CurrentSize, req.ReplicaAddress); err != nil {
			return nil, err
		}
	} else {
		if err := task.AddReplica(req.Size, req.CurrentSize, req.ReplicaAddress, int(req.FileSyncHttpClientTimeout), req.FastSync); err != nil {
			return nil, err
		}
	}

	return &empty.Empty{}, nil
}

func (p *Proxy) ReplicaList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaListProxyResponse, err error) {
<<<<<<< HEAD
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Trace("Listing replicas")

	c, err := eclient.NewControllerClient(req.Address)
=======
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":         req.Address,
		"engineName":         req.EngineName,
		"volumeName":         req.VolumeName,
		"backendStoreDriver": req.BackendStoreDriver,
	})
	log.Trace("Listing replicas")

	switch req.BackendStoreDriver {
	case rpc.BackendStoreDriver_v1:
		return p.replicaList(ctx, req)
	case rpc.BackendStoreDriver_v2:
		return p.spdkReplicaList(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown backend store driver %v", req.BackendStoreDriver)
	}
}

func (p *Proxy) replicaList(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaListProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
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
<<<<<<< HEAD
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Trace("Getting replica rebuilding status")

	task, err := esync.NewTask(ctx, req.Address)
=======
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":         req.Address,
		"engineName":         req.EngineName,
		"volumeName":         req.VolumeName,
		"backendStoreDriver": req.BackendStoreDriver,
	})
	log.Trace("Getting replica rebuilding status")

	switch req.BackendStoreDriver {
	case rpc.BackendStoreDriver_v1:
		return p.replicaRebuildingStatus(ctx, req)
	case rpc.BackendStoreDriver_v2:
		return p.spdkReplicaRebuildingStatus(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown backend store driver %v", req.BackendStoreDriver)
	}
}

func (p *Proxy) replicaRebuildingStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineReplicaRebuildStatusProxyResponse, err error) {
	task, err := esync.NewTask(ctx, req.Address, req.VolumeName, req.EngineName)
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
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
	log.Infof("Verifying replica %v rebuild", req.ReplicaAddress)

	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
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
<<<<<<< HEAD
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
=======
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":     req.ProxyEngineRequest.Address,
		"engineName":     req.ProxyEngineRequest.EngineName,
		"volumeName":     req.ProxyEngineRequest.VolumeName,
		"replicaName":    req.ReplicaName,
		"replicaAddress": req.ReplicaAddress,
	})
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
	log.Info("Removing replica")

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

<<<<<<< HEAD
=======
func (p *Proxy) replicaDelete(ctx context.Context, req *rpc.EngineReplicaRemoveRequest) error {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return err
	}
	defer c.Close()

	return c.ReplicaDelete(req.ReplicaAddress)
}

func (p *Proxy) spdkReplicaDelete(ctx context.Context, req *rpc.EngineReplicaRemoveRequest) error {
	c, err := spdkclient.NewSPDKClient(p.spdkServiceAddress)
	if err != nil {
		return err
	}
	defer c.Close()

	return c.EngineReplicaDelete(req.ProxyEngineRequest.EngineName, req.ReplicaName, req.ReplicaAddress)
}

>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
func (p *Proxy) ReplicaModeUpdate(ctx context.Context, req *rpc.EngineReplicaModeUpdateRequest) (resp *empty.Empty, err error) {
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

	return &empty.Empty{}, nil
}
