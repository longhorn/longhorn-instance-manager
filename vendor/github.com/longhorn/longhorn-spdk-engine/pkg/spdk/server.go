package spdk

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util/broadcaster"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

const (
	MonitorInterval = 3 * time.Second
)

type Server struct {
	sync.RWMutex

	ctx context.Context

	spdkClient    *spdkclient.Client
	portAllocator *util.Bitmap

	replicaMap map[string]*Replica
	engineMap  map[string]*Engine

	broadcasters map[types.InstanceType]*broadcaster.Broadcaster
	broadcastChs map[types.InstanceType]chan interface{}
	updateChs    map[types.InstanceType]chan interface{}
}

func NewServer(ctx context.Context, portStart, portEnd int32) (*Server, error) {
	cli, err := spdkclient.NewClient()
	if err != nil {
		return nil, err
	}

	broadcasters := map[types.InstanceType]*broadcaster.Broadcaster{}
	broadcastChs := map[types.InstanceType]chan interface{}{}
	updateChs := map[types.InstanceType]chan interface{}{}
	for _, t := range []types.InstanceType{types.InstanceTypeReplica, types.InstanceTypeEngine} {
		broadcasters[t] = &broadcaster.Broadcaster{}
		broadcastChs[t] = make(chan interface{})
		updateChs[t] = make(chan interface{})
	}

	s := &Server{
		ctx: ctx,

		spdkClient:    cli,
		portAllocator: util.NewBitmap(portStart, portEnd),

		replicaMap: map[string]*Replica{},
		engineMap:  map[string]*Engine{},

		broadcasters: broadcasters,
		broadcastChs: broadcastChs,
		updateChs:    updateChs,
	}

	if _, err := s.broadcasters[types.InstanceTypeReplica].Subscribe(ctx, s.replicaBroadcastConnector); err != nil {
		return nil, err
	}
	if _, err := s.broadcasters[types.InstanceTypeEngine].Subscribe(ctx, s.engineBroadcastConnector); err != nil {
		return nil, err
	}

	// TODO: There is no need to maintain the replica map in cache when we can use one SPDK JSON API call to fetch the Lvol tree/chain info
	go s.monitoring()
	go s.broadcasting()

	return s, nil
}

func (s *Server) monitoring() {
	ticker := time.NewTicker(MonitorInterval)
	defer ticker.Stop()

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("SPDK Server: stopped monitoring replicas due to the context done")
			done = true
		case <-ticker.C:
			if err := s.verify(); err != nil {
				logrus.WithError(err).Errorf("SPDK Server: failed to verify and update replica cache, will retry later")
			}
		}
		if done {
			break
		}
	}
}

func (s *Server) verify() error {
	s.Lock()
	defer s.Unlock()

	lvsList, err := s.spdkClient.BdevLvolGetLvstore("", "")
	if err != nil {
		return err
	}
	bdevList, err := s.spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		return err
	}
	subsystemList, err := s.spdkClient.NvmfGetSubsystems("", "")
	if err != nil {
		return err
	}

	lvsUUIDNameMap := map[string]string{}
	for _, lvs := range lvsList {
		lvsUUIDNameMap[lvs.UUID] = lvs.Name
	}

	subsystemMap := map[string]*spdktypes.NvmfSubsystem{}
	for idx := range subsystemList {
		subsystem := &subsystemList[idx]
		subsystemMap[subsystem.Nqn] = subsystem
	}

	bdevMap := map[string]*spdktypes.BdevInfo{}
	bdevLvolMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevList {
		bdev := &bdevList[idx]
		bdevType := spdktypes.GetBdevType(bdev)

		switch bdevType {
		case spdktypes.BdevTypeLvol:
			if len(bdev.Aliases) != 1 {
				continue
			}
			lvolName := spdktypes.GetLvolNameFromAlias(bdev.Aliases[0])
			bdevMap[bdev.Aliases[0]] = bdev
			bdevLvolMap[lvolName] = bdev

			// Detect if the lvol bdev is an uncached replica.
			if bdev.DriverSpecific.Lvol.Snapshot {
				continue
			}
			if s.replicaMap[lvolName] != nil {
				continue
			}
			lvsUUID := bdev.DriverSpecific.Lvol.LvolStoreUUID
			specSize := bdev.NumBlocks * uint64(bdev.BlockSize)
			// TODO: May need to cache Disks
			s.replicaMap[lvolName] = NewReplica(lvolName, lvsUUIDNameMap[lvsUUID], lvsUUID, specSize, s.updateChs[types.InstanceTypeReplica])
		case spdktypes.BdevTypeRaid:
			// Cannot detect if a RAID bdev is an engine since:
			//   1. we don't know the frontend
			//   2. RAID bdevs are not persist objects in SPDK. After spdk_tgt start/restart, there is no RAID bdev henc there is no need to do detection.
			fallthrough
		default:
			bdevMap[bdev.Name] = bdev
		}
	}

	for _, r := range s.replicaMap {
		r.Sync(bdevLvolMap, subsystemMap)
	}

	for _, e := range s.engineMap {
		e.ValidateAndUpdate(bdevMap, subsystemMap)
	}

	// TODO: send update signals if there is a Replica/Replica change

	return nil
}

func (s *Server) broadcasting() {
	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("SPDK Server: stopped broadcasting instances due to the context done")
			done = true
		case <-s.updateChs[types.InstanceTypeReplica]:
			s.broadcastChs[types.InstanceTypeReplica] <- nil
		case <-s.updateChs[types.InstanceTypeEngine]:
			s.broadcastChs[types.InstanceTypeEngine] <- nil
		}
		if done {
			break
		}
	}
}

func (s *Server) Subscribe(instanceType types.InstanceType) (<-chan interface{}, error) {
	switch instanceType {
	case types.InstanceTypeEngine:
		return s.broadcasters[types.InstanceTypeEngine].Subscribe(context.TODO(), s.engineBroadcastConnector)
	case types.InstanceTypeReplica:
		return s.broadcasters[types.InstanceTypeReplica].Subscribe(context.TODO(), s.replicaBroadcastConnector)
	}
	return nil, fmt.Errorf("invalid instance type %v for subscription", instanceType)
}

func (s *Server) replicaBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeReplica], nil
}

func (s *Server) engineBroadcastConnector() (chan interface{}, error) {
	return s.broadcastChs[types.InstanceTypeEngine], nil
}

func (s *Server) ReplicaCreate(ctx context.Context, req *spdkrpc.ReplicaCreateRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required")
	}
	if req.LvsName == "" && req.LvsUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "lvs name or lvs UUID are required")
	}

	s.Lock()
	defer s.Unlock()

	if _, ok := s.replicaMap[req.Name]; !ok {
		s.replicaMap[req.Name] = NewReplica(req.Name, req.LvsName, req.LvsUuid, req.SpecSize, s.updateChs[types.InstanceTypeReplica])
	}
	r := s.replicaMap[req.Name]

	return r.Create(s.spdkClient, req.ExposeRequired, s.portAllocator)
}

func (s *Server) ReplicaDelete(ctx context.Context, req *spdkrpc.ReplicaDeleteRequest) (ret *empty.Empty, err error) {
	s.Lock()
	defer s.Unlock()

	r := s.replicaMap[req.Name]
	defer func() {
		if err == nil && req.CleanupRequired {
			delete(s.replicaMap, req.Name)
		}
	}()

	if r != nil {
		if err := r.Delete(s.spdkClient, req.CleanupRequired, s.portAllocator); err != nil {
			return nil, err
		}
	}

	return &empty.Empty{}, nil
}

func (s *Server) ReplicaGet(ctx context.Context, req *spdkrpc.ReplicaGetRequest) (ret *spdkrpc.Replica, err error) {
	s.RLock()
	r := s.replicaMap[req.Name]
	s.RUnlock()

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %v", req.Name)
	}

	return r.Get(), nil
}

func (s *Server) ReplicaList(ctx context.Context, req *empty.Empty) (*spdkrpc.ReplicaListResponse, error) {
	res := map[string]*spdkrpc.Replica{}

	s.RLock()
	for replicaName, r := range s.replicaMap {
		res[replicaName] = r.Get()
	}
	s.RUnlock()

	return &spdkrpc.ReplicaListResponse{Replicas: res}, nil
}

func (s *Server) ReplicaWatch(req *empty.Empty, srv spdkrpc.SPDKService_ReplicaWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeReplica)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service replica watch errored out")
		} else {
			logrus.Info("SPDK service replica watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("SPDK Server: stopped replica watch due to the context done")
			done = true
		case <-responseCh:
			if err := srv.Send(&empty.Empty{}); err != nil {
				return err
			}
		}
		if done {
			break
		}
	}

	return nil
}

func (s *Server) ReplicaSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Replica, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.Lock()
	defer s.Unlock()

	r := s.replicaMap[req.Name]

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot create", req.Name)
	}

	return r.SnapshotCreate(s.spdkClient, req.Name)
}

func (s *Server) ReplicaSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *empty.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name and snapshot name are required")
	}

	s.Lock()
	defer s.Unlock()

	r := s.replicaMap[req.Name]

	if r == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica %s during snapshot delete", req.Name)
	}

	_, err = r.SnapshotDelete(s.spdkClient, req.Name)
	return &empty.Empty{}, err
}

func (s *Server) EngineCreate(ctx context.Context, req *spdkrpc.EngineCreateRequest) (ret *spdkrpc.Engine, err error) {
	if req.Name == "" || req.VolumeName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and volume name are required")
	}
	if req.SpecSize == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine spec size is required")
	}
	if req.Frontend != types.FrontendSPDKTCPBlockdev && req.Frontend != types.FrontendSPDKTCPNvmf && req.Frontend != types.FrontendSPDKVoid {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine frontend is required")
	}

	s.Lock()
	defer s.Unlock()

	if _, ok := s.engineMap[req.Name]; ok {
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "engine %v already exists", req.Name)
	}

	s.engineMap[req.Name] = NewEngine(req.Name, req.VolumeName, req.Frontend, req.SpecSize, s.updateChs[types.InstanceTypeEngine])
	e := s.engineMap[req.Name]

	return e.Create(s.spdkClient, req.ReplicaAddressMap, s.getLocalReplicaBdevMap(req.ReplicaAddressMap), s.portAllocator)
}

func (s *Server) getLocalReplicaBdevMap(replicaAddressMap map[string]string) (replicaBdevMap map[string]string) {
	replicaBdevMap = map[string]string{}
	for replicaName := range replicaAddressMap {
		r := s.replicaMap[replicaName]
		if r == nil {
			continue
		}
		// For a lvol bdev, the name is actually UUID, but we use the alias here.
		replicaBdevMap[replicaName] = spdktypes.GetLvolAlias(r.LvsName, r.Name)
	}

	return replicaBdevMap
}

func (s *Server) EngineDelete(ctx context.Context, req *spdkrpc.EngineDeleteRequest) (ret *empty.Empty, err error) {
	s.Lock()
	defer s.Unlock()

	e := s.engineMap[req.Name]
	defer func() {
		if err == nil {
			delete(s.engineMap, req.Name)
		}
	}()

	if e != nil {
		if err := e.Delete(s.spdkClient, s.portAllocator); err != nil {
			return nil, err
		}
	}

	return &empty.Empty{}, nil
}

func (s *Server) EngineGet(ctx context.Context, req *spdkrpc.EngineGetRequest) (ret *spdkrpc.Engine, err error) {
	s.RLock()
	e := s.engineMap[req.Name]
	s.RUnlock()

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v", req.Name)
	}

	return e.Get(), nil
}

func (s *Server) EngineList(ctx context.Context, req *empty.Empty) (*spdkrpc.EngineListResponse, error) {
	res := map[string]*spdkrpc.Engine{}

	s.RLock()
	for engineName, e := range s.engineMap {
		res[engineName] = e.Get()
	}
	s.RUnlock()

	return &spdkrpc.EngineListResponse{Engines: res}, nil
}

func (s *Server) EngineWatch(req *empty.Empty, srv spdkrpc.SPDKService_EngineWatchServer) error {
	responseCh, err := s.Subscribe(types.InstanceTypeEngine)
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			logrus.WithError(err).Error("SPDK service engine watch errored out")
		} else {
			logrus.Info("SPDK service engine watch ended successfully")
		}
	}()
	logrus.Info("Started new SPDK service update watch")

	done := false
	for {
		select {
		case <-s.ctx.Done():
			logrus.Info("SPDK Server: stopped engine watch due to the context done")
			done = true
		case <-responseCh:
			if err := srv.Send(&empty.Empty{}); err != nil {
				return err
			}
		}
		if done {
			break
		}
	}

	return nil
}

func (s *Server) EngineReplicaDelete(ctx context.Context, req *spdkrpc.EngineReplicaDeleteRequest) (ret *empty.Empty, err error) {
	s.Lock()
	defer s.Unlock()

	e := s.engineMap[req.EngineName]
	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for replica %s with address %s delete", req.EngineName, req.ReplicaName, req.ReplicaAddress)
	}

	if err := e.ReplicaDelete(s.spdkClient, req.ReplicaName, req.ReplicaAddress); err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (s *Server) EngineSnapshotCreate(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *spdkrpc.Engine, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	defer s.RUnlock()

	e := s.engineMap[req.Name]

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot creation", req.Name)
	}

	return e.SnapshotCreate(s.spdkClient, req.Name, req.SnapshotName)
}

func (s *Server) EngineSnapshotDelete(ctx context.Context, req *spdkrpc.SnapshotRequest) (ret *empty.Empty, err error) {
	if req.Name == "" || req.SnapshotName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "engine name and snapshot name are required")
	}

	s.RLock()
	defer s.RUnlock()

	e := s.engineMap[req.Name]

	if e == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find engine %v for snapshot deletion", req.Name)
	}

	return e.SnapshotDelete(s.spdkClient, req.Name, req.SnapshotName)
}

func (s *Server) DiskCreate(ctx context.Context, req *spdkrpc.DiskCreateRequest) (ret *spdkrpc.Disk, err error) {
	s.Lock()
	defer s.Unlock()
	return svcDiskCreate(s.spdkClient, req.DiskName, req.DiskPath, req.BlockSize)
}

func (s *Server) DiskDelete(ctx context.Context, req *spdkrpc.DiskDeleteRequest) (ret *emptypb.Empty, err error) {
	s.Lock()
	defer s.Unlock()
	return svcDiskDelete(s.spdkClient, req.DiskName, req.DiskUuid)
}

func (s *Server) DiskGet(ctx context.Context, req *spdkrpc.DiskGetRequest) (ret *spdkrpc.Disk, err error) {
	s.Lock()
	defer s.Unlock()
	return svcDiskGet(s.spdkClient, req.DiskName)
}

func (s *Server) VersionDetailGet(context.Context, *empty.Empty) (*spdkrpc.VersionDetailGetReply, error) {
	// TODO: Implement this
	return &spdkrpc.VersionDetailGetReply{
		Version: &spdkrpc.VersionOutput{},
	}, nil
}
