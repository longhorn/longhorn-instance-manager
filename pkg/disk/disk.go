package disk

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"
	enginerpc "github.com/longhorn/types/pkg/generated/enginerpc"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
	spdkrpc "github.com/longhorn/types/pkg/generated/spdkrpc"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

const (
	spdkTgtReadinessProbeTimeout = 60 * time.Second
)

type DiskOps interface {
	DiskCreate(context.Context, *rpc.DiskCreateRequest) (*rpc.Disk, error)
	DiskDelete(*rpc.DiskDeleteRequest) (*emptypb.Empty, error)
	DiskGet(req *rpc.DiskGetRequest) (*rpc.Disk, error)
	DiskReplicaInstanceList(*rpc.DiskReplicaInstanceListRequest) (*rpc.DiskReplicaInstanceListResponse, error)
	DiskReplicaInstanceDelete(*rpc.DiskReplicaInstanceDeleteRequest) (*emptypb.Empty, error)
	MetricsGet(*rpc.DiskGetRequest) (*rpc.DiskMetricsGetReply, error)
}

type FilesystemDiskOps struct{}
type BlockDiskOps struct {
	spdkClient *spdkclient.SPDKClient
}

type Server struct {
	rpc.UnimplementedDiskServiceServer
	sync.RWMutex

	ctx           context.Context
	HealthChecker HealthChecker

	spdkServiceAddress string
	ops                map[rpc.DiskType]DiskOps
}

func NewServer(ctx context.Context, spdkEnabled bool, spdkServiceAddress string) (srv *Server, err error) {
	var spdkClient *spdkclient.SPDKClient

	if spdkEnabled {
		logrus.Info("Disk Server: Creating SPDK client since SPDK is enabled")

		if !util.IsSPDKTgtReady(spdkTgtReadinessProbeTimeout) {
			return nil, fmt.Errorf("spdk_tgt is not ready in %v", spdkTgtReadinessProbeTimeout)
		}

		spdkClient, err = spdkclient.NewSPDKClient(spdkServiceAddress)
		if err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
		}
	}

	ops := map[rpc.DiskType]DiskOps{
		rpc.DiskType_filesystem: FilesystemDiskOps{},
		rpc.DiskType_block: BlockDiskOps{
			spdkClient: spdkClient,
		},
	}

	s := &Server{
		ctx:                ctx,
		spdkServiceAddress: spdkServiceAddress,
		HealthChecker:      &GRPCHealthChecker{},
		ops:                ops,
	}

	go s.startMonitoring()

	return s, nil
}

func (s *Server) startMonitoring() {
	<-s.ctx.Done()
	logrus.Infof("%s: stopped monitoring due to the context done", types.DiskGrpcService)
}

func (s *Server) VersionGet(ctx context.Context, req *emptypb.Empty) (*rpc.DiskVersionResponse, error) {
	v := meta.GetDiskServiceVersion()
	return &rpc.DiskVersionResponse{
		Version:   v.Version,
		GitCommit: v.GitCommit,
		BuildDate: v.BuildDate,

		InstanceManagerDiskServiceAPIVersion:    int64(v.InstanceManagerDiskServiceAPIVersion),
		InstanceManagerDiskServiceAPIMinVersion: int64(v.InstanceManagerDiskServiceAPIMinVersion),
	}, nil
}

func (s *Server) DiskCreate(ctx context.Context, req *rpc.DiskCreateRequest) (*rpc.Disk, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskType":   req.DiskType,
		"diskName":   req.DiskName,
		"diskPath":   req.DiskPath,
		"blockSize":  req.BlockSize,
		"diskDriver": req.DiskDriver,
	})

	log.Info("Disk Server: Creating disk")

	if req.DiskName == "" || req.DiskPath == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk path are required")
	}

	ops, ok := s.ops[req.DiskType]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
	return ops.DiskCreate(ctx, req)
}

func (ops FilesystemDiskOps) DiskCreate(ctx context.Context, req *rpc.DiskCreateRequest) (*rpc.Disk, error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
}

func (ops BlockDiskOps) DiskCreate(ctx context.Context, req *rpc.DiskCreateRequest) (*rpc.Disk, error) {
	ret, err := ops.spdkClient.DiskCreate(req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver, req.BlockSize)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	return spdkDiskToDisk(ret), nil
}

func (s *Server) DiskDelete(ctx context.Context, req *rpc.DiskDeleteRequest) (*emptypb.Empty, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskType":   req.DiskType,
		"diskName":   req.DiskName,
		"diskUUID":   req.DiskUuid,
		"diskPath":   req.DiskPath,
		"diskDriver": req.DiskDriver,
	})

	log.Info("Disk Server: Deleting disk")

	if req.DiskName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	ops, ok := s.ops[req.DiskType]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
	return ops.DiskDelete(req)
}

func (ops FilesystemDiskOps) DiskDelete(req *rpc.DiskDeleteRequest) (*emptypb.Empty, error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
}

func (ops BlockDiskOps) DiskDelete(req *rpc.DiskDeleteRequest) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, ops.spdkClient.DiskDelete(req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver)
}

func (s *Server) DiskGet(ctx context.Context, req *rpc.DiskGetRequest) (*rpc.Disk, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskType": req.DiskType,
		"diskName": req.DiskName,
		"diskPath": req.DiskPath,
	})

	log.Trace("Disk Server: Getting disk info")

	if req.DiskName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	ops, ok := s.ops[req.DiskType]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
	return ops.DiskGet(req)
}

func (ops FilesystemDiskOps) DiskGet(req *rpc.DiskGetRequest) (*rpc.Disk, error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
}

func (ops BlockDiskOps) DiskGet(req *rpc.DiskGetRequest) (*rpc.Disk, error) {
	ret, err := ops.spdkClient.DiskGet(req.DiskName, req.DiskPath, req.DiskDriver)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	return spdkDiskToDisk(ret), nil
}

func (s *Server) DiskReplicaInstanceList(ctx context.Context, req *rpc.DiskReplicaInstanceListRequest) (*rpc.DiskReplicaInstanceListResponse, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskType": req.DiskType,
		"diskName": req.DiskName,
	})

	log.Trace("Disk Server: Listing disk replica instances")

	if req.DiskName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	ops, ok := s.ops[req.DiskType]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
	return ops.DiskReplicaInstanceList(req)
}

func (ops FilesystemDiskOps) DiskReplicaInstanceList(req *rpc.DiskReplicaInstanceListRequest) (*rpc.DiskReplicaInstanceListResponse, error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
}

func (ops BlockDiskOps) DiskReplicaInstanceList(req *rpc.DiskReplicaInstanceListRequest) (*rpc.DiskReplicaInstanceListResponse, error) {
	replicas, err := ops.spdkClient.ReplicaList()
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	instances := map[string]*rpc.ReplicaInstance{}
	for name, replica := range replicas {
		instances[name] = replicaToReplicaInstance(replica)
	}
	return &rpc.DiskReplicaInstanceListResponse{
		ReplicaInstances: instances,
	}, nil
}

func (s *Server) DiskReplicaInstanceDelete(ctx context.Context, req *rpc.DiskReplicaInstanceDeleteRequest) (*emptypb.Empty, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskType":            req.DiskType,
		"diskName":            req.DiskName,
		"diskUUID":            req.DiskUuid,
		"replciaInstanceName": req.ReplciaInstanceName,
	})

	log.Info("Disk Server: Deleting disk replica instance")

	if req.DiskName == "" || req.DiskUuid == "" || req.ReplciaInstanceName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name, disk UUID and replica instance name are required")
	}

	ops, ok := s.ops[req.DiskType]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
	return ops.DiskReplicaInstanceDelete(req)
}

func (ops FilesystemDiskOps) DiskReplicaInstanceDelete(req *rpc.DiskReplicaInstanceDeleteRequest) (*emptypb.Empty, error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
}

func (ops BlockDiskOps) DiskReplicaInstanceDelete(req *rpc.DiskReplicaInstanceDeleteRequest) (*emptypb.Empty, error) {
	err := ops.spdkClient.ReplicaDelete(req.ReplciaInstanceName, true)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) MetricsGet(ctx context.Context, req *rpc.DiskGetRequest) (*rpc.DiskMetricsGetReply, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskType": req.DiskType,
		"diskName": req.DiskName,
		"diskPath": req.DiskPath,
	})

	log.Trace("Disk Server: Getting disk metrics")

	if req.DiskName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	ops, ok := s.ops[req.DiskType]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
	return ops.MetricsGet(req)
}

func (ops FilesystemDiskOps) MetricsGet(req *rpc.DiskGetRequest) (*rpc.DiskMetricsGetReply, error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
}

func (ops BlockDiskOps) MetricsGet(req *rpc.DiskGetRequest) (*rpc.DiskMetricsGetReply, error) {
	metrics, err := ops.spdkClient.MetricsGet(req.DiskName)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}

	// Convert SPDK metrics to ptypes.Metrics format
	return &rpc.DiskMetricsGetReply{
		Metrics: &enginerpc.Metrics{
			ReadThroughput:  metrics.ReadThroughput,
			WriteThroughput: metrics.WriteThroughput,
			ReadLatency:     metrics.ReadLatency,
			WriteLatency:    metrics.WriteLatency,
			ReadIOPS:        metrics.ReadIOPS,
			WriteIOPS:       metrics.WriteIOPS,
		},
	}, nil
}

func spdkDiskToDisk(disk *spdkrpc.Disk) *rpc.Disk {
	return &rpc.Disk{
		Id:          disk.Id,
		Name:        disk.Name,
		Uuid:        disk.Uuid,
		Path:        disk.Path,
		Type:        disk.Type,
		Driver:      disk.Driver,
		TotalSize:   disk.TotalSize,
		FreeSize:    disk.FreeSize,
		TotalBlocks: disk.TotalBlocks,
		FreeBlocks:  disk.FreeBlocks,
		BlockSize:   disk.BlockSize,
		ClusterSize: disk.ClusterSize,
		State:       disk.State,
	}
}

func replicaToReplicaInstance(r *api.Replica) *rpc.ReplicaInstance {
	return &rpc.ReplicaInstance{
		Name:       r.Name,
		DiskName:   r.LvsName,
		DiskUuid:   r.LvsUUID,
		SpecSize:   r.SpecSize,
		ActualSize: r.ActualSize,
	}
}
