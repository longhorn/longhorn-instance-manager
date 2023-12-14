package disk

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"
	spdkrpc "github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

const (
	spdkTgtReadinessProbeTimeout = 60 * time.Second
)

type Server struct {
	sync.RWMutex

	spdkServiceAddress string
	shutdownCh         chan error
	HealthChecker      HealthChecker

	spdkClient *spdkclient.SPDKClient
}

func NewServer(spdkEnabled bool, spdkServiceAddress string, shutdownCh chan error) (*Server, error) {
	s := &Server{
		spdkServiceAddress: spdkServiceAddress,
		shutdownCh:         shutdownCh,
		HealthChecker:      &GRPCHealthChecker{},
	}

	if spdkEnabled {
		logrus.Info("Disk Server: Creating SPDK client since SPDK is enabled")

		if !util.IsSPDKTgtReady(spdkTgtReadinessProbeTimeout) {
			return nil, fmt.Errorf("spdk_tgt is not ready in %v", spdkTgtReadinessProbeTimeout)
		}

		spdkClient, err := spdkclient.NewSPDKClient(s.spdkServiceAddress)
		if err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
		}
		s.spdkClient = spdkClient
	}

	go s.startMonitoring()

	return s, nil
}

func (s *Server) startMonitoring() {
	for {
		select {
		case <-s.shutdownCh:
			logrus.Info("Disk Server: Shutting down")
			return
		}
	}
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
		"diskType":  req.DiskType,
		"diskName":  req.DiskName,
		"diskPath":  req.DiskPath,
		"blockSize": req.BlockSize,
	})

	log.Info("Disk Server: Creating disk")

	if req.DiskName == "" || req.DiskPath == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk path are required")
	}

	switch req.DiskType {
	case rpc.DiskType_block:
		ret, err := s.spdkClient.DiskCreate(req.DiskName, req.DiskUuid, req.DiskPath, req.BlockSize)
		if err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		return spdkDiskToDisk(ret), nil
	case rpc.DiskType_filesystem:
		// TODO: implement filesystem disk type
		fallthrough
	default:
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
}

func (s *Server) DiskDelete(ctx context.Context, req *rpc.DiskDeleteRequest) (*emptypb.Empty, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName": req.DiskName,
		"diskUUID": req.DiskUuid,
	})

	log.Info("Disk Server: Deleting disk")

	if req.DiskName == "" || req.DiskUuid == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk UUID are required")
	}

	switch req.DiskType {
	case rpc.DiskType_block:
		return &emptypb.Empty{}, s.spdkClient.DiskDelete(req.DiskName, req.DiskUuid)
	case rpc.DiskType_filesystem:
		// TODO: implement filesystem disk type
		fallthrough
	default:
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
}

func (s *Server) DiskGet(ctx context.Context, req *rpc.DiskGetRequest) (*rpc.Disk, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskType": req.DiskType,
		"diskName": req.DiskName,
		"diskPath": req.DiskPath,
	})

	log.Trace("Disk Server: Getting disk info")

	if req.DiskName == "" || req.DiskPath == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk path are required")
	}

	switch req.DiskType {
	case rpc.DiskType_block:
		ret, err := s.spdkClient.DiskGet(req.DiskName)
		if err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		return spdkDiskToDisk(ret), nil
	case rpc.DiskType_filesystem:
		// TODO: implement filesystem disk type
		fallthrough
	default:
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
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

	switch req.DiskType {
	case rpc.DiskType_block:
		replicas, err := s.spdkClient.ReplicaList()
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
	case rpc.DiskType_filesystem:
		// TODO: implement filesystem disk type
		fallthrough
	default:
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
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

	switch req.DiskType {
	case rpc.DiskType_block:
		err := s.spdkClient.ReplicaDelete(req.ReplciaInstanceName, true)
		if err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		return &emptypb.Empty{}, nil
	case rpc.DiskType_filesystem:
		// TODO: implement filesystem disk type
		fallthrough
	default:
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported disk type %v", req.DiskType)
	}
}

func spdkDiskToDisk(disk *spdkrpc.Disk) *rpc.Disk {
	return &rpc.Disk{
		Id:          disk.Id,
		Uuid:        disk.Uuid,
		Path:        disk.Path,
		Type:        disk.Type,
		TotalSize:   disk.TotalSize,
		FreeSize:    disk.FreeSize,
		TotalBlocks: disk.TotalBlocks,
		FreeBlocks:  disk.FreeBlocks,
		BlockSize:   disk.BlockSize,
		ClusterSize: disk.ClusterSize,
	}
}

func replicaToReplicaInstance(r *api.Replica) *rpc.ReplicaInstance {
	return &rpc.ReplicaInstance{
		Name:       r.Name,
		Uuid:       r.UUID,
		DiskName:   r.LvsName,
		DiskUuid:   r.LvsUUID,
		SpecSize:   r.SpecSize,
		ActualSize: r.ActualSize,
	}
}
