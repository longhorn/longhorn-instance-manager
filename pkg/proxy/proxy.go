package proxy

import (
	"net"
	"strconv"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"

	"github.com/longhorn/types/pkg/generated/enginerpc"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

type ProxyOps interface {
	VolumeGet(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineVolumeGetProxyResponse, error)
	VolumeExpand(context.Context, *rpc.EngineVolumeExpandRequest) (*emptypb.Empty, error)
	VolumeFrontendStart(context.Context, *rpc.EngineVolumeFrontendStartRequest) (*emptypb.Empty, error)
	VolumeFrontendShutdown(context.Context, *rpc.ProxyEngineRequest) (*emptypb.Empty, error)
	VolumeUnmapMarkSnapChainRemovedSet(context.Context, *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (*emptypb.Empty, error)

	ReplicaAdd(context.Context, *rpc.EngineReplicaAddRequest) (*emptypb.Empty, error)
	ReplicaList(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineReplicaListProxyResponse, error)
	ReplicaRebuildingStatus(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineReplicaRebuildStatusProxyResponse, error)
	ReplicaRemove(context.Context, *rpc.EngineReplicaRemoveRequest) (*emptypb.Empty, error)
	ReplicaVerifyRebuild(context.Context, *rpc.EngineReplicaVerifyRebuildRequest) (*emptypb.Empty, error)
	ReplicaModeUpdate(context.Context, *rpc.EngineReplicaModeUpdateRequest) (*emptypb.Empty, error)

	VolumeSnapshot(context.Context, *rpc.EngineVolumeSnapshotRequest) (*rpc.EngineVolumeSnapshotProxyResponse, error)
	SnapshotList(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineSnapshotListProxyResponse, error)
	SnapshotClone(context.Context, *rpc.EngineSnapshotCloneRequest) (*emptypb.Empty, error)
	SnapshotCloneStatus(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineSnapshotCloneStatusProxyResponse, error)
	SnapshotRevert(context.Context, *rpc.EngineSnapshotRevertRequest) (*emptypb.Empty, error)
	SnapshotPurge(context.Context, *rpc.EngineSnapshotPurgeRequest) (*emptypb.Empty, error)
	SnapshotPurgeStatus(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineSnapshotPurgeStatusProxyResponse, error)
	SnapshotRemove(context.Context, *rpc.EngineSnapshotRemoveRequest) (*emptypb.Empty, error)
	SnapshotHash(context.Context, *rpc.EngineSnapshotHashRequest) (*emptypb.Empty, error)
	SnapshotHashStatus(context.Context, *rpc.EngineSnapshotHashStatusRequest) (*rpc.EngineSnapshotHashStatusProxyResponse, error)
	VolumeSnapshotMaxCountSet(context.Context, *rpc.EngineVolumeSnapshotMaxCountSetRequest) (*emptypb.Empty, error)
	VolumeSnapshotMaxSizeSet(context.Context, *rpc.EngineVolumeSnapshotMaxSizeSetRequest) (*emptypb.Empty, error)

	SnapshotBackup(context.Context, *rpc.EngineSnapshotBackupRequest, map[string]string, []string) (*rpc.EngineSnapshotBackupProxyResponse, error)
	SnapshotBackupStatus(context.Context, *rpc.EngineSnapshotBackupStatusRequest) (*rpc.EngineSnapshotBackupStatusProxyResponse, error)
	BackupRestore(context.Context, *rpc.EngineBackupRestoreRequest, map[string]string) error
	BackupRestoreStatus(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineBackupRestoreStatusProxyResponse, error)
}

type V1DataEngineProxyOps struct{}
type V2DataEngineProxyOps struct{}

type Proxy struct {
	rpc.UnimplementedProxyEngineServiceServer
	ctx           context.Context
	logsDir       string
	HealthChecker HealthChecker
	ops           map[rpc.DataEngine]ProxyOps

	spdkServiceAddress string
	spdkLocalClient    *spdkclient.SPDKClient
}

func NewProxy(ctx context.Context, logsDir, diskServiceAddress, spdkServiceAddress string) (*Proxy, error) {

	ops := map[rpc.DataEngine]ProxyOps{
		rpc.DataEngine_DATA_ENGINE_V1: V1DataEngineProxyOps{},
		rpc.DataEngine_DATA_ENGINE_V2: V2DataEngineProxyOps{},
	}

	spdkLocalClient, err := spdkclient.NewSPDKClient(spdkServiceAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}

	p := &Proxy{
		ctx:           ctx,
		logsDir:       logsDir,
		HealthChecker: &GRPCHealthChecker{},
		ops:           ops,

		spdkServiceAddress: spdkServiceAddress,
		spdkLocalClient:    spdkLocalClient,
	}

	go p.startMonitoring()

	return p, nil
}

func (p *Proxy) startMonitoring() {
	<-p.ctx.Done()
	logrus.Infof("%s: stopped monitoring due to the context done", types.ProxyGRPCService)
}

func (p *Proxy) ServerVersionGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVersionProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Trace("Getting server version")

	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			log.WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	recv, err := c.VersionDetailGet()
	if err != nil {
		return nil, err
	}

	return &rpc.EngineVersionProxyResponse{
		Version: &enginerpc.VersionOutput{
			Version:                 recv.Version,
			GitCommit:               recv.GitCommit,
			BuildDate:               recv.BuildDate,
			CliAPIVersion:           int64(recv.CLIAPIVersion),
			CliAPIMinVersion:        int64(recv.CLIAPIMinVersion),
			ControllerAPIVersion:    int64(recv.ControllerAPIVersion),
			ControllerAPIMinVersion: int64(recv.ControllerAPIMinVersion),
			DataFormatVersion:       int64(recv.DataFormatVersion),
			DataFormatMinVersion:    int64(recv.DataFormatMinVersion),
		},
	}, nil
}

func getSPDKClientFromAddress(address string) (*spdkclient.SPDKClient, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}

	spdkServiceAddress := net.JoinHostPort(host, strconv.Itoa(types.InstanceManagerSpdkServiceDefaultPort))
	if err != nil {
		return nil, err
	}

	return spdkclient.NewSPDKClient(spdkServiceAddress)
}
