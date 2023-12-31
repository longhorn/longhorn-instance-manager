package proxy

import (
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
)

type ProxyOps int

const (
	// Volume operations
	ProxyOpsVolumeGet ProxyOps = iota
	ProxyOpsVolumeExpand
	ProxyOpsVolumeFrontendStart
	ProxyOpsVolumeFrontendShutdown
	ProxyOpsVolumeUnmapMarkSnapChainRemovedSet

	// Replica operations
	ProxyOpsReplicaAdd
	ProxyOpsReplicaList
	ProxyOpsReplicaRebuildingStatus
	ProxyOpsReplicaRemove
	ProxyOpsReplicaVerifyRebuild
	ProxyOpsReplicaModeUpdate

	// Snapshot operations
	ProxyOpsVolumeSnapshot
	ProxyOpsSnapshotList
	ProxyOpsSnapshotClone
	ProxyOpsSnapshotCloneStatus
	ProxyOpsSnapshotRevert
	ProxyOpsSnapshotPurge
	ProxyOpsSnapshotPurgeStatus
	ProxyOpsSnapshotRemove
	ProxyOpsSnapshotHash
	ProxyOpsSnapshotHashStatus

	// Backup operations
	ProxyOpsSnapshotBackup
	ProxyOpsSnapshotBackupStatus
	ProxyOpsBackupRestore
	ProxyOpsBackupRestoreStatus
)

// Type definitions for the functions that execute the operations
type volumeGetFunc func(context.Context, *rpc.ProxyEngineRequest, ...string) (*rpc.EngineVolumeGetProxyResponse, error)
type volumeExpandFunc func(context.Context, *rpc.EngineVolumeExpandRequest) (*emptypb.Empty, error)
type volumeFrontendStartFunc func(context.Context, *rpc.EngineVolumeFrontendStartRequest) (*emptypb.Empty, error)
type volumeFrontendShutdownFunc func(context.Context, *rpc.ProxyEngineRequest) (*emptypb.Empty, error)
type volumeUnmapMarkSnapChainRemovedSetFunc func(context.Context, *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (*emptypb.Empty, error)

type replicaAddFunc func(context.Context, *rpc.EngineReplicaAddRequest, ...string) (*emptypb.Empty, error)
type replicaListFunc func(context.Context, *rpc.ProxyEngineRequest, ...string) (*rpc.EngineReplicaListProxyResponse, error)
type replicaRebuildingStatusFunc func(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineReplicaRebuildStatusProxyResponse, error)
type replicaRemoveFunc func(context.Context, *rpc.EngineReplicaRemoveRequest, ...string) (*emptypb.Empty, error)
type replicaVerifyRebuildFunc func(context.Context, *rpc.EngineReplicaVerifyRebuildRequest) (*emptypb.Empty, error)
type replicaModeUpdateFunc func(context.Context, *rpc.EngineReplicaModeUpdateRequest) (*emptypb.Empty, error)

type volumeSnapshotFunc func(context.Context, *rpc.EngineVolumeSnapshotRequest) (*rpc.EngineVolumeSnapshotProxyResponse, error)
type snapshotListFunc func(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineSnapshotListProxyResponse, error)
type snapshotCloneFunc func(context.Context, *rpc.EngineSnapshotCloneRequest) (*emptypb.Empty, error)
type snapshotCloneStatusFunc func(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineSnapshotCloneStatusProxyResponse, error)
type snapshotRevertFunc func(context.Context, *rpc.EngineSnapshotRevertRequest) (*emptypb.Empty, error)
type snapshotPurgeFunc func(context.Context, *rpc.EngineSnapshotPurgeRequest) (*emptypb.Empty, error)
type snapshotPurgeStatusFunc func(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineSnapshotPurgeStatusProxyResponse, error)
type snapshotRemoveFunc func(context.Context, *rpc.EngineSnapshotRemoveRequest) (*emptypb.Empty, error)
type snapshotHashFunc func(context.Context, *rpc.EngineSnapshotHashRequest) (*emptypb.Empty, error)
type snapshotHashStatusFunc func(context.Context, *rpc.EngineSnapshotHashStatusRequest) (*rpc.EngineSnapshotHashStatusProxyResponse, error)

type snapshotBackupFunc func(context.Context, *rpc.EngineSnapshotBackupRequest) (*rpc.EngineSnapshotBackupProxyResponse, error)
type snapshotBackupStatusFunc func(context.Context, *rpc.EngineSnapshotBackupStatusRequest) (*rpc.EngineSnapshotBackupStatusProxyResponse, error)
type backupRestoreFunc func(context.Context, *rpc.EngineBackupRestoreRequest) (*rpc.EngineBackupRestoreProxyResponse, error)
type backupRestoreStatusFunc func(context.Context, *rpc.ProxyEngineRequest) (*rpc.EngineBackupRestoreStatusProxyResponse, error)

// ProxyOpsFuncs is a map of ProxyOps to a map of DataEngine to the function that executes the operation
var (
	ProxyOpsFuncs = map[ProxyOps]map[rpc.DataEngine]interface{}{
		// Volume operations
		ProxyOpsVolumeGet: {
			rpc.DataEngine_DATA_ENGINE_V1: volumeGet,
			rpc.DataEngine_DATA_ENGINE_V2: spdkVolumeGet,
		},
		ProxyOpsVolumeExpand: {
			rpc.DataEngine_DATA_ENGINE_V1: volumeExpand,
			rpc.DataEngine_DATA_ENGINE_V2: spdkVolumeExpand,
		},
		ProxyOpsVolumeFrontendStart: {
			rpc.DataEngine_DATA_ENGINE_V1: volumeFrontendStart,
			rpc.DataEngine_DATA_ENGINE_V2: spdkVolumeFrontendStart,
		},
		ProxyOpsVolumeFrontendShutdown: {
			rpc.DataEngine_DATA_ENGINE_V1: volumeFrontendShutdown,
			rpc.DataEngine_DATA_ENGINE_V2: spdkVolumeFrontendShutdown,
		},
		ProxyOpsVolumeUnmapMarkSnapChainRemovedSet: {
			rpc.DataEngine_DATA_ENGINE_V1: volumeUnmapMarkSnapChainRemovedSet,
			rpc.DataEngine_DATA_ENGINE_V2: spdkVolumeUnmapMarkSnapChainRemovedSet,
		},

		// Replica operations
		ProxyOpsReplicaAdd: {
			rpc.DataEngine_DATA_ENGINE_V1: replicaAdd,
			rpc.DataEngine_DATA_ENGINE_V2: spdkReplicaAdd,
		},
		ProxyOpsReplicaList: {
			rpc.DataEngine_DATA_ENGINE_V1: replicaList,
			rpc.DataEngine_DATA_ENGINE_V2: spdkReplicaList,
		},
		ProxyOpsReplicaRebuildingStatus: {
			rpc.DataEngine_DATA_ENGINE_V1: replicaRebuildingStatus,
			rpc.DataEngine_DATA_ENGINE_V2: spdkReplicaRebuildingStatus,
		},
		ProxyOpsReplicaRemove: {
			rpc.DataEngine_DATA_ENGINE_V1: replicaRemove,
			rpc.DataEngine_DATA_ENGINE_V2: spdkReplicaRemove,
		},
		ProxyOpsReplicaVerifyRebuild: {
			rpc.DataEngine_DATA_ENGINE_V1: replicaVerifyRebuild,
			rpc.DataEngine_DATA_ENGINE_V2: spdkReplicaVerifyRebuild,
		},
		ProxyOpsReplicaModeUpdate: {
			rpc.DataEngine_DATA_ENGINE_V1: replicaModeUpdate,
			rpc.DataEngine_DATA_ENGINE_V2: spdkReplicaModeUpdate,
		},

		// Snapshot operations
		ProxyOpsVolumeSnapshot: {
			rpc.DataEngine_DATA_ENGINE_V1: volumeSnapshot,
			rpc.DataEngine_DATA_ENGINE_V2: spdkVolumeSnapshot,
		},
		ProxyOpsSnapshotList: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotList,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotList,
		},
		ProxyOpsSnapshotClone: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotClone,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotClone,
		},
		ProxyOpsSnapshotCloneStatus: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotCloneStatus,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotCloneStatus,
		},
		ProxyOpsSnapshotRevert: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotRevert,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotRevert,
		},
		ProxyOpsSnapshotPurge: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotPurge,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotPurge,
		},
		ProxyOpsSnapshotPurgeStatus: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotPurgeStatus,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotPurgeStatus,
		},
		ProxyOpsSnapshotRemove: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotRemove,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotRemove,
		},
		ProxyOpsSnapshotHash: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotHash,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotHash,
		},
		ProxyOpsSnapshotHashStatus: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotHashStatus,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotHashStatus,
		},

		// Backup operations
		ProxyOpsSnapshotBackup: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotBackup,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotBackup,
		},
		ProxyOpsSnapshotBackupStatus: {
			rpc.DataEngine_DATA_ENGINE_V1: snapshotBackupStatus,
			rpc.DataEngine_DATA_ENGINE_V2: spdkSnapshotBackupStatus,
		},
		ProxyOpsBackupRestore: {
			rpc.DataEngine_DATA_ENGINE_V1: backupRestore,
			rpc.DataEngine_DATA_ENGINE_V2: spdkBackupRestore,
		},
		ProxyOpsBackupRestoreStatus: {
			rpc.DataEngine_DATA_ENGINE_V1: backupRestoreStatus,
			rpc.DataEngine_DATA_ENGINE_V2: spdkBackupRestoreStatus,
		},
	}
)

func executeProxyOp(ctx context.Context, op ProxyOps, dataEngine rpc.DataEngine, req interface{}) (interface{}, error) {
	opFuncs, ok := ProxyOpsFuncs[op]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown proxy operation %v", op)
	}

	fn, ok := opFuncs[dataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", dataEngine)
	}

	switch op {
	// Volume operations
	case ProxyOpsVolumeGet:
		return fn.(volumeGetFunc)(ctx, req.(*rpc.ProxyEngineRequest))
	case ProxyOpsVolumeExpand:
		return fn.(volumeExpandFunc)(ctx, req.(*rpc.EngineVolumeExpandRequest))
	case ProxyOpsVolumeFrontendStart:
		return fn.(volumeFrontendStartFunc)(ctx, req.(*rpc.EngineVolumeFrontendStartRequest))
	case ProxyOpsVolumeFrontendShutdown:
		return fn.(volumeFrontendShutdownFunc)(ctx, req.(*rpc.ProxyEngineRequest))
	case ProxyOpsVolumeUnmapMarkSnapChainRemovedSet:
		return fn.(volumeUnmapMarkSnapChainRemovedSetFunc)(ctx, req.(*rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest))
	// Replica operations
	case ProxyOpsReplicaAdd:
		return fn.(replicaAddFunc)(ctx, req.(*rpc.EngineReplicaAddRequest))
	case ProxyOpsReplicaList:
		return fn.(replicaListFunc)(ctx, req.(*rpc.ProxyEngineRequest))
	case ProxyOpsReplicaRebuildingStatus:
		return fn.(replicaRebuildingStatusFunc)(ctx, req.(*rpc.ProxyEngineRequest))
	case ProxyOpsReplicaRemove:
		return fn.(replicaRemoveFunc)(ctx, req.(*rpc.EngineReplicaRemoveRequest))
	case ProxyOpsReplicaVerifyRebuild:
		return fn.(replicaVerifyRebuildFunc)(ctx, req.(*rpc.EngineReplicaVerifyRebuildRequest))
	case ProxyOpsReplicaModeUpdate:
		return fn.(replicaModeUpdateFunc)(ctx, req.(*rpc.EngineReplicaModeUpdateRequest))
	// Snapshot operations
	case ProxyOpsVolumeSnapshot:
		return fn.(volumeSnapshotFunc)(ctx, req.(*rpc.EngineVolumeSnapshotRequest))
	case ProxyOpsSnapshotList:
		return fn.(snapshotListFunc)(ctx, req.(*rpc.ProxyEngineRequest))
	case ProxyOpsSnapshotClone:
		return fn.(snapshotCloneFunc)(ctx, req.(*rpc.EngineSnapshotCloneRequest))
	case ProxyOpsSnapshotCloneStatus:
		return fn.(snapshotCloneStatusFunc)(ctx, req.(*rpc.ProxyEngineRequest))
	case ProxyOpsSnapshotRevert:
		return fn.(snapshotRevertFunc)(ctx, req.(*rpc.EngineSnapshotRevertRequest))
	case ProxyOpsSnapshotPurge:
		return fn.(snapshotPurgeFunc)(ctx, req.(*rpc.EngineSnapshotPurgeRequest))
	case ProxyOpsSnapshotPurgeStatus:
		return fn.(snapshotPurgeStatusFunc)(ctx, req.(*rpc.ProxyEngineRequest))
	case ProxyOpsSnapshotRemove:
		return fn.(snapshotRemoveFunc)(ctx, req.(*rpc.EngineSnapshotRemoveRequest))
	case ProxyOpsSnapshotHash:
		return fn.(snapshotHashFunc)(ctx, req.(*rpc.EngineSnapshotHashRequest))
	case ProxyOpsSnapshotHashStatus:
		return fn.(snapshotHashStatusFunc)(ctx, req.(*rpc.EngineSnapshotHashStatusRequest))
	// Backup operations
	case ProxyOpsSnapshotBackup:
		return fn.(snapshotBackupFunc)(ctx, req.(*rpc.EngineSnapshotBackupRequest))
	case ProxyOpsSnapshotBackupStatus:
		return fn.(snapshotBackupStatusFunc)(ctx, req.(*rpc.EngineSnapshotBackupStatusRequest))
	case ProxyOpsBackupRestore:
		return fn.(backupRestoreFunc)(ctx, req.(*rpc.EngineBackupRestoreRequest))
	case ProxyOpsBackupRestoreStatus:
		return fn.(backupRestoreStatusFunc)(ctx, req.(*rpc.ProxyEngineRequest))

	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown proxy operation")
	}
}
