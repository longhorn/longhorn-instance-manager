package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	backupstore "github.com/longhorn/backupstore"
	butil "github.com/longhorn/backupstore/util"
	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	rclient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
)

func (p *Proxy) CleanupBackupMountPoints(ctx context.Context, req *emptypb.Empty) (resp *emptypb.Empty, err error) {
	if err := backupstore.CleanUpAllMounts(); err != nil {
		return &emptypb.Empty{}, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to unmount all mount points").Error())
	}
	return &emptypb.Empty{}, nil
}

func (p *Proxy) SnapshotBackup(ctx context.Context, req *rpc.EngineSnapshotBackupRequest) (resp *rpc.EngineSnapshotBackupProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Backing up snapshot %v to backup %v", req.SnapshotName, req.BackupName)

	switch req.ProxyEngineRequest.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.snapshotBackup(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkSnapshotBackup(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.ProxyEngineRequest.DataEngine)
	}
}

func (p *Proxy) snapshotBackup(ctx context.Context, req *rpc.EngineSnapshotBackupRequest) (resp *rpc.EngineSnapshotBackupProxyResponse, err error) {
	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	credential, err := butil.GetBackupCredential(req.BackupTarget)
	if err != nil {
		return nil, err
	}

	labels := []string{}
	for k, v := range req.Labels {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}

	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}

	recv, err := task.CreateBackup(
		req.BackupName,
		req.SnapshotName,
		req.BackupTarget,
		req.BackingImageName,
		req.BackingImageChecksum,
		req.CompressionMethod,
		int(req.ConcurrentLimit),
		req.StorageClassName,
		labels,
		credential,
	)
	if err != nil {
		return nil, err
	}

	return &rpc.EngineSnapshotBackupProxyResponse{
		BackupId:      recv.BackupID,
		Replica:       recv.ReplicaAddress,
		IsIncremental: recv.IsIncremental,
	}, nil
}

func (p *Proxy) spdkSnapshotBackup(ctx context.Context, req *rpc.EngineSnapshotBackupRequest) (resp *rpc.EngineSnapshotBackupProxyResponse, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}

func (p *Proxy) SnapshotBackupStatus(ctx context.Context, req *rpc.EngineSnapshotBackupStatusRequest) (resp *rpc.EngineSnapshotBackupStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Tracef("Getting %v backup status from replica %v", req.BackupName, req.ReplicaAddress)

	switch req.ProxyEngineRequest.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.snapshotBackupStatus(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkSnapshotBackupStatus(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.ProxyEngineRequest.DataEngine)
	}
}

func (p *Proxy) snapshotBackupStatus(ctx context.Context, req *rpc.EngineSnapshotBackupStatusRequest) (resp *rpc.EngineSnapshotBackupStatusProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	replicas, err := p.ReplicaList(ctx, req.ProxyEngineRequest)
	if err != nil {
		return nil, err
	}

	replicaAddress := req.ReplicaAddress
	if replicaAddress == "" {
		// find a replica which has the corresponding backup
		for _, r := range replicas.ReplicaList.Replicas {
			mode := eptypes.GRPCReplicaModeToReplicaMode(r.Mode)
			if mode != etypes.RW {
				continue
			}

			// We don't know the replicaName here since we retrieved address from the engine, which doesn't know it.
			// Pass it anyway, since the default empty string disables validation and we may know it with a future
			// code change.
			cReplica, err := rclient.NewReplicaClient(r.Address.Address, req.ProxyEngineRequest.VolumeName,
				r.Address.InstanceName)
			if err != nil {
				logrus.WithError(err).Debugf("Failed to create replica client with %v", r.Address.Address)
				continue
			}

			_, err = esync.FetchBackupStatus(cReplica, req.BackupName, r.Address.Address)
			cReplica.Close()
			if err == nil {
				replicaAddress = r.Address.Address
				break
			}
		}
	}

	if replicaAddress == "" {
		return nil, errors.Errorf("failed to find a replica with backup %s", req.BackupName)
	}

	for _, r := range replicas.ReplicaList.Replicas {
		if r.Address.Address != replicaAddress {
			continue
		}
		mode := eptypes.GRPCReplicaModeToReplicaMode(r.Mode)
		if mode != etypes.RW {
			return nil, errors.Errorf("failed to get %v backup status on unknown replica %s", req.BackupName, replicaAddress)
		}
	}

	// We may know replicaName here. If we don't, we pass an empty string, which disables validation.
	cReplica, err := rclient.NewReplicaClient(replicaAddress, req.ProxyEngineRequest.VolumeName, req.ReplicaName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create replica client with %v", replicaAddress)
	}
	defer cReplica.Close()

	status, err := esync.FetchBackupStatus(cReplica, req.BackupName, replicaAddress)
	if err != nil {
		return nil, err
	}

	return &rpc.EngineSnapshotBackupStatusProxyResponse{
		BackupUrl:      status.BackupURL,
		Error:          status.Error,
		Progress:       int32(status.Progress),
		SnapshotName:   status.SnapshotName,
		State:          status.State,
		ReplicaAddress: replicaAddress,
	}, nil
}

func (p *Proxy) spdkSnapshotBackupStatus(ctx context.Context, req *rpc.EngineSnapshotBackupStatusRequest) (resp *rpc.EngineSnapshotBackupStatusProxyResponse, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}

func (p *Proxy) BackupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest) (resp *rpc.EngineBackupRestoreProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Restoring backup %v to %v", req.Url, req.VolumeName)

	switch req.ProxyEngineRequest.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.backupRestore(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkBackupRestore(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.ProxyEngineRequest.DataEngine)
	}
}

func (p *Proxy) backupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest) (resp *rpc.EngineBackupRestoreProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})

	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	credential, err := butil.GetBackupCredential(req.Target)
	if err != nil {
		return nil, err
	}

	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineBackupRestoreProxyResponse{
		TaskError: []byte{},
	}
	err = task.RestoreBackup(req.Url, credential, int(req.ConcurrentLimit))
	if err != nil {
		errInfo, jsonErr := json.Marshal(err)
		if jsonErr != nil {
			log.WithError(jsonErr).Debugf("Cannot marshal err [%v] to json", err)
		}
		// If the error is not `TaskError`, the marshaled result is an empty json string.
		if string(errInfo) != "{}" {
			resp.TaskError = errInfo
		} else {
			resp.TaskError = []byte(err.Error())
		}
	}

	return resp, nil
}

func (p *Proxy) spdkBackupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest) (resp *rpc.EngineBackupRestoreProxyResponse, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}

func (p *Proxy) BackupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Getting backup restore status")

	switch req.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.backupRestoreStatus(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkBackupRestoreStatus(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.DataEngine)
	}
}

func (p *Proxy) backupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
	task, err := esync.NewTask(ctx, req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}

	recv, err := task.RestoreStatus()
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineBackupRestoreStatusProxyResponse{
		Status: map[string]*rpc.EngineBackupRestoreStatus{},
	}
	for k, v := range recv {
		resp.Status[k] = &rpc.EngineBackupRestoreStatus{
			IsRestoring:            v.IsRestoring,
			LastRestored:           v.LastRestored,
			CurrentRestoringBackup: v.CurrentRestoringBackup,
			Progress:               int32(v.Progress),
			Error:                  v.Error,
			Filename:               v.Filename,
			State:                  v.State,
			BackupUrl:              v.BackupURL,
		}
	}

	return resp, nil
}

func (p *Proxy) spdkBackupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
	/* TODO: implement this */
	return &rpc.EngineBackupRestoreStatusProxyResponse{
		Status: map[string]*rpc.EngineBackupRestoreStatus{},
	}, nil
}
