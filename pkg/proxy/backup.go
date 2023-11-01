package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

<<<<<<< HEAD
	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

=======
	backupstore "github.com/longhorn/backupstore"
	butil "github.com/longhorn/backupstore/util"
>>>>>>> 055db7e (feat(backuptarget): add cleanup all mounts grpc function)
	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	rclient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eutil "github.com/longhorn/longhorn-engine/pkg/util"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (p *Proxy) CleanupBackupMountPoints(ctx context.Context, req *empty.Empty) (resp *empty.Empty, err error) {
	if err := backupstore.CleanUpAllMounts(); err != nil {
		return &empty.Empty{}, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to unmount all mount points").Error())
	}
	return &empty.Empty{}, nil
}

func (p *Proxy) SnapshotBackup(ctx context.Context, req *rpc.EngineSnapshotBackupRequest) (resp *rpc.EngineSnapshotBackupProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Infof("Backing up snapshot %v to backup %v", req.SnapshotName, req.BackupName)

	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	credential, err := eutil.GetBackupCredential(req.BackupTarget)
	if err != nil {
		return nil, err
	}

	labels := []string{}
	for k, v := range req.Labels {
		labels = append(labels, fmt.Sprintf("%s=%s", k, v))
	}

	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}

	recv, err := task.CreateBackup(
		req.BackupName,
		req.SnapshotName,
		req.BackupTarget,
		req.BackingImageName,
		req.BackingImageChecksum,
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

func (p *Proxy) SnapshotBackupStatus(ctx context.Context, req *rpc.EngineSnapshotBackupStatusRequest) (resp *rpc.EngineSnapshotBackupStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Tracef("Getting %v backup status from replica %v", req.BackupName, req.ReplicaAddress)

	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address)
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

			cReplica, err := rclient.NewReplicaClient(r.Address.Address)
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

	cReplica, err := rclient.NewReplicaClient(replicaAddress)
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

func (p *Proxy) BackupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest) (resp *rpc.EngineBackupRestoreProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Infof("Restoring backup %v to %v", req.Url, req.VolumeName)

	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	credential, err := eutil.GetBackupCredential(req.Target)
	if err != nil {
		return nil, err
	}

	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineBackupRestoreProxyResponse{
		TaskError: []byte{},
	}
	err = task.RestoreBackup(req.Url, credential)
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

func (p *Proxy) BackupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Trace("Getting backup restore status")

	task, err := esync.NewTask(ctx, req.Address)
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
