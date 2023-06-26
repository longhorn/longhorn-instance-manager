package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	rclient "github.com/longhorn/longhorn-engine/pkg/replica/client"
	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	etypes "github.com/longhorn/longhorn-engine/pkg/types"
	eutil "github.com/longhorn/longhorn-engine/pkg/util"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (p *Proxy) SnapshotBackup(ctx context.Context, req *rpc.EngineSnapshotBackupRequest) (resp *rpc.EngineSnapshotBackupProxyResponse, err error) {
<<<<<<< HEAD
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
=======
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":         req.ProxyEngineRequest.Address,
		"engineName":         req.ProxyEngineRequest.EngineName,
		"volumeName":         req.ProxyEngineRequest.VolumeName,
		"backendStoreDriver": req.ProxyEngineRequest.BackendStoreDriver,
	})
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
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
<<<<<<< HEAD
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Tracef("Getting %v backup status from replica %v", req.BackupName, req.ReplicaAddress)

	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address)
=======
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":         req.ProxyEngineRequest.Address,
		"engineName":         req.ProxyEngineRequest.EngineName,
		"volumeName":         req.ProxyEngineRequest.VolumeName,
		"backendStoreDriver": req.ProxyEngineRequest.BackendStoreDriver,
	})
	log.Tracef("Getting %v backup status from replica %v", req.BackupName, req.ReplicaAddress)

	switch req.ProxyEngineRequest.BackendStoreDriver {
	case rpc.BackendStoreDriver_v1:
		return p.snapshotBackupStatus(ctx, req)
	case rpc.BackendStoreDriver_v2:
		return p.spdkSnapshotBackupStatus(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown backend store driver %v", req.ProxyEngineRequest.BackendStoreDriver)
	}
}

func (p *Proxy) snapshotBackupStatus(ctx context.Context, req *rpc.EngineSnapshotBackupStatusRequest) (resp *rpc.EngineSnapshotBackupStatusProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
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
<<<<<<< HEAD
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Infof("Restoring backup %v to %v", req.Url, req.VolumeName)

=======
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":         req.ProxyEngineRequest.Address,
		"engineName":         req.ProxyEngineRequest.EngineName,
		"volumeName":         req.ProxyEngineRequest.VolumeName,
		"backendStoreDriver": req.ProxyEngineRequest.BackendStoreDriver,
	})
	log.Infof("Restoring backup %v to %v", req.Url, req.VolumeName)

	switch req.ProxyEngineRequest.BackendStoreDriver {
	case rpc.BackendStoreDriver_v1:
		return p.backupRestore(ctx, req)
	case rpc.BackendStoreDriver_v2:
		return p.spdkBackupRestore(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown backend store driver %v", req.ProxyEngineRequest.BackendStoreDriver)
	}
}

func (p *Proxy) backupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest) (resp *rpc.EngineBackupRestoreProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":         req.ProxyEngineRequest.Address,
		"engineName":         req.ProxyEngineRequest.EngineName,
		"volumeName":         req.ProxyEngineRequest.VolumeName,
		"backendStoreDriver": req.ProxyEngineRequest.BackendStoreDriver,
	})

>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
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

	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
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
<<<<<<< HEAD
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Trace("Getting backup restore status")

	task, err := esync.NewTask(ctx, req.Address)
=======
	log := logrus.WithFields(logrus.Fields{
		"serviceURL":         req.Address,
		"engineName":         req.EngineName,
		"volumeName":         req.VolumeName,
		"backendStoreDriver": req.BackendStoreDriver,
	})
	log.Trace("Getting backup restore status")

	switch req.BackendStoreDriver {
	case rpc.BackendStoreDriver_v1:
		return p.backupRestoreStatus(ctx, req)
	case rpc.BackendStoreDriver_v2:
		return p.spdkBackupRestoreStatus(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown backend store driver %v", req.BackendStoreDriver)
	}
}

func (p *Proxy) backupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
	task, err := esync.NewTask(ctx, req.Address, req.VolumeName, req.EngineName)
>>>>>>> 04a30dc (Use fields from ProxyEngineRequest to instantiate tasks and controller clients)
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
