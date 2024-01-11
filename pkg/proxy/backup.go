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
	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"

	"github.com/longhorn/longhorn-instance-manager/pkg/util"

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

	if err := setEnv(req.Envs); err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to set envs").Error())
	}

	credential, err := butil.GetBackupCredential(req.BackupTarget)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, errors.Wrapf(err, "failed to get backup credential").Error())
	}

	labels := getLabels(req.Labels)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.SnapshotBackup(ctx, req, credential, labels)
}

func (ops V1DataEngineProxyOps) SnapshotBackup(ctx context.Context, req *rpc.EngineSnapshotBackupRequest, credential map[string]string, labels []string) (resp *rpc.EngineSnapshotBackupProxyResponse, err error) {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName, req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create task").Error())
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
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create backup").Error())
	}

	return &rpc.EngineSnapshotBackupProxyResponse{
		BackupId:      recv.BackupID,
		Replica:       recv.ReplicaAddress,
		IsIncremental: recv.IsIncremental,
	}, nil
}

func (ops V2DataEngineProxyOps) SnapshotBackup(ctx context.Context, req *rpc.EngineSnapshotBackupRequest, credential map[string]string, labels []string) (resp *rpc.EngineSnapshotBackupProxyResponse, err error) {
	c, err := getSPDKClientFromEngineAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get SPDK client from engine address %v", req.ProxyEngineRequest.Address).Error())
	}
	defer c.Close()

	snapshotName := req.SnapshotName
	if snapshotName == "" {
		snapshotName = util.UUID()
	}

	recv, err := c.EngineBackupCreate(&spdkclient.BackupCreateRequest{
		BackupName:           req.BackupName,
		SnapshotName:         snapshotName,
		VolumeName:           req.ProxyEngineRequest.VolumeName,
		EngineName:           req.ProxyEngineRequest.EngineName,
		BackupTarget:         req.BackupTarget,
		StorageClassName:     req.StorageClassName,
		BackingImageName:     req.BackingImageName,
		BackingImageChecksum: req.BackingImageChecksum,
		CompressionMethod:    req.CompressionMethod,
		ConcurrentLimit:      req.ConcurrentLimit,
		Labels:               labels,
		Credential:           credential,
	})
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create backup").Error())
	}

	return &rpc.EngineSnapshotBackupProxyResponse{
		BackupId:      recv.Backup,
		Replica:       recv.ReplicaAddress,
		IsIncremental: recv.IsIncremental,
	}, nil
}

func (p *Proxy) SnapshotBackupStatus(ctx context.Context, req *rpc.EngineSnapshotBackupStatusRequest) (resp *rpc.EngineSnapshotBackupStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Tracef("Getting %v backup status from replica %v", req.BackupName, req.ReplicaAddress)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.SnapshotBackupStatus(ctx, req)
}

func (ops V1DataEngineProxyOps) SnapshotBackupStatus(ctx context.Context, req *rpc.EngineSnapshotBackupStatusRequest) (resp *rpc.EngineSnapshotBackupStatusProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	replicas, err := ops.ReplicaList(ctx, req.ProxyEngineRequest)
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
			return nil, errors.Errorf("failed to get backup %v status on unknown replica %s", req.BackupName, replicaAddress)
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

func (ops V2DataEngineProxyOps) SnapshotBackupStatus(ctx context.Context, req *rpc.EngineSnapshotBackupStatusRequest) (resp *rpc.EngineSnapshotBackupStatusProxyResponse, err error) {
	c, err := getSPDKClientFromEngineAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get SPDK client from engine address %v", req.ProxyEngineRequest.Address).Error())
	}
	defer c.Close()

	status, err := c.EngineBackupStatus(req.BackupName, req.ProxyEngineRequest.EngineName, req.ReplicaAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get backup status").Error())
	}

	return &rpc.EngineSnapshotBackupStatusProxyResponse{
		BackupUrl:      status.BackupUrl,
		Error:          status.Error,
		Progress:       int32(status.Progress),
		SnapshotName:   status.SnapshotName,
		State:          status.State,
		ReplicaAddress: status.ReplicaAddress,
	}, nil
}

func (p *Proxy) BackupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest) (resp *rpc.EngineBackupRestoreProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Restoring backup %v to %v", req.Url, req.VolumeName)

	if err := setEnv(req.Envs); err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to set envs").Error())
	}

	credential, err := butil.GetBackupCredential(req.Target)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, errors.Wrapf(err, "failed to get backup credential").Error())
	}

	resp = &rpc.EngineBackupRestoreProxyResponse{
		TaskError: []byte{},
	}

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	err = ops.BackupRestore(ctx, req, credential)
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

func (ops V1DataEngineProxyOps) BackupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest, credential map[string]string) error {
	task, err := esync.NewTask(ctx, req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return err
	}
	return task.RestoreBackup(req.Url, credential, int(req.ConcurrentLimit))
}

func (ops V2DataEngineProxyOps) BackupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest, credential map[string]string) error {
	c, err := getSPDKClientFromEngineAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get SPDK client from engine address %v", req.ProxyEngineRequest.Address).Error())
	}
	defer c.Close()

	return c.EngineBackupRestore(&spdkclient.BackupRestoreRequest{
		BackupUrl:       req.Url,
		EngineName:      req.ProxyEngineRequest.EngineName,
		Credential:      credential,
		ConcurrentLimit: req.ConcurrentLimit,
	})
}

func (p *Proxy) BackupRestoreFinish(ctx context.Context, req *rpc.EngineBackupRestoreFinishRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Info("Finishing backup restoration")

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.BackupRestoreFinish(ctx, req)
}

func (ops V1DataEngineProxyOps) BackupRestoreFinish(ctx context.Context, req *rpc.EngineBackupRestoreFinishRequest) (*emptypb.Empty, error) {
	// TODO: implement this
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}

func (ops V2DataEngineProxyOps) BackupRestoreFinish(ctx context.Context, req *rpc.EngineBackupRestoreFinishRequest) (*emptypb.Empty, error) {
	c, err := getSPDKClientFromEngineAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get SPDK client from engine address %v", req.ProxyEngineRequest.Address).Error())
	}
	defer c.Close()

	err = c.EngineBackupRestoreFinish(req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to finish backup restoration").Error())
	}
	return &emptypb.Empty{}, nil
}

func (p *Proxy) BackupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Getting backup restore status")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.BackupRestoreStatus(ctx, req)
}

func (ops V1DataEngineProxyOps) BackupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
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

func (ops V2DataEngineProxyOps) BackupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
	c, err := getSPDKClientFromEngineAddress(req.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get SPDK client from engine address %v", req.Address).Error())
	}
	defer c.Close()

	recv, err := c.EngineRestoreStatus(req.EngineName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, errors.Wrapf(err, "failed to get restore status").Error())
	}

	resp = &rpc.EngineBackupRestoreStatusProxyResponse{
		Status: map[string]*rpc.EngineBackupRestoreStatus{},
	}
	for address, status := range recv.Status {
		replicaURL := "tcp://" + address
		resp.Status[replicaURL] = &rpc.EngineBackupRestoreStatus{
			IsRestoring:            status.IsRestoring,
			LastRestored:           status.LastRestored,
			CurrentRestoringBackup: status.CurrentRestoringBackup,
			Progress:               int32(status.Progress),
			Error:                  status.Error,
			State:                  status.State,
			BackupUrl:              status.BackupUrl,
			Filename:               status.DestFileName,
		}
	}
	return resp, nil
}

func setEnv(envs []string) error {
	for _, env := range envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return err
		}
	}
	return nil
}

func getLabels(labels map[string]string) []string {
	kvs := []string{}
	for k, v := range labels {
		kvs = append(kvs, fmt.Sprintf("%s=%s", k, v))
	}
	return kvs
}
