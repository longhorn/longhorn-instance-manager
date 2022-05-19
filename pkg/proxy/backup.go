package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
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

	"github.com/longhorn/backupstore"
	bsutil "github.com/longhorn/backupstore/util"
)

func (p *Proxy) SnapshotBackup(ctx context.Context, req *rpc.EngineSnapshotBackupRequest) (resp *rpc.EngineSnapshotBackupProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Debugf("Backing up snapshots %v to backup %v", req.SnapshotName, req.BackupName)

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
	log.Debugf("Getting %v backup status from replica %v", req.BackupName, req.ReplicaAddress)

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
				logrus.Debugf("Failed to create replica client with %v", r.Address.Address)
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
			return nil, errors.Errorf("failed to get % v backup status on unknown replica %s", req.BackupName, replicaAddress)
		}
	}

	cReplica, err := rclient.NewReplicaClient(replicaAddress)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create replica client with %v", replicaAddress)
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
	log.Debugf("Restoring backup %v to %v", req.Url, req.VolumeName)

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
			log.Debugf("Cannot marshal err [%v] to json: %v", err, jsonErr)
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
	log.Debug("Getting backup restore status")

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

func (p *Proxy) BackupGet(ctx context.Context, req *rpc.EngineBackupGetRequest) (resp *rpc.EngineBackupGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	log.Debug("Getting backup")

	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	recv, err := backupstore.InspectBackup(req.DestUrl)
	if err != nil {
		return nil, err
	}

	return &rpc.EngineBackupGetProxyResponse{
		Backup: parseBackup(recv),
	}, nil
}

func (p *Proxy) BackupVolumeGet(ctx context.Context, req *rpc.EngineBackupVolumeGetRequest) (resp *rpc.EngineBackupVolumeGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	log.Debug("Getting backup volume")

	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	recv, err := backupstore.InspectVolume(req.DestUrl)
	if err != nil {
		return nil, err
	}

	return &rpc.EngineBackupVolumeGetProxyResponse{
		Volume: &rpc.EngineBackupVolumeInfo{
			Name:                 recv.Name,
			Size:                 recv.Size,
			Labels:               recv.Labels,
			Created:              recv.Created,
			LastBackupName:       recv.LastBackupName,
			LastBackupAt:         recv.LastBackupAt,
			DataStored:           recv.DataStored,
			Messages:             parseMessages(recv.Messages),
			Backups:              parseBackups(recv.Backups),
			BackingImageName:     recv.BackingImageName,
			BackingImageChecksum: recv.BackingImageChecksum,
		},
	}, nil
}

func (p *Proxy) BackupVolumeList(ctx context.Context, req *rpc.EngineBackupVolumeListRequest) (resp *rpc.EngineBackupVolumeListProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	log.Debug("Listing backup volumes")

	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	recv, err := backupstore.List(req.VolumeName, req.DestUrl, req.VolumeOnly)
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineBackupVolumeListProxyResponse{
		Volumes: map[string]*rpc.EngineBackupVolumeInfo{},
	}
	for k, v := range recv {
		resp.Volumes[k] = &rpc.EngineBackupVolumeInfo{
			Name:                 v.Name,
			Size:                 v.Size,
			Labels:               v.Labels,
			Created:              v.Created,
			LastBackupName:       v.LastBackupName,
			LastBackupAt:         v.LastBackupAt,
			DataStored:           v.DataStored,
			Messages:             parseMessages(v.Messages),
			Backups:              parseBackups(v.Backups),
			BackingImageName:     v.BackingImageName,
			BackingImageChecksum: v.BackingImageChecksum,
		}
	}

	return resp, nil
}

func parseBackups(in map[string]*backupstore.BackupInfo) (out map[string]*rpc.EngineBackupInfo) {
	out = map[string]*rpc.EngineBackupInfo{}
	for k, v := range in {
		out[k] = parseBackup(v)
	}
	return out
}

func parseBackup(in *backupstore.BackupInfo) (out *rpc.EngineBackupInfo) {
	return &rpc.EngineBackupInfo{
		Name:                   in.Name,
		Url:                    in.URL,
		SnapshotName:           in.SnapshotName,
		SnapshotCreated:        in.SnapshotCreated,
		Created:                in.Created,
		Size:                   in.Size,
		Labels:                 in.Labels,
		IsIncremental:          in.IsIncremental,
		VolumeName:             in.VolumeName,
		VolumeSize:             in.VolumeSize,
		VolumeCreated:          in.VolumeCreated,
		VolumeBackingImageName: in.VolumeBackingImageName,
		Messages:               parseMessages(in.Messages),
	}
}

func parseMessages(in map[backupstore.MessageType]string) (out map[string]string) {
	out = map[string]string{}
	for k, v := range in {
		out[string(k)] = v
	}
	return out
}

func (p *Proxy) BackupConfigMetaGet(ctx context.Context, req *rpc.EngineBackupConfigMetaGetRequest) (resp *rpc.EngineBackupConfigMetaGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	log.Debug("Getting backup config metadata")

	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	ts, err := backupstore.GetConfigMetadata(req.DestUrl)
	if err != nil {
		return nil, err
	}

	resp = &rpc.EngineBackupConfigMetaGetProxyResponse{}
	resp.ModificationTime, err = ptypes.TimestampProto(ts.ModificationTime)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *Proxy) BackupRemove(ctx context.Context, req *rpc.EngineBackupRemoveRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	if req.VolumeName != "" {
		log = log.WithField("volume", req.VolumeName)
	}
	log.Debug("Removing backups")

	for _, env := range req.Envs {
		part := strings.SplitN(env, "=", 2)
		if len(part) < 2 {
			continue
		}

		if err := os.Setenv(part[0], part[1]); err != nil {
			return nil, err
		}
	}

	if req.VolumeName == "" {
		if err := backupstore.DeleteDeltaBlockBackup(req.DestUrl); err != nil {
			return nil, err
		}
	} else {
		if !bsutil.ValidateName(req.VolumeName) {
			return nil, errors.Errorf("invalid backup volume name %v", req.VolumeName)
		}
		if err := backupstore.DeleteBackupVolume(req.VolumeName, req.DestUrl); err != nil {
			return nil, err
		}
	}

	return &empty.Empty{}, nil
}
