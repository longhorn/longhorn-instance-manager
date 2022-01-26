package proxy

import (
	"fmt"
	"os"
	"strings"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"

	esync "github.com/longhorn/longhorn-engine/pkg/sync"
	eutil "github.com/longhorn/longhorn-engine/pkg/util"
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

	resp = &rpc.EngineSnapshotBackupStatusProxyResponse{}

	// TODO

	return resp, nil
}

func (p *Proxy) BackupRestore(ctx context.Context, req *rpc.EngineBackupRestoreRequest) (resp *rpc.EngineBackupRestoreProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Debugf("Restoring backup %v to %v", req.Url, req.VolumeName)

	resp = &rpc.EngineBackupRestoreProxyResponse{}

	// TODO

	return resp, nil
}

func (p *Proxy) BackupRestoreStatus(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineBackupRestoreStatusProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Debug("Getting backup restore status")

	resp = &rpc.EngineBackupRestoreStatusProxyResponse{
		Status: map[string]*rpc.EngineBackupRestoreStatus{},
	}

	// TODO

	return resp, nil
}

func (p *Proxy) BackupGet(ctx context.Context, req *rpc.EngineBackupGetRequest) (resp *rpc.EngineBackupGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	log.Debug("Getting backup")

	resp = &rpc.EngineBackupGetProxyResponse{
		Backup: &rpc.EngineBackupInfo{},
	}

	// TODO

	return resp, nil
}

func (p *Proxy) BackupVolumeGet(ctx context.Context, req *rpc.EngineBackupVolumeGetRequest) (resp *rpc.EngineBackupVolumeGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	log.Debug("Getting backup volume")

	resp = &rpc.EngineBackupVolumeGetProxyResponse{
		Volume: &rpc.EngineBackupVolumeInfo{},
	}

	// TODO

	return resp, nil
}

func (p *Proxy) BackupVolumeList(ctx context.Context, req *rpc.EngineBackupVolumeListRequest) (resp *rpc.EngineBackupVolumeListProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	log.Debug("Listing backup volumes")

	resp = &rpc.EngineBackupVolumeListProxyResponse{
		Volumes: map[string]*rpc.EngineBackupVolumeInfo{},
	}

	// TODO

	return resp, nil
}

func (p *Proxy) BackupConfigMetaGet(ctx context.Context, req *rpc.EngineBackupConfigMetaGetRequest) (resp *rpc.EngineBackupConfigMetaGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	log.Debug("Getting backup config metadata")

	resp = &rpc.EngineBackupConfigMetaGetProxyResponse{}

	// TODO

	return resp, nil
}

func (p *Proxy) BackupRemove(ctx context.Context, req *rpc.EngineBackupRemoveRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"destURL": req.DestUrl})
	if req.VolumeName != "" {
		log = log.WithField("volume", req.VolumeName)
	}
	log.Debug("Removing backups")

	// TODO

	return &empty.Empty{}, nil
}
