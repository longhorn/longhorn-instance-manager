package client

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/backupstore"
)

func (c *ProxyClient) SnapshotBackup(serviceAddress,
	backupName, snapshotName, backupTarget,
	backingImageName, backingImageChecksum string,
	labels map[string]string, envs []string) (backupID, replicaAddress string, err error) {
	if serviceAddress == "" {
		return "", "", errors.Wrapf(ErrParameter, "failed to backup snapshot")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Backing up snapshot %v to %v via proxy", snapshotName, backupName)

	return backupID, replicaAddress, nil
}

func (c *ProxyClient) SnapshotBackupStatus(serviceAddress, backupName, replicaAddress string) (status *SnapshotBackupStatus, err error) {
	if serviceAddress == "" || backupName == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup status")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Getting %v backup status via proxy", backupName)

	return status, nil
}

func (c *ProxyClient) BackupRestore(serviceAddress, url, target, volumeName string, envs []string) (taskError []byte, err error) {
	if serviceAddress == "" || url == "" || target == "" || volumeName == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to restore backup to volume")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Restoring %v backup to %v via proxy", url, volumeName)

	return taskError, nil
}

func (c *ProxyClient) BackupRestoreStatus(serviceAddress string) (status map[string]*BackupRestoreStatus, err error) {
	if serviceAddress == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup restore status")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debug("Getting backup restore status via proxy")

	return status, nil
}

func (c *ProxyClient) BackupGet(destURL string, envs []string) (info *EngineBackupInfo, err error) {
	if destURL == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Getting %v backup via proxy", destURL)

	return info, nil
}

func (c *ProxyClient) BackupVolumeGet(destURL string, envs []string) (info *EngineBackupVolumeInfo, err error) {
	if destURL == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup volume")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Getting %v backup volume via proxy", destURL)

	return info, nil
}

func (c *ProxyClient) BackupVolumeList(destURL, volumeName string, volumeOnly bool, envs []string) (info map[string]*EngineBackupVolumeInfo, err error) {
	if destURL == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to list backup volumes")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	if volumeName != "" {
		log = log.WithField("volume", volumeName)
	}
	log.Debugf("Listing %v backup volumes via proxy", destURL)

	return info, nil
}

func (c *ProxyClient) BackupConfigMetaGet(destURL string, envs []string) (meta *backupstore.ConfigMetadata, err error) {
	if destURL == "" {
		return nil, errors.Wrapf(ErrParameter, "failed to get backup config metadata")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	log.Debugf("Getting %v backup config metadata via proxy", destURL)

	return nil, nil
}

func (c *ProxyClient) BackupRemove(destURL, volumeName string, envs []string) (err error) {
	if destURL == "" {
		return errors.Wrapf(ErrParameter, "failed to remove backup")
	}

	log := logrus.WithFields(logrus.Fields{"serviceURL": c.ServiceURL})
	if volumeName != "" {
		log = log.WithField("volume", volumeName)
	}
	log.Debugf("Removing %v backup via proxy", destURL)

	return nil
}
