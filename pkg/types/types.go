package types

import (
	"path"
	"strings"
	"time"

	"k8s.io/mount-utils"
)

const (
	GRPCServiceTimeout = 3 * time.Minute

	ProcessStateRunning  = "running"
	ProcessStateStarting = "starting"
	ProcessStateStopped  = "stopped"
	ProcessStateStopping = "stopping"
	ProcessStateError    = "error"
)

var (
	WaitInterval = 100 * time.Millisecond
	WaitCount    = 600
)

const (
	RetryInterval = 3 * time.Second
	RetryCounts   = 3
)

const (
	GlobalMountPathPattern = "/host/var/lib/kubelet/plugins/kubernetes.io/csi/driver.longhorn.io/*/globalmount"

	EngineConditionFilesystemReadOnly = "FilesystemReadOnly"
)

func IsMountPointReadOnly(mp mount.MountPoint) bool {
	for _, opt := range mp.Opts {
		if opt == "ro" {
			return true
		}
	}
	return false
}

func GetVolumeMountPointMap() (map[string]mount.MountPoint, error) {
	volumeMountPointMap := make(map[string]mount.MountPoint)

	mounter := mount.New("")
	mountPoints, err := mounter.List()
	if err != nil {
		return nil, err
	}

	for _, mp := range mountPoints {
		match, err := path.Match(types.GlobalMountPathPattern, mp.Path)
		if err != nil {
			return nil, err
		}
		if match {
			volumeNameSHAStr := GetVolumeNameSHAStrFromPath(mp.Path)
			volumeMountPointMap[volumeNameSHAStr] = mp
		}
	}
	return volumeMountPointMap, nil
}

func GetVolumeNameSHAStrFromPath(path string) string {
	// mount path for volume: "/host/var/lib/kubelet/plugins/kubernetes.io/csi/driver.longhorn.io/${VolumeNameSHAStr}/globalmount"
	pathSlices := strings.Split(path, "/")
	volumeNameSHAStr := pathSlices[len(pathSlices)-2]
	return volumeNameSHAStr
}

func ProcessNameToVolumeName(processName string) string {
	// process name: "pvc-e130e369-274d-472d-98d1-f6074d2725e8-e-0"
	nameSlices := strings.Split(processName, "-")
	volumeName := strings.Join(nameSlices[:len(nameSlices)-2], "-")
	return volumeName
}
