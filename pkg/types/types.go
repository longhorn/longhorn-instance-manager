package types

import (
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
