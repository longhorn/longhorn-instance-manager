package types

import "github.com/longhorn/types/pkg/generated/spdkrpc"

type Mode string

const (
	ModeWO  = Mode("WO")
	ModeRW  = Mode("RW")
	ModeERR = Mode("ERR")
)

const (
	FrontendSPDKTCPNvmf     = "spdk-tcp-nvmf"
	FrontendSPDKTCPBlockdev = "spdk-tcp-blockdev"
	FrontendEmpty           = ""
)

type InstanceState string

const (
	InstanceStatePending     = "pending"
	InstanceStateStopped     = "stopped"
	InstanceStateRunning     = "running"
	InstanceStateTerminating = "terminating"
	InstanceStateError       = "error"
	InstanceStateSuspended   = "suspended"
)

type InstanceType string

const (
	InstanceTypeReplica      = InstanceType("replica")
	InstanceTypeEngine       = InstanceType("engine")
	InstanceTypeBackingImage = InstanceType("backingImage")
)

type BackingImageState string

const (
	BackingImageStatePending    = BackingImageState("pending")
	BackingImageStateStarting   = BackingImageState("starting")
	BackingImageStateReady      = BackingImageState("ready")
	BackingImageStateInProgress = BackingImageState("in-progress")
	BackingImageStateFailed     = BackingImageState("failed")
	BackingImageStateUnknown    = BackingImageState("unknown")
)

const (
	BackingImagePortCount = 1
)

const VolumeHead = "volume-head"

const SPDKServicePort = 8504

func ReplicaModeToGRPCReplicaMode(mode Mode) spdkrpc.ReplicaMode {
	switch mode {
	case ModeWO:
		return spdkrpc.ReplicaMode_WO
	case ModeRW:
		return spdkrpc.ReplicaMode_RW
	case ModeERR:
		return spdkrpc.ReplicaMode_ERR
	}
	return spdkrpc.ReplicaMode_ERR
}

func GRPCReplicaModeToReplicaMode(replicaMode spdkrpc.ReplicaMode) Mode {
	switch replicaMode {
	case spdkrpc.ReplicaMode_WO:
		return ModeWO
	case spdkrpc.ReplicaMode_RW:
		return ModeRW
	case spdkrpc.ReplicaMode_ERR:
		return ModeERR
	}
	return ModeERR
}

func IsFrontendSupported(frontend string) bool {
	return frontend == FrontendEmpty || frontend == FrontendSPDKTCPNvmf || frontend == FrontendSPDKTCPBlockdev
}

const (
	ProgressStateError      = "error"
	ProgressStateComplete   = "complete"
	ProgressStateInProgress = "in_progress"
	ProgressStateStarting   = "starting"

	// SPDKShallowCopyStateNew is the state returned from spdk_tgt. There is no underscore in the string.
	SPDKShallowCopyStateInProgress = "in progress"
)

// Longhorn defined snapshot attributes
const (
	LonghornBackingImageSnapshotAttrChecksum     = "longhorn_backing_image_checksum"
	LonghornBackingImageSnapshotAttrUUID         = "longhorn_backing_image_uuid"
	LonghornBackingImageSnapshotAttrPrepareState = "longhorn_backing_image_prepare_state"
)
