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
)

type InstanceType string

const (
	InstanceTypeReplica = InstanceType("replica")
	InstanceTypeEngine  = InstanceType("engine")
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
