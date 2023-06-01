package spdk

import "fmt"

const (
	DiskTypeFilesystem = "filesystem"
	DiskTypeBlock      = "block"
)

func GetReplicaSnapshotLvolName(replicaName, snapshotName string) string {
	return fmt.Sprintf("%s-snaps-%s", replicaName, snapshotName)
}

func GetNvmfEndpoint(nqn, ip string, port int32) string {
	return fmt.Sprintf("%s://%s:%d", nqn, ip, port)
}
