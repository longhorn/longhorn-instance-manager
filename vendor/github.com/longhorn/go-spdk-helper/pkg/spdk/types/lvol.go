package types

import (
	"fmt"
	"strings"
)

type BdevDriverSpecificLvol struct {
	LvolStoreUUID string   `json:"lvol_store_uuid"`
	BaseBdev      string   `json:"base_bdev"`
	BaseSnapshot  string   `json:"base_snapshot,omitempty"`
	ThinProvision bool     `json:"thin_provision"`
	Snapshot      bool     `json:"snapshot"`
	Clone         bool     `json:"clone"`
	Clones        []string `json:"clones,omitempty"`
}

type LvstoreInfo struct {
	UUID              string `json:"uuid"`
	Name              string `json:"name"`
	BaseBdev          string `json:"base_bdev"`
	TotalDataClusters uint64 `json:"total_data_clusters"`
	FreeClusters      uint64 `json:"free_clusters"`
	BlockSize         uint64 `json:"block_size"`
	ClusterSize       uint64 `json:"cluster_size"`
}

type LvolInfo struct {
	Alias             string `json:"alias"`
	UUID              string `json:"uuid"`
	Name              string `json:"name"`
	IsThinProvisioned bool   `json:"is_thin_provisioned"`
	IsSnapshot        bool   `json:"is_snapshot"`
	IsClone           bool   `json:"is_clone"`
	IsEsnapClone      bool   `json:"is_esnap_clone"`
	IsDegraded        bool   `json:"is_degraded"`
	Lvs               struct {
		Name string `json:"name"`
		UUID string `json:"uuid"`
	} `json:"lvs"`
}

type BdevLvolCreateLvstoreRequest struct {
	BdevName string `json:"bdev_name"`
	LvsName  string `json:"lvs_name"`

	ClusterSz uint32 `json:"cluster_sz,omitempty"`
	// ClearMethod               string `json:"clear_method,omitempty"`
	// NumMdPagesPerClusterRatio uint32 `json:"num_md_pages_per_cluster_ratio,omitempty"`
}

type BdevLvolDeleteLvstoreRequest struct {
	UUID    string `json:"uuid,omitempty"`
	LvsName string `json:"lvs_name,omitempty"`
}

type BdevLvolRenameLvstoreRequest struct {
	OldName string `json:"old_name"`
	NewName string `json:"new_name"`
}

type BdevLvolGetXattrRequest struct {
	Name      string `json:"name"`
	XattrName string `json:"xattr_name"`
}

type BdevLvolGetLvstoreRequest struct {
	UUID    string `json:"uuid,omitempty"`
	LvsName string `json:"lvs_name,omitempty"`
}

type BdevLvolClearMethod string

const (
	BdevLvolClearMethodNone        = "none"
	BdevLvolClearMethodUnmap       = "unmap"
	BdevLvolClearMethodWriteZeroes = "write_zeroes"
)

type BdevLvolCreateRequest struct {
	LvsName       string              `json:"lvs_name,omitempty"`
	UUID          string              `json:"uuid,omitempty"`
	LvolName      string              `json:"lvol_name"`
	SizeInMib     uint64              `json:"size_in_mib"`
	ClearMethod   BdevLvolClearMethod `json:"clear_method,omitempty"`
	ThinProvision bool                `json:"thin_provision,omitempty"`
}

type BdevLvolDeleteRequest struct {
	Name string `json:"name"`
}

type BdevLvolSnapshotRequest struct {
	LvolName     string `json:"lvol_name"`
	SnapshotName string `json:"snapshot_name"`
}

type BdevLvolCloneRequest struct {
	SnapshotName string `json:"snapshot_name"`
	CloneName    string `json:"clone_name"`
}

type BdevLvolDecoupleParentRequest struct {
	Name string `json:"name"`
}

type BdevLvolResizeRequest struct {
	Name string `json:"name"`
	Size uint64 `json:"size"`
}

type BdevLvolShallowCopyRequest struct {
	SrcLvolName string `json:"src_lvol_name"`
	DstBdevName string `json:"dst_bdev_name"`
}

func GetLvolAlias(lvsName, lvolName string) string {
	return fmt.Sprintf("%s/%s", lvsName, lvolName)
}

func GetLvsNameFromAlias(alias string) string {
	splitRes := strings.Split(alias, "/")
	if len(splitRes) != 2 {
		return ""
	}
	return splitRes[0]
}

func GetLvolNameFromAlias(alias string) string {
	splitRes := strings.Split(alias, "/")
	if len(splitRes) != 2 {
		return ""
	}
	return splitRes[1]
}
