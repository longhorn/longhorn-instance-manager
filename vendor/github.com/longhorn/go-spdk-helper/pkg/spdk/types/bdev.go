package types

type BdevProductName string

const (
	BdevProductNameAio  = BdevProductName("AIO disk")
	BdevProductNameLvol = BdevProductName("Logical Volume")
	BdevProductNameRaid = BdevProductName("Raid Volume")
	BdevProductNameNvme = BdevProductName("NVMe disk")
)

type BdevType string

const (
	BdevTypeAio  = "aio"
	BdevTypeLvol = "lvol"
	BdevTypeRaid = "raid"
	BdevTypeNvme = "nvme"
)

func GetBdevType(bdev *BdevInfo) BdevType {
	if bdev == nil {
		return ""
	}
	if bdev.ProductName == BdevProductNameAio && bdev.DriverSpecific.Aio != nil {
		return BdevTypeAio
	}
	if bdev.ProductName == BdevProductNameLvol && bdev.DriverSpecific.Lvol != nil {
		return BdevTypeLvol
	}
	if bdev.ProductName == BdevProductNameRaid && bdev.DriverSpecific.Raid != nil {
		return BdevTypeRaid
	}
	if bdev.ProductName == BdevProductNameNvme && bdev.DriverSpecific.Nvme != nil {
		return BdevTypeNvme
	}
	return ""
}

type BdevInfoBasic struct {
	Name         string          `json:"name"`
	Aliases      []string        `json:"aliases"`
	ProductName  BdevProductName `json:"product_name"`
	BlockSize    uint32          `json:"block_size"`
	NumBlocks    uint64          `json:"num_blocks"`
	UUID         string          `json:"uuid,omitempty"`
	CreationTime string          `json:"creation_time,omitempty"`

	MdSize               uint32 `json:"md_size,omitempty"`
	MdInterleave         bool   `json:"md_interleave,omitempty"`
	DifType              uint32 `json:"dif_type,omitempty"`
	DifIsHeadOfMd        bool   `json:"dif_is_head_of_md,omitempty"`
	EnabledDifCheckTypes *struct {
		Reftag bool `json:"reftag"`
		Apptag bool `json:"apptag"`
		Guard  bool `json:"guard"`
	} `json:"enabled_dif_check_types,omitempty"`

	AssignedRateLimits AssignedRateLimits `json:"assigned_rate_limits"`

	Claimed   bool      `json:"claimed"`
	ClaimType ClaimType `json:"claim_type,omitempty"`

	Zoned            bool   `json:"zoned"`
	ZoneSize         uint64 `json:"zone_size,omitempty"`
	MaxOpenZones     uint64 `json:"max_open_zones,omitempty"`
	OptimalOpenZones uint64 `json:"optimal_open_zones,omitempty"`

	SupportedIoTypes SupportedIoTypes `json:"supported_io_types"`

	MemoryDomains []struct {
		DmaDeviceID   string `json:"dma_device_id"`
		DmaDeviceType int32  `json:"dma_device_type"`
	} `json:"memory_domains,omitempty"`
}

type AssignedRateLimits struct {
	RwIosPerSec    uint64 `json:"rw_ios_per_sec"`
	RwMbytesPerSec uint64 `json:"rw_mbytes_per_sec"`
	RMbytesPerSec  uint64 `json:"r_mbytes_per_sec"`
	WMbytesPerSec  uint64 `json:"w_mbytes_per_sec"`
}

type ClaimType string

const (
	ClaimTypeNone                = ClaimType("none")
	ClaimTypeExclusiveWrite      = ClaimType("exclusive_write")
	ClaimTypeReadManyWriteOne    = ClaimType("read_many_write_one")
	ClaimTypeReadManyWriteNone   = ClaimType("read_many_write_none")
	ClaimTypeReadManyWriteShared = ClaimType("read_many_write_shared")
)

type SupportedIoTypes struct {
	Read            bool `json:"read"`
	Write           bool `json:"write"`
	Unmap           bool `json:"unmap"`
	WriteZeroes     bool `json:"write_zeroes"`
	Flush           bool `json:"flush"`
	Reset           bool `json:"reset"`
	Compare         bool `json:"compare"`
	CompareAndWrite bool `json:"compare_and_write"`
	Abort           bool `json:"abort"`
	NvmeAdmin       bool `json:"nvme_admin"`
	NvmeIo          bool `json:"nvme_io"`
}

type BdevDriverSpecific struct {
	Aio *BdevDriverSpecificAio `json:"aio,omitempty"`

	Lvol *BdevDriverSpecificLvol `json:"lvol,omitempty"`

	Raid *BdevRaidInfo `json:"raid,omitempty"`

	Nvme     *BdevDriverSpecificNvme `json:"nvme,omitempty"`
	MpPolicy BdevNvmeMultipathPolicy `json:"mp_policy,omitempty"`
}

type BdevInfo struct {
	BdevInfoBasic

	DriverSpecific *BdevDriverSpecific `json:"driver_specific"`
}

type BdevGetBdevsRequest struct {
	Name    string `json:"name,omitempty"`
	Timeout uint64 `json:"timeout,omitempty"`
}

type BdevGetBdevsResponse struct {
	bdevs []BdevInfo
}
