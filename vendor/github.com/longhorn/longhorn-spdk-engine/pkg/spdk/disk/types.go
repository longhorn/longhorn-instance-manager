package disk

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/pkg/errors"

	commonTypes "github.com/longhorn/go-common-libs/types"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

type BlockDiskSubsystem string

const (
	BlockDiskSubsystemVirtio = BlockDiskSubsystem("virtio")
	BlockDiskSubsystemPci    = BlockDiskSubsystem("pci")
	BlockDiskSubsystemNvme   = BlockDiskSubsystem("nvme")
	BlockDiskSubsystemScsi   = BlockDiskSubsystem("scsi")
)

type BlockDiskType string

const (
	BlockDiskTypeDisk = BlockDiskType("disk")
	BlockDiskTypeLoop = BlockDiskType("loop")
)

func GetDiskDriver(diskDriver commonTypes.DiskDriver, diskPathOrBdf string) (commonTypes.DiskDriver, error) {
	if isBDF(diskPathOrBdf) {
		return getDiskDriverForBDF(diskDriver, diskPathOrBdf)
	}

	return getDiskDriverForPath(diskDriver, diskPathOrBdf)
}

func getDiskDriverForBDF(diskDriver commonTypes.DiskDriver, bdf string) (commonTypes.DiskDriver, error) {
	executor, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for disk driver detection")
	}

	diskStatus, err := spdksetup.GetDiskStatus(bdf, executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get disk status for BDF %s", bdf)
	}

	switch diskDriver {
	case commonTypes.DiskDriverAuto:
		diskPath := ""
		if diskStatus.Driver != string(commonTypes.DiskDriverVfioPci) &&
			diskStatus.Driver != string(commonTypes.DiskDriverUioPciGeneric) {
			devName, err := util.GetDevNameFromBDF(bdf)
			if err != nil {
				return "", errors.Wrapf(err, "failed to get device name from BDF %s", bdf)
			}
			diskPath = fmt.Sprintf("/dev/%s", devName)
		}
		return getDriverForAuto(diskStatus, diskPath)
	case commonTypes.DiskDriverAio, commonTypes.DiskDriverNvme, commonTypes.DiskDriverVirtioScsi, commonTypes.DiskDriverVirtioBlk:
		return diskDriver, nil
	default:
		return commonTypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for BDF %s", diskDriver, bdf)
	}
}

func getDriverForAuto(diskStatus *helpertypes.DiskStatus, diskPath string) (commonTypes.DiskDriver, error) {
	// SPDK supports various types of disks, including NVMe, virtio-blk, and virtio-scsi.
	//
	// NVMe disks can be managed by either NVMe bdev or AIO bdev.
	// VirtIO disks can be managed by virtio-blk, virtio-scsi, or AIO bdev.
	//
	// To use the correct bdev,  need to identify the disk type.
	// Here's how to identify the disk type:
	// - If a block device uses the subsystems virtio and pci, it's a virtio-blk disk.
	// - If it uses the subsystems virtio, pci, and scsi, it's a virtio-scsi disk.
	switch diskStatus.Driver {
	case string(commonTypes.DiskDriverNvme):
		return commonTypes.DiskDriverNvme, nil
	case string(commonTypes.DiskDriverVirtioPci):
		blockdevice, err := util.GetBlockDevice(diskPath)
		if err != nil {
			return commonTypes.DiskDriverNone, errors.Wrapf(err, "failed to get blockdevice info for %s", diskPath)
		}

		if slices.Contains(blockdevice.Subsystems, string(BlockDiskSubsystemVirtio)) && slices.Contains(blockdevice.Subsystems, string(BlockDiskSubsystemPci)) {
			diskDriver := commonTypes.DiskDriverVirtioBlk
			if slices.Contains(blockdevice.Subsystems, string(BlockDiskSubsystemScsi)) {
				diskDriver = commonTypes.DiskDriverVirtioScsi
			}
			return diskDriver, nil
		}

		return commonTypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for disk path %s", diskStatus.Driver, diskPath)
	default:
		return commonTypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for disk path %s", diskStatus.Driver, diskPath)
	}
}

func getDiskDriverForPath(diskDriver commonTypes.DiskDriver, diskPath string) (commonTypes.DiskDriver, error) {
	switch diskDriver {
	case commonTypes.DiskDriverAuto, commonTypes.DiskDriverAio:
		return commonTypes.DiskDriverAio, nil
	default:
		return commonTypes.DiskDriverNone, fmt.Errorf("unsupported disk driver %s for disk path %s", diskDriver, diskPath)
	}
}

func isBDF(addr string) bool {
	bdfFormat := "[a-f0-9]{4}:[a-f0-9]{2}:[a-f0-9]{2}\\.[a-f0-9]{1}"
	bdfPattern := regexp.MustCompile(bdfFormat)
	return bdfPattern.MatchString(addr)
}
