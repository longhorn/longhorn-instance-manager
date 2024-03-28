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
		if diskStatus.Driver != string(commonTypes.DiskDriverUioPciGeneric) {
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
	switch diskStatus.Driver {
	case string(commonTypes.DiskDriverNvme):
		return commonTypes.DiskDriverNvme, nil
	case string(commonTypes.DiskDriverVirtioPci):
		subsystems, err := util.GetBlockDiskSubsystems(diskPath)
		if err != nil {
			return commonTypes.DiskDriverNone, errors.Wrapf(err, "failed to get disk subsystems for %s", diskPath)
		}

		if slices.Contains(subsystems, string(BlockDiskSubsystemVirtio)) && slices.Contains(subsystems, string(BlockDiskSubsystemPci)) {
			diskDriver := commonTypes.DiskDriverVirtioBlk
			if slices.Contains(subsystems, string(BlockDiskSubsystemScsi)) {
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
