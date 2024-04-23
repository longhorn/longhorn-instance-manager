package nvme

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	commonTypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
)

type DiskDriverNvme struct {
}

func init() {
	driver := &DiskDriverNvme{}
	disk.RegisterDiskDriver(string(commonTypes.DiskDriverNvme), driver)
}

func (d *DiskDriverNvme) DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath string, blockSize uint64) (string, error) {
	// TODO: validate the diskPath
	executor, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for NVMe disk create %v", diskPath)
	}

	_, err = spdksetup.Bind(diskPath, string(commonTypes.DiskDriverUioPciGeneric), executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to bind NVMe disk %v with %v", diskPath, string(commonTypes.DiskDriverUioPciGeneric))
	}

	bdevs, err := spdkClient.BdevNvmeAttachController(diskName, "", diskPath, "", "PCIe", "",
		helpertypes.DefaultCtrlrLossTimeoutSec, helpertypes.DefaultReconnectDelaySec, helpertypes.DefaultFastIOFailTimeoutSec,
		helpertypes.DefaultMultipath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach NVMe disk %v", diskPath)
	}
	if len(bdevs) == 0 {
		return "", errors.Errorf("failed to attach NVMe disk %v", diskPath)
	}
	return bdevs[0], nil
}

func (d *DiskDriverNvme) DiskDelete(spdkClient *spdkclient.Client, diskName, diskPath string) (deleted bool, err error) {
	executor, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get the executor for NVMe disk %v deletion", diskName)
	}

	_, err = spdkClient.BdevNvmeDetachController(diskName)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return false, errors.Wrapf(err, "failed to detach NVMe disk %v", diskName)
		}
	}

	_, err = spdksetup.Unbind(diskPath, executor)
	if err != nil {
		return false, errors.Wrapf(err, "failed to unbind NVMe disk %v", diskPath)
	}
	return true, nil
}

func (d *DiskDriverNvme) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	bdevs, err := spdkClient.BdevGetBdevs("", 0)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get bdevs")
	}
	foundBdevs := []spdktypes.BdevInfo{}
	for _, bdev := range bdevs {
		if bdev.DriverSpecific == nil {
			continue
		}
		if bdev.DriverSpecific.Nvme == nil {
			continue
		}
		nvmes := *bdev.DriverSpecific.Nvme
		for _, nvme := range nvmes {
			if nvme.PciAddress == diskPath {
				logrus.Infof("Found bdev %v for NVMe disk %v", bdev, diskName)
				foundBdevs = append(foundBdevs, bdev)
			}
		}
	}
	return foundBdevs, nil
}
