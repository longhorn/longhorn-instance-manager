package virtioscsi

import (
	"github.com/pkg/errors"

	commonTypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdksetup "github.com/longhorn/go-spdk-helper/pkg/spdk/setup"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
)

type DiskDriverVirtioScsi struct {
}

func init() {
	driver := &DiskDriverVirtioScsi{}
	disk.RegisterDiskDriver(string(commonTypes.DiskDriverVirtioScsi), driver)
}

func (d *DiskDriverVirtioScsi) DiskCreate(spdkClient *spdkclient.Client, diskName, diskPath string, blockSize uint64) (string, error) {
	// TODO: validate the diskPath
	executor, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get the executor for virtio-scsi disk create %v", diskPath)
	}

	_, err = spdksetup.Bind(diskPath, string(commonTypes.DiskDriverUioPciGeneric), executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to bind virtio-scsi disk %v with %v", diskPath, string(commonTypes.DiskDriverUioPciGeneric))
	}

	bdevs, err := spdkClient.BdevVirtioAttachController(diskName, "pci", diskPath, "scsi")
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach virtio-scsi disk %v", diskPath)
	}
	if len(bdevs) == 0 {
		return "", errors.Errorf("failed to attach virtio-scsi disk %v", diskPath)
	}
	return bdevs[0], nil
}

func (d *DiskDriverVirtioScsi) DiskDelete(spdkClient *spdkclient.Client, diskName, diskPath string) (deleted bool, err error) {
	executor, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return false, errors.Wrapf(err, "failed to get the executor for virtio-scsi disk %v deletion", diskName)
	}

	_, err = spdkClient.BdevVirtioDetachController(diskName)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return false, errors.Wrapf(err, "failed to detach virtio-scsi disk %v", diskName)
		}
	}

	_, err = spdksetup.Unbind(diskPath, executor)
	if err != nil {
		return false, errors.Wrapf(err, "failed to unbind virtio-scsi disk %v", diskPath)
	}
	return true, nil
}

func (d *DiskDriverVirtioScsi) DiskGet(spdkClient *spdkclient.Client, diskName, diskPath string, timeout uint64) ([]spdktypes.BdevInfo, error) {
	return spdkClient.BdevGetBdevs(diskName, timeout)
}
