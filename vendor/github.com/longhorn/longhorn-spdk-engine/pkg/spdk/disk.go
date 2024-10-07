package spdk

import (
	"fmt"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	commontypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	spdkutil "github.com/longhorn/go-spdk-helper/pkg/util"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk"
	_ "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk/aio"
	_ "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk/nvme"
	_ "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk/virtio-blk"
	_ "github.com/longhorn/longhorn-spdk-engine/pkg/spdk/disk/virtio-scsi"
)

const (
	defaultClusterSize = 1 * 1024 * 1024 // 1MB
	defaultBlockSize   = 4096            // 4KB

	hostPrefix = "/host"
)

func svcDiskCreate(spdkClient *spdkclient.Client, diskName, diskUUID, diskPath, diskDriver string, blockSize int64) (ret *spdkrpc.Disk, err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPath":   diskPath,
		"blockSize":  blockSize,
		"diskDriver": diskDriver,
	})

	log.Info("Creating disk")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Failed to create disk")
		} else {
			log.Info("Created disk")
		}
	}()

	if diskName == "" || diskPath == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name and disk path are required")
	}

	exactDiskDriver, err := disk.GetDiskDriver(commontypes.DiskDriver(diskDriver), diskPath)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get disk driver for disk %s", diskName)
	}

	lvstoreUUID, err := addBlockDevice(spdkClient, diskName, diskUUID, diskPath, exactDiskDriver, blockSize)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrap(err, "failed to add disk block device").Error())
	}

	return lvstoreToDisk(spdkClient, diskPath, "", lvstoreUUID, exactDiskDriver)
}

func svcDiskDelete(spdkClient *spdkclient.Client, diskName, diskUUID, diskPath, diskDriver string) (ret *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPath":   diskPath,
		"diskDriver": diskDriver,
	})

	log.Info("Deleting disk")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Failed to delete disk")
		} else {
			log.Info("Deleted disk")
		}
	}()

	if diskName == "" {
		return &emptypb.Empty{}, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	var lvstores []spdktypes.LvstoreInfo
	bdevName := ""

	if diskUUID != "" {
		lvstores, err = spdkClient.BdevLvolGetLvstore("", diskUUID)
	} else {
		// The disk is not successfully created in creation stage because the diskUUID is not provided,
		// so we blindly use the diskName as the bdevName here.
		log.Warn("Disk UUID is not provided, trying to get lvstore with disk name")
		lvstores, err = spdkClient.BdevLvolGetLvstore(diskName, "")
	}
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return nil, errors.Wrapf(err, "failed to get lvstore with UUID %v", diskUUID)
		}
		log.WithError(err).Warn("Failed to get lvstore with UUID or disk name, blindly use disk name as bdev name")
		bdevName = diskName
	} else {
		lvstore := &lvstores[0]
		if lvstore.Name != diskName {
			log.Warnf("Disk name %v does not match lvstore name %v", diskName, lvstore.Name)
			return nil, grpcstatus.Errorf(grpccodes.NotFound, "disk name %v does not match lvstore name %v", diskName, lvstore.Name)
		}
		bdevName = lvstore.BaseBdev
	}

	if _, err := disk.DiskDelete(spdkClient, bdevName, diskPath, diskDriver); err != nil {
		return nil, errors.Wrapf(err, "failed to delete disk %v", diskName)
	}

	return &emptypb.Empty{}, nil
}

type DeviceInfo struct {
	DeviceDriver string `json:"device_driver"`
}

func svcDiskGet(spdkClient *spdkclient.Client, diskName, diskPath, diskDriver string) (ret *spdkrpc.Disk, err error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskPath":   diskPath,
		"diskDriver": diskDriver,
	})

	log.Trace("Getting disk info")
	defer func() {
		if err != nil {
			log.WithError(err).Error("Failed to get disk info")
		} else {
			log.Trace("Got disk info")
		}
	}()

	if diskName == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name is required")
	}

	// Check if the disk exists
	bdevs, err := disk.DiskGet(spdkClient, diskName, diskPath, diskDriver, 0)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get bdev with name %v: %v", diskName, err)
		}
	}
	if len(bdevs) == 0 {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find disk bdev with name %v", diskName)
	}

	var targetBdev *spdktypes.BdevInfo
	var exactDiskDriver commontypes.DiskDriver

	for i, bdev := range bdevs {
		switch bdev.ProductName {
		case spdktypes.BdevProductNameAio:
			if bdev.DriverSpecific != nil {
				targetBdev = &bdevs[i]
				diskPath = util.RemovePrefix(bdev.DriverSpecific.Aio.FileName, hostPrefix)
			}
			exactDiskDriver = commontypes.DiskDriverAio
		case spdktypes.BdevProductNameVirtioBlk:
			exactDiskDriver = commontypes.DiskDriverVirtioBlk
			targetBdev = &bdevs[i]
		case spdktypes.BdevProductNameVirtioScsi:
			exactDiskDriver = commontypes.DiskDriverVirtioScsi
			targetBdev = &bdevs[i]
		case spdktypes.BdevProductNameNvme:
			exactDiskDriver = commontypes.DiskDriverNvme
			targetBdev = &bdevs[i]
		}
		if targetBdev != nil {
			break
		}
	}

	if targetBdev == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "failed to get disk bdev for disk %v: %v", diskName, err)
	}

	return lvstoreToDisk(spdkClient, diskPath, targetBdev.Name, "", exactDiskDriver)
}

func getDiskPath(path string) string {
	return filepath.Join(hostPrefix, path)
}

func getDiskID(filename string) (string, error) {
	executor, err := spdkutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", err
	}

	dev, err := spdkutil.DetectDevice(filename, executor)
	if err != nil {
		return "", errors.Wrapf(err, "failed to detect disk device %v", filename)
	}

	return fmt.Sprintf("%d-%d", dev.Major, dev.Minor), nil
}

func validateAioDiskCreation(spdkClient *spdkclient.Client, diskPath string, diskDriver commontypes.DiskDriver) error {
	diskID, err := getDiskID(getDiskPath(diskPath))
	if err != nil {
		return errors.Wrap(err, "failed to get disk device number")
	}

	bdevs, err := disk.DiskGet(spdkClient, "", "", string(diskDriver), 0)
	if err != nil {
		return errors.Wrap(err, "failed to get disk bdevs")
	}

	for _, bdev := range bdevs {
		id, err := getDiskID(bdev.DriverSpecific.Aio.FileName)
		if err != nil {
			return errors.Wrap(err, "failed to get disk device number")
		}

		if id == diskID {
			return fmt.Errorf("disk %v is already used by disk bdev %v", diskPath, bdev.Name)
		}
	}

	return nil
}

func addBlockDevice(spdkClient *spdkclient.Client, diskName, diskUUID, originalDiskPath string, diskDriver commontypes.DiskDriver, blockSize int64) (string, error) {
	log := logrus.WithFields(logrus.Fields{
		"diskName":   diskName,
		"diskUUID":   diskUUID,
		"diskPath":   originalDiskPath,
		"diskDriver": diskDriver,
		"blockSize":  blockSize,
	})

	diskPath := originalDiskPath
	if diskDriver == commontypes.DiskDriverAio {
		if err := validateAioDiskCreation(spdkClient, diskPath, diskDriver); err != nil {
			return "", errors.Wrap(err, "failed to validate disk creation")
		}
		diskPath = getDiskPath(originalDiskPath)
	}

	log.Info("Creating disk bdev")

	bdevName, err := disk.DiskCreate(spdkClient, diskName, diskPath, string(diskDriver), uint64(blockSize))
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorFileExists(err) {
			return "", errors.Wrapf(err, "failed to create disk bdev")
		}
	}

	bdevs, err := disk.DiskGet(spdkClient, bdevName, diskPath, "", 0)
	if err != nil {
		return "", errors.Wrapf(err, "failed to get disk bdev")
	}
	if len(bdevs) == 0 {
		return "", fmt.Errorf("cannot find disk bdev with name %v", bdevName)
	}
	if len(bdevs) > 1 {
		return "", fmt.Errorf("found multiple disk bdevs with name %v", bdevName)
	}
	bdev := bdevs[0]

	// Name of the lvstore is the same as the name of the disk bdev
	lvstoreName := bdev.Name

	log.Infof("Creating lvstore %v", lvstoreName)

	lvstores, err := spdkClient.BdevLvolGetLvstore("", "")
	if err != nil {
		return "", errors.Wrapf(err, "failed to get lvstores")
	}

	for _, lvstore := range lvstores {
		if lvstore.BaseBdev != bdevName {
			continue
		}

		if diskUUID != "" && diskUUID != lvstore.UUID {
			continue
		}

		log.Infof("Found an existing lvstore %v", lvstore.Name)
		if lvstore.Name == lvstoreName {
			return lvstore.UUID, nil
		}

		// Rename the existing lvstore to the name of the disk bdev if the UUID matches
		log.Infof("Renaming the existing lvstore %v to %v", lvstore.Name, lvstoreName)
		renamed, err := spdkClient.BdevLvolRenameLvstore(lvstore.Name, lvstoreName)
		if err != nil {
			return "", errors.Wrapf(err, "failed to rename lvstore %v to %v", lvstore.Name, lvstoreName)
		}
		if !renamed {
			return "", fmt.Errorf("failed to rename lvstore %v to %v", lvstore.Name, lvstoreName)
		}
		return lvstore.UUID, nil
	}

	if diskUUID == "" {
		log.Infof("Creating a new lvstore %v", lvstoreName)
		return spdkClient.BdevLvolCreateLvstore(bdev.Name, lvstoreName, defaultClusterSize)
	}

	// The lvstore should be created before, but it cannot be found now.
	return "", grpcstatus.Error(grpccodes.NotFound, fmt.Sprintf("cannot find lvstore with UUID %v", diskUUID))
}

func lvstoreToDisk(spdkClient *spdkclient.Client, diskPath, lvstoreName, lvstoreUUID string, diskDriver commontypes.DiskDriver) (*spdkrpc.Disk, error) {
	lvstores, err := spdkClient.BdevLvolGetLvstore(lvstoreName, lvstoreUUID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get lvstore with name %v and UUID %v", lvstoreName, lvstoreUUID)
	}
	lvstore := &lvstores[0]

	// A disk does not have a fsid, so we use the device number as the disk ID
	diskID := diskPath
	if diskDriver == commontypes.DiskDriverAio {
		diskID, err = getDiskID(getDiskPath(diskPath))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get disk ID")
		}
	}

	return &spdkrpc.Disk{
		Id:          diskID,
		Name:        lvstore.Name,
		Uuid:        lvstore.UUID,
		Path:        diskPath,
		Type:        DiskTypeBlock,
		Driver:      string(diskDriver),
		TotalSize:   int64(lvstore.TotalDataClusters * lvstore.ClusterSize),
		FreeSize:    int64(lvstore.FreeClusters * lvstore.ClusterSize),
		TotalBlocks: int64(lvstore.TotalDataClusters * lvstore.ClusterSize / lvstore.BlockSize),
		FreeBlocks:  int64(lvstore.FreeClusters * lvstore.ClusterSize / lvstore.BlockSize),
		BlockSize:   int64(lvstore.BlockSize),
		ClusterSize: int64(lvstore.ClusterSize),
	}, nil
}
