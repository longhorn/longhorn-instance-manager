package lvm

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	lhexec "github.com/longhorn/go-common-libs/exec"
	lhtypes "github.com/longhorn/go-common-libs/types"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

const (
	VGOpaquePrefix = "longhorn-vg-"
	// DevicesFileName is persisted by the local IM under the host control path.
	DevicesFileName = "longhorn.devices"
	DevicesFilePath = "/etc/lvm/devices/" + DevicesFileName
	ThinPoolName    = "longhorn-thin-pool"

	ProvisioningModeThick = "thick"
	ProvisioningModeThin  = "thin"

	lvmRepresentativePVTag = "longhorn-representative"

	ThinPoolReserveBytes int64 = 5 * 1024 * 1024 * 1024

	lvmDiskStateReady = "ready"

	// The local instance manager bind-mounts the host /dev into its mount
	// namespace. Let LVM synchronously manage device nodes there instead of
	// depending on host udev rules or udev cookie synchronization.
	lvmNoUdevConfig = `devices { external_device_info_source = "none" } activation { udev_sync = 0 udev_rules = 0 }`
)

func IsProvisioningMode(mode string) bool {
	switch mode {
	case ProvisioningModeThick, ProvisioningModeThin:
		return true
	default:
		return false
	}
}

func IsThinProvisioningMode(mode string) bool {
	return mode == ProvisioningModeThin
}

type Disk struct {
	executor       lhexec.ExecuteInterface
	metricsSampler *KernelBlockMetricsSampler
}

func NewDisk() Disk {
	return Disk{executor: NewExecutor(), metricsSampler: NewKernelBlockMetricsSampler()}
}

// InitializeDevicesFile creates the devices file when it does not already exist.
func InitializeDevicesFile() error {
	created, err := createDevicesFileIfMissing(DevicesFilePath)
	if err != nil {
		return err
	}
	log := logrus.WithField("devicesFile", DevicesFilePath)
	if created {
		log.Info("Created LVM devices file")
	} else {
		log.Info("Using existing LVM devices file")
	}
	return nil
}

func createDevicesFileIfMissing(path string) (bool, error) {
	created := true
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if os.IsExist(err) {
		created = false
		file, err = os.OpenFile(path, os.O_WRONLY, 0600)
	}
	if err != nil {
		return false, fmt.Errorf("failed to create or open LVM devices file %v: %w", path, err)
	}
	if err := file.Close(); err != nil {
		return false, fmt.Errorf("failed to close LVM devices file %v: %w", path, err)
	}
	return created, nil
}

// CommandArgs limits an LVM command to Longhorn's persistent devices file and
// makes its device-node management independent of udev.
func CommandArgs(args ...string) []string {
	return append([]string{"--devicesfile", DevicesFileName, "--config", lvmNoUdevConfig}, args...)
}

func newVGName() string {
	return VGOpaquePrefix + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
}

// VGNameForPVUUID resolves the VG containing a Longhorn disk PV when the
// instance request does not carry the Longhorn disk name.
func VGNameForPVUUID(executor lhexec.ExecuteInterface, pvUUID string) (string, error) {
	output, err := executor.Execute(nil, "pvs", CommandArgs(
		"--noheadings", "--separator", ";", "--select", "pv_uuid="+pvUUID,
		"-o", "vg_name"), lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return "", fmt.Errorf("failed to find physical volume %v: %w", pvUUID, err)
	}
	if strings.TrimSpace(output) == "" {
		return "", fmt.Errorf("cannot find physical volume %v", pvUUID)
	}
	if len(strings.Split(strings.TrimSpace(output), "\n")) != 1 {
		return "", fmt.Errorf("physical volume UUID %v matched multiple volume groups: %v", pvUUID, output)
	}
	vgName := strings.TrimSpace(output)
	if vgName == "" {
		return "", fmt.Errorf("physical volume %v does not belong to a volume group", pvUUID)
	}
	return vgName, nil
}

// GetOrCreateThinPool returns the managed thin pool, creating it from all but
// the fixed VG reserve when necessary.
func GetOrCreateThinPool(executor lhexec.ExecuteInterface, vgName string) (*LogicalVolume, error) {
	pool, err := GetLogicalVolume(executor, vgName, ThinPoolName)
	if err != nil {
		return nil, err
	}
	if pool != nil {
		validationErr := validateThinPool(pool)
		if validationErr == nil {
			return pool, nil
		}
		if err := RemoveThinPoolIfUnused(executor, vgName); err != nil {
			return nil, fmt.Errorf("%v; failed to remove the empty thin pool: %w", validationErr, err)
		}
		pool, err = GetLogicalVolume(executor, vgName, ThinPoolName)
		if err != nil {
			return nil, err
		}
		if pool != nil {
			return nil, validationErr
		}
	}

	output, err := executor.Execute(nil, "vgs",
		CommandArgs("--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "-o", "vg_free,vg_extent_size", vgName),
		lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect free capacity of volume group %v: %w", vgName, err)
	}
	fields := strings.Split(strings.TrimSpace(output), ";")
	if len(fields) != 2 {
		return nil, fmt.Errorf("unexpected free-capacity output for volume group %v: %v", vgName, output)
	}
	freeSize, err := strconv.ParseInt(strings.TrimSpace(fields[0]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse free size of volume group %v: %w", vgName, err)
	}
	extentSize, err := strconv.ParseInt(strings.TrimSpace(fields[1]), 10, 64)
	if err != nil || extentSize <= 0 {
		return nil, fmt.Errorf("failed to parse extent size of volume group %v: %w", vgName, err)
	}
	poolDataSize := ((freeSize - ThinPoolReserveBytes) / extentSize) * extentSize
	if poolDataSize <= 0 {
		return nil, fmt.Errorf("volume group %v has %v bytes free, not enough to retain the %v-byte thin-pool reserve", vgName, freeSize, ThinPoolReserveBytes)
	}

	if _, err := executor.Execute(nil, "lvcreate", CommandArgs(
		"-y", "--type", "thin-pool", "-n", ThinPoolName,
		"-L", fmt.Sprintf("%vb", poolDataSize), "--zero", "y", vgName), lhtypes.ExecuteDefaultTimeout); err != nil {
		return nil, fmt.Errorf("failed to create thin pool %v/%v: %w", vgName, ThinPoolName, err)
	}

	pool, err = GetLogicalVolume(executor, vgName, ThinPoolName)
	if err != nil {
		return nil, err
	}
	if pool == nil {
		return nil, fmt.Errorf("thin pool %v/%v is absent after creation", vgName, ThinPoolName)
	}
	if err := validateThinPool(pool); err != nil {
		return nil, err
	}
	logrus.WithFields(logrus.Fields{
		"dataSize": pool.Size,
		"vgName":   vgName,
	}).Info("Created local data engine thin pool")
	return pool, nil
}

func RemoveThinPoolIfUnused(executor lhexec.ExecuteInterface, vgName string) error {
	lvs, err := ListLogicalVolumes(executor)
	if err != nil {
		return err
	}
	var pool *LogicalVolume
	for i := range lvs {
		if lvs[i].VGName != vgName {
			continue
		}
		if lvs[i].IsThinVolume() {
			return nil
		}
		if lvs[i].Name == ThinPoolName {
			pool = &lvs[i]
		}
	}
	if pool == nil {
		return nil
	}
	if !pool.IsThinPool() {
		return fmt.Errorf("refusing to remove unmanaged logical volume %v/%v", vgName, ThinPoolName)
	}
	if _, err := executor.Execute(nil, "lvremove", CommandArgs("-y", vgName+"/"+ThinPoolName), lhtypes.ExecuteDefaultTimeout); err != nil {
		return fmt.Errorf("failed to remove empty thin pool %v/%v: %w", vgName, ThinPoolName, err)
	}
	logrus.WithField("vgName", vgName).Info("Deleted empty local data engine thin pool")
	return nil
}

func validateThinPool(pool *LogicalVolume) error {
	if !pool.IsThinPool() {
		return fmt.Errorf("logical volume %v/%v is not the expected thin pool", pool.VGName, pool.Name)
	}
	zeroed := len(pool.Attr) > 7 && pool.Attr[7] == 'z'
	if !zeroed {
		return fmt.Errorf("thin pool %v/%v has zeroing disabled", pool.VGName, pool.Name)
	}
	return nil
}

func (ops Disk) Create(req *rpc.DiskCreateRequest) (*rpc.Disk, error) {
	if req.DiskPath == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk path is required for LVM disk creation")
	}
	if err := ops.addDevice(req.DiskPath); err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}

	pv, err := ops.getPV(req.DiskPath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if pv == nil {
		if err := ops.verifyDeviceEmpty(req.DiskPath); err != nil {
			_ = ops.removeDevice(req.DiskPath)
			return nil, grpcstatus.Error(grpccodes.InvalidArgument, err.Error())
		}

		if _, err := ops.executor.Execute(nil, "pvcreate", CommandArgs(req.DiskPath), lhtypes.ExecuteDefaultTimeout); err != nil {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to create physical volume on %v: %v", req.DiskPath, err)
		}
		pv, err = ops.getPV(req.DiskPath)
		if err != nil || pv == nil {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to inspect newly created physical volume %v: %v", req.DiskPath, err)
		}
	}

	if pv.VGName == "" {
		vgName, needExtend, err := ops.getVG(req.StorageLayout)
		if err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		if needExtend {
			if _, err := ops.executor.Execute(nil, "vgextend", CommandArgs(vgName, req.DiskPath), lhtypes.ExecuteDefaultTimeout); err != nil {
				return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to extend volume group %v with %v: %v", vgName, req.DiskPath, err)
			}
		} else {
			if _, err := ops.executor.Execute(nil, "vgcreate", CommandArgs(vgName, req.DiskPath), lhtypes.ExecuteDefaultTimeout); err != nil {
				return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to create volume group %v on %v: %v", vgName, req.DiskPath, err)
			}
		}
		pv.VGName = vgName
	}

	if req.StorageLayout == rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_NODE {
		if err := ops.extendThinPool(pv.VGName); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
	}
	if err := ops.setRepresentativeIfMissing(pv.VGName, pv.UUID); err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}

	diskInfo, err := ops.getDiskInfo(pv.VGName, req.DiskName, req.DiskPath)
	if err == nil {
		logrus.WithFields(logrus.Fields{
			"devicePath": req.DiskPath,
			"diskName":   req.DiskName,
			"vgName":     pv.VGName,
		}).Info("Initialized LVM disk")
	}
	return diskInfo, err
}

func (ops Disk) getVG(layout rpc.LVMStorageLayout) (name string, needExtend bool, err error) {
	if layout == rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_DISK {
		return newVGName(), false, nil
	}

	vgNames, err := ops.listVGs()
	if err != nil {
		return "", false, err
	}
	if len(vgNames) > 1 {
		return "", false, fmt.Errorf("per-node layout requires one volume group, found %v", vgNames)
	}
	if len(vgNames) == 1 {
		return vgNames[0], true, nil
	}
	return newVGName(), false, nil
}

func (ops Disk) Delete(req *rpc.DiskDeleteRequest) (*emptypb.Empty, error) {
	if req.DiskPath == "" {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk path is required for LVM disk deletion")
	}

	pv, err := ops.getPV(req.DiskPath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if pv == nil {
		if err := ops.removeDevice(req.DiskPath); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		return &emptypb.Empty{}, nil
	}
	if req.DiskUuid != "" && pv.UUID != req.DiskUuid {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "physical volume %v UUID %v does not match disk UUID %v, refusing to remove it", req.DiskPath, pv.UUID, req.DiskUuid)
	}
	if pv.VGName != "" {
		pvs, err := ops.listVGPhysicalVolumes(pv.VGName)
		if err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		if len(pvs) > 1 && pv.Representative {
			return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition,
				"cannot remove representative physical volume %v while volume group %v has multiple physical volumes; remove the member disks first",
				req.DiskPath, pv.VGName)
		}
		lvs, err := queryLogicalVolumes(ops.executor, "vg_name="+pv.VGName)
		if err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		if len(lvs) > 0 {
			return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition,
				"cannot remove physical volume %v while volume group %v contains logical volumes",
				req.DiskPath, pv.VGName)
		}
		if len(pvs) == 1 {
			if _, err := ops.executor.Execute(nil, "vgremove", CommandArgs("-y", pv.VGName), lhtypes.ExecuteDefaultTimeout); err != nil {
				return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to remove volume group %v: %v", pv.VGName, err)
			}
		} else {
			if _, err := ops.executor.Execute(nil, "vgreduce", CommandArgs(pv.VGName, pv.Path), lhtypes.ExecuteDefaultTimeout); err != nil {
				return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to remove physical volume %v from volume group %v: %v", req.DiskPath, pv.VGName, err)
			}
		}
	}
	if _, err := ops.executor.Execute(nil, "pvremove", CommandArgs("-y", req.DiskPath), lhtypes.ExecuteDefaultTimeout); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to remove physical volume %v: %v", req.DiskPath, err)
	}
	if err := ops.removeDevice(req.DiskPath); err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	logrus.WithFields(logrus.Fields{
		"devicePath": req.DiskPath,
		"diskName":   req.DiskName,
		"vgName":     pv.VGName,
	}).Info("Deleted LVM disk")
	return &emptypb.Empty{}, nil
}

func (ops Disk) Get(req *rpc.DiskGetRequest) (*rpc.Disk, error) {
	pv, err := ops.getPV(req.DiskPath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if pv == nil || pv.VGName == "" {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find a managed physical volume on %v", req.DiskPath)
	}
	return ops.getDiskInfo(pv.VGName, req.DiskName, req.DiskPath)
}

func (ops Disk) Metrics(req *rpc.DiskGetRequest) (*rpc.DiskMetricsGetReply, error) {
	metrics, err := ops.metricsSampler.Sample("disk:"+req.DiskPath, req.DiskPath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	return &rpc.DiskMetricsGetReply{Metrics: metrics}, nil
}

// verifyDeviceEmpty rejects devices that carry an existing filesystem,
// partition table, or other signature.
func (ops Disk) verifyDeviceEmpty(devPath string) error {
	fi, err := os.Stat(devPath)
	if err != nil {
		return fmt.Errorf("failed to stat device %v: %v", devPath, err)
	}
	if fi.Mode()&os.ModeDevice == 0 || fi.Mode()&os.ModeCharDevice != 0 {
		return fmt.Errorf("device %v is not a block device", devPath)
	}

	output, err := ops.executor.Execute(nil, "wipefs", []string{"--noheadings", "--parsable", devPath}, lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return fmt.Errorf("failed to check signatures on device %v: %v", devPath, err)
	}
	if strings.TrimSpace(output) != "" {
		return fmt.Errorf("device %v has an existing filesystem or partition table", devPath)
	}
	return nil
}

type lvmPV struct {
	Path           string
	UUID           string
	VGName         string
	Representative bool
}

func (ops Disk) getPV(devPath string) (*lvmPV, error) {
	output, err := ops.executor.Execute(nil, "pvs", CommandArgs(
		"--noheadings", "--separator", ";", "--select", "pv_name="+devPath,
		"-o", "pv_name,pv_uuid,vg_name,pv_tags"), lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect physical volume %v: %w", devPath, err)
	}
	if strings.TrimSpace(output) == "" {
		return nil, nil
	}
	return parsePhysicalVolume(output)
}

func parsePhysicalVolume(output string) (*lvmPV, error) {
	fields := strings.Split(strings.TrimSpace(output), ";")
	if len(fields) != 4 {
		return nil, fmt.Errorf("unexpected physical-volume output: %v", output)
	}
	tags := strings.TrimSpace(fields[3])
	pv := &lvmPV{
		Path:   strings.TrimSpace(fields[0]),
		UUID:   strings.TrimSpace(fields[1]),
		VGName: strings.TrimSpace(fields[2]),
	}
	for _, tag := range strings.Split(tags, ",") {
		if strings.TrimSpace(tag) == lvmRepresentativePVTag {
			pv.Representative = true
			break
		}
	}
	return pv, nil
}

func (ops Disk) listVGPhysicalVolumes(vgName string) ([]lvmPV, error) {
	output, err := ops.executor.Execute(nil, "pvs", CommandArgs(
		"--noheadings", "--separator", ";", "--select", "vg_name="+vgName,
		"-o", "pv_name,pv_uuid,vg_name,pv_tags"), lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to list physical volumes in volume group %v: %w", vgName, err)
	}
	pvs := []lvmPV{}
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		pv, err := parsePhysicalVolume(line)
		if err != nil {
			return nil, err
		}
		pvs = append(pvs, *pv)
	}
	return pvs, nil
}

func (ops Disk) listVGs() ([]string, error) {
	output, err := ops.executor.Execute(nil, "vgs", CommandArgs("--noheadings", "-o", "vg_name"), lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to list volume groups: %w", err)
	}
	return strings.Fields(output), nil
}

func (ops Disk) setPVTag(path, tag string, add bool) error {
	action := "--addtag"
	if !add {
		action = "--deltag"
	}
	if _, err := ops.executor.Execute(nil, "pvchange", CommandArgs(action, tag, path), lhtypes.ExecuteDefaultTimeout); err != nil {
		return fmt.Errorf("failed to update tag %v on physical volume %v: %w", tag, path, err)
	}
	return nil
}

func (ops Disk) setRepresentativeIfMissing(vgName, preferredPVUUID string) error {
	pvs, err := ops.listVGPhysicalVolumes(vgName)
	if err != nil {
		return err
	}
	for _, pv := range pvs {
		if pv.Representative {
			return nil
		}
	}
	for _, pv := range pvs {
		if pv.UUID == preferredPVUUID {
			return ops.setPVTag(pv.Path, lvmRepresentativePVTag, true)
		}
	}
	return fmt.Errorf("cannot find preferred representative physical volume %v in volume group %v", preferredPVUUID, vgName)
}

func (ops Disk) extendThinPool(vgName string) error {
	pool, err := GetLogicalVolume(ops.executor, vgName, ThinPoolName)
	if err != nil {
		return err
	}
	if pool == nil {
		return nil
	}
	if !pool.IsThinPool() {
		return fmt.Errorf("logical volume %v/%v is not the expected thin pool", vgName, ThinPoolName)
	}
	output, err := ops.executor.Execute(nil, "vgs", CommandArgs(
		"--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "-o", "vg_free,vg_extent_size", vgName), lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return fmt.Errorf("failed to inspect free capacity after extending volume group %v: %w", vgName, err)
	}
	fields := strings.Split(strings.TrimSpace(output), ";")
	if len(fields) != 2 {
		return fmt.Errorf("unexpected free-capacity output for volume group %v: %v", vgName, output)
	}
	freeSize, err := strconv.ParseInt(strings.TrimSpace(fields[0]), 10, 64)
	if err != nil {
		return fmt.Errorf("failed to parse free size of volume group %v: %w", vgName, err)
	}
	extentSize, err := strconv.ParseInt(strings.TrimSpace(fields[1]), 10, 64)
	if err != nil || extentSize <= 0 {
		return fmt.Errorf("failed to parse extent size of volume group %v: %w", vgName, err)
	}
	extendSize := ((freeSize - ThinPoolReserveBytes) / extentSize) * extentSize
	if extendSize <= 0 {
		return nil
	}
	if _, err := ops.executor.Execute(nil, "lvextend", CommandArgs(
		"-y", "-L", fmt.Sprintf("+%vb", extendSize), vgName+"/"+ThinPoolName), lhtypes.ExecuteDefaultTimeout); err != nil {
		return fmt.Errorf("failed to extend thin pool %v/%v by %v bytes: %w", vgName, ThinPoolName, extendSize, err)
	}
	return nil
}

func (ops Disk) getDiskInfo(vgName, diskName, diskPath string) (*rpc.Disk, error) {
	pv, err := ops.getPV(diskPath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if pv == nil || pv.VGName != vgName {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find physical volume %v in volume group %v", diskPath, vgName)
	}
	output, err := ops.executor.Execute(nil, "vgs",
		CommandArgs("--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "-o", "vg_size,vg_free,vg_extent_size", vgName),
		lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find volume group %v", vgName)
		}
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get volume group %v: %v", vgName, err)
	}

	fields := strings.Split(strings.TrimSpace(output), ";")
	if len(fields) != 3 {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "unexpected vgs output for volume group %v: %v", vgName, output)
	}
	totalSize, err := strconv.ParseInt(strings.TrimSpace(fields[0]), 10, 64)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to parse size of volume group %v: %v", vgName, err)
	}
	freeSize, err := strconv.ParseInt(strings.TrimSpace(fields[1]), 10, 64)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to parse free size of volume group %v: %v", vgName, err)
	}
	extentSize, err := strconv.ParseInt(strings.TrimSpace(fields[2]), 10, 64)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to parse extent size of volume group %v: %v", vgName, err)
	}

	pool, err := GetLogicalVolume(ops.executor, vgName, ThinPoolName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to inspect thin pool in volume group %v: %v", vgName, err)
	}
	if pool != nil {
		if !pool.IsThinPool() {
			return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "logical volume %v/%v is not the expected thin pool", vgName, ThinPoolName)
		}
		totalSize = pool.Size
		allocatedSize := int64(float64(pool.Size) * pool.DataUsagePercentage / 100)
		freeSize = pool.Size - allocatedSize
		if freeSize < 0 {
			freeSize = 0
		}
		// TODO: Monitor thin-pool metadata usage and either stop scheduling near
		// exhaustion or expand metadata from the VG capacity reserved outside the pool.
	}
	if !pv.Representative {
		totalSize = 0
		freeSize = 0
	}

	return &rpc.Disk{
		Id:          pv.UUID,
		Uuid:        pv.UUID,
		Name:        diskName,
		Path:        diskPath,
		Type:        rpc.DiskType_lvm.String(),
		TotalSize:   totalSize,
		FreeSize:    freeSize,
		TotalBlocks: totalSize / extentSize,
		FreeBlocks:  freeSize / extentSize,
		BlockSize:   extentSize,
		State:       lvmDiskStateReady,
	}, nil
}

func (ops Disk) addDevice(devicePath string) error {
	if _, err := ops.executor.Execute(nil, "lvmdevices", CommandArgs("--adddev", devicePath), lhtypes.ExecuteDefaultTimeout); err != nil {
		return fmt.Errorf("failed to add device %v to LVM devices file %v: %v", devicePath, DevicesFileName, err)
	}
	logrus.WithFields(logrus.Fields{
		"devicePath":  devicePath,
		"devicesFile": DevicesFilePath,
	}).Info("Added device to LVM devices file")
	return nil
}

func (ops Disk) removeDevice(devicePath string) error {
	if _, err := ops.executor.Execute(nil, "lvmdevices", CommandArgs("--deldev", devicePath), lhtypes.ExecuteDefaultTimeout); err != nil {
		return fmt.Errorf("failed to remove device %v from LVM devices file %v: %v", devicePath, DevicesFileName, err)
	}
	logrus.WithFields(logrus.Fields{
		"devicePath":  devicePath,
		"devicesFile": DevicesFilePath,
	}).Info("Removed device from LVM devices file")
	return nil
}
