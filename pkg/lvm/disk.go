package lvm

import (
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	lhexec "github.com/longhorn/go-common-libs/exec"
	lhtypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/types/pkg/generated/enginerpc"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

const (
	// VGOpaquePrefix names the volume groups Longhorn creates from empty block
	// devices. An adopted volume group keeps the administrator's name.
	VGOpaquePrefix = "longhorn-vg-"
	// VGTag marks every volume group the local data engine created or
	// adopted; it is what makes a volume group a Longhorn disk.
	VGTag = "longhorn-local-data-engine"
	// DiskPathSeparator separates the block devices of a multi-device disk
	// path.
	DiskPathSeparator = ";"
	// DevicesFileName is persisted by the local IM under the host control path.
	DevicesFileName = "longhorn.devices"
	DevicesFilePath = "/etc/lvm/devices/" + DevicesFileName
	ThinPoolName    = "longhorn-thin-pool"

	ProvisioningModeThick = "thick"
	ProvisioningModeThin  = "thin"

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
	deviceNumbers  deviceNumbersFunc
}

func NewDisk() Disk {
	return Disk{
		executor:       NewExecutor(),
		metricsSampler: NewKernelBlockMetricsSampler(),
		deviceNumbers:  lookupDeviceNumbers,
	}
}

// deviceNumbersFunc reports the major and minor numbers of the block device a
// path names, following symlinks. found is false when nothing exists at the
// path; a path that exists but is not a block device is an error.
type deviceNumbersFunc func(devPath string) (major, minor int64, found bool, err error)

func lookupDeviceNumbers(devPath string) (int64, int64, bool, error) {
	var stat unix.Stat_t
	if err := unix.Stat(devPath, &stat); err != nil {
		if errors.Is(err, unix.ENOENT) || errors.Is(err, unix.ENOTDIR) {
			return 0, 0, false, nil
		}
		return 0, 0, false, fmt.Errorf("failed to stat device %v: %w", devPath, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFBLK {
		return 0, 0, false, fmt.Errorf("device path %v is not a block device", devPath)
	}
	return int64(unix.Major(uint64(stat.Rdev))), int64(unix.Minor(uint64(stat.Rdev))), true, nil
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

// VGNameForDiskUUID resolves the volume group a Longhorn disk UUID names. The
// disk UUID of a local data engine disk is the UUID of its volume group.
func VGNameForDiskUUID(executor lhexec.ExecuteInterface, diskUUID string) (string, error) {
	vg, err := getVGByUUID(executor, diskUUID)
	if err != nil {
		return "", err
	}
	if vg == nil {
		return "", fmt.Errorf("cannot find volume group %v", diskUUID)
	}
	return vg.Name, nil
}

// getVGByUUID returns the volume group with that UUID, or nil when none is
// visible through the devices file.
func getVGByUUID(executor lhexec.ExecuteInterface, vgUUID string) (*lvmVG, error) {
	vgs, err := queryVGs(executor, "vg_uuid="+vgUUID)
	if err != nil {
		return nil, err
	}
	switch len(vgs) {
	case 0:
		return nil, nil
	case 1:
		return &vgs[0], nil
	default:
		return nil, fmt.Errorf("volume group UUID %v matched several volume groups", vgUUID)
	}
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
			return pool, growThinPool(executor, pool)
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

	vg, err := requireVG(executor, vgName)
	if err != nil {
		return nil, err
	}
	poolDataSize := ((vg.Free - ThinPoolReserveBytes) / vg.ExtentSize) * vg.ExtentSize
	if poolDataSize <= 0 {
		return nil, fmt.Errorf("volume group %v has %v bytes free, not enough to retain the %v-byte thin-pool reserve", vgName, vg.Free, ThinPoolReserveBytes)
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

// diskPath is a parsed local data engine disk path: either block devices that
// form, or are to form, one volume group, or the name of a volume group.
type diskPath struct {
	devices []diskDevice
	vgName  string
}

type diskDevice struct {
	path  string
	major int64
	minor int64
}

func (dp *diskPath) String() string {
	if dp.vgName != "" {
		return dp.vgName
	}
	return joinDevicePaths(dp.devices)
}

func (dp *diskPath) devicePaths() []string {
	paths := make([]string, 0, len(dp.devices))
	for _, device := range dp.devices {
		paths = append(paths, device.path)
	}
	return paths
}

// parseDiskPath accepts `dev[;dev...]`, where every element is a block
// device, or a single volume group name given as `vg` or `/dev/vg`.
func (ops Disk) parseDiskPath(path string) (*diskPath, error) {
	var elements []string
	for _, element := range strings.Split(path, DiskPathSeparator) {
		if element = strings.TrimSpace(element); element != "" {
			elements = append(elements, element)
		}
	}
	if len(elements) == 0 {
		return nil, errors.New("disk path is required")
	}

	dp := &diskPath{}
	for _, element := range elements {
		major, minor, found, err := ops.deviceNumbers(element)
		if err != nil {
			return nil, err
		}
		if found {
			dp.devices = append(dp.devices, diskDevice{path: element, major: major, minor: minor})
			continue
		}
		if len(elements) > 1 {
			return nil, fmt.Errorf("%v in disk path %v is not a block device; a multi-device path lists block devices only", element, path)
		}
		name := strings.TrimPrefix(element, "/dev/")
		if name == "" || strings.ContainsAny(name, "/ \t") {
			return nil, fmt.Errorf("disk path %v is neither a block device nor a volume group name", path)
		}
		dp.vgName = name
	}
	return dp, nil
}

// lvmVG is the vgs report of one volume group.
type lvmVG struct {
	Name       string
	UUID       string
	Tags       []string
	LVCount    int
	Size       int64
	Free       int64
	ExtentSize int64
}

// lvmVGFields is the vgs report queryVGs parses; parseVolumeGroup follows its
// order.
const lvmVGFields = "vg_name,vg_uuid,vg_tags,lv_count,vg_size,vg_free,vg_extent_size"

// managedByLonghorn reports whether the local data engine owns the volume
// group, which it marks with a tag.
func (vg *lvmVG) managedByLonghorn() bool {
	return slices.Contains(vg.Tags, VGTag)
}

// createdUntagged reports a volume group the local data engine created before
// it tagged them; it is tagged when its disk is registered again.
func (vg *lvmVG) createdUntagged() bool {
	return !vg.managedByLonghorn() && strings.HasPrefix(vg.Name, VGOpaquePrefix)
}

// growableSize is the whole number of extents the thin pool can still take
// from the volume group while leaving the fixed reserve; negative when the
// group is already short of the reserve.
func (vg *lvmVG) growableSize() int64 {
	return ((vg.Free - ThinPoolReserveBytes) / vg.ExtentSize) * vg.ExtentSize
}

func queryVGs(executor lhexec.ExecuteInterface, selection string) ([]lvmVG, error) {
	args := []string{"--noheadings", "--units", "b", "--nosuffix", "--separator", ";"}
	if selection != "" {
		args = append(args, "--select", selection)
	}
	args = append(args, "-o", lvmVGFields)
	output, err := executor.Execute(nil, "vgs", CommandArgs(args...), lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to list volume groups: %w", err)
	}
	vgs := []lvmVG{}
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		vg, err := parseVolumeGroup(line)
		if err != nil {
			return nil, err
		}
		vgs = append(vgs, *vg)
	}
	return vgs, nil
}

func parseVolumeGroup(output string) (*lvmVG, error) {
	fields := strings.Split(strings.TrimSpace(output), ";")
	if len(fields) != 7 {
		return nil, fmt.Errorf("unexpected volume-group output: %v", output)
	}
	for i := range fields {
		fields[i] = strings.TrimSpace(fields[i])
	}
	vg := &lvmVG{Name: fields[0], UUID: fields[1], Tags: parseLVMTags(fields[2])}
	var err error
	if vg.LVCount, err = strconv.Atoi(fields[3]); err != nil {
		return nil, fmt.Errorf("failed to parse the logical volume count of volume group %v: %w", vg.Name, err)
	}
	if vg.Size, err = strconv.ParseInt(fields[4], 10, 64); err != nil {
		return nil, fmt.Errorf("failed to parse the size of volume group %v: %w", vg.Name, err)
	}
	if vg.Free, err = strconv.ParseInt(fields[5], 10, 64); err != nil {
		return nil, fmt.Errorf("failed to parse the free size of volume group %v: %w", vg.Name, err)
	}
	if vg.ExtentSize, err = strconv.ParseInt(fields[6], 10, 64); err != nil || vg.ExtentSize <= 0 {
		return nil, fmt.Errorf("failed to parse the extent size of volume group %v: %v", vg.Name, fields[6])
	}
	return vg, nil
}

// getVG returns the volume group of that name, or nil when none is visible
// through the devices file.
func getVG(executor lhexec.ExecuteInterface, name string) (*lvmVG, error) {
	vgs, err := queryVGs(executor, "vg_name="+name)
	if err != nil {
		return nil, err
	}
	if len(vgs) == 0 {
		return nil, nil
	}
	return &vgs[0], nil
}

// requireVG returns the volume group of that name, or an error when it is not
// visible through the devices file.
func requireVG(executor lhexec.ExecuteInterface, name string) (*lvmVG, error) {
	vg, err := getVG(executor, name)
	if err != nil {
		return nil, err
	}
	if vg == nil {
		return nil, fmt.Errorf("cannot find volume group %v", name)
	}
	return vg, nil
}

// resolveVG finds the volume group a disk path refers to: the one the block
// devices' physical volumes belong to, or the named one. It returns nil when
// the path refers to no volume group yet.
func (ops Disk) resolveVG(dp *diskPath) (*lvmVG, error) {
	if dp.vgName != "" {
		return getVG(ops.executor, dp.vgName)
	}
	vg, _, err := ops.resolveDisk(dp)
	return vg, err
}

// resolveDisk is resolveVG that also returns the physical volumes of the
// volume group it found.
func (ops Disk) resolveDisk(dp *diskPath) (*lvmVG, []lvmPV, error) {
	if dp.vgName != "" {
		vg, err := getVG(ops.executor, dp.vgName)
		if err != nil || vg == nil {
			return nil, nil, err
		}
		pvs, err := ops.listVGPhysicalVolumes(vg.Name)
		return vg, pvs, err
	}
	pvs, err := ops.queryPhysicalVolumes("")
	if err != nil {
		return nil, nil, err
	}
	vgName, err := commonVGName(pvs, dp.devices)
	if err != nil || vgName == "" {
		return nil, nil, err
	}
	vg, err := getVG(ops.executor, vgName)
	if err != nil || vg == nil {
		return nil, nil, err
	}
	members := []lvmPV{}
	for _, pv := range pvs {
		if pv.VGName == vgName {
			members = append(members, pv)
		}
	}
	return vg, members, nil
}

// commonVGName is the volume group the devices' physical volumes belong to,
// empty when none of them is in a volume group; devices in different groups
// are an error.
func commonVGName(pvs []lvmPV, devices []diskDevice) (string, error) {
	vgName := ""
	for _, device := range devices {
		pv := findPV(pvs, device)
		if pv == nil || pv.VGName == "" {
			continue
		}
		if vgName != "" && vgName != pv.VGName {
			return "", fmt.Errorf("devices %v belong to different volume groups %v and %v", joinDevicePaths(devices), vgName, pv.VGName)
		}
		vgName = pv.VGName
	}
	return vgName, nil
}

func joinDevicePaths(devices []diskDevice) string {
	paths := make([]string, 0, len(devices))
	for _, device := range devices {
		paths = append(paths, device.path)
	}
	return strings.Join(paths, DiskPathSeparator)
}

func (ops Disk) Create(req *rpc.DiskCreateRequest) (*rpc.Disk, error) {
	dp, err := ops.parseDiskPath(req.DiskPath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, err.Error())
	}

	var vg *lvmVG
	var existed bool
	if dp.vgName != "" {
		vg, existed, err = ops.adoptVolumeGroup(dp.vgName)
	} else {
		vg, existed, err = ops.createVolumeGroup(dp.devices)
	}
	if err != nil {
		return nil, err
	}
	if existed {
		// A disk registered again after the node resource was lost still holds
		// the volumes of attached engines; bring them back as a restart does.
		if err := ActivateAttachedLogicalVolumes(ops.executor); err != nil {
			logrus.WithError(err).WithField("vgName", vg.Name).Warn("Failed to reactivate every attached local replica of a re-registered disk")
		}
	}

	diskInfo, err := ops.getDiskInfo(vg, req.DiskName, req.DiskPath)
	if err == nil {
		logrus.WithFields(logrus.Fields{
			"adopted":  dp.vgName != "",
			"diskName": req.DiskName,
			"diskPath": req.DiskPath,
			"existed":  existed,
			"vgName":   vg.Name,
			"vgUUID":   vg.UUID,
		}).Info("Registered LVM disk")
	}
	return diskInfo, err
}

// createVolumeGroup turns empty block devices into one tagged volume group.
// Devices that already form one Longhorn volume group are returned as they
// are, so registration is idempotent. On a partial failure the physical
// volumes and devices-file entries this call added are removed again.
func (ops Disk) createVolumeGroup(devices []diskDevice) (vg *lvmVG, existed bool, err error) {
	paths := strings.Split(joinDevicePaths(devices), DiskPathSeparator)
	// Only what this call changed is undone when it fails: devices already
	// listed in the devices file may back a volume group that is in use.
	var added, created []string
	rollback := func() {
		for _, path := range created {
			if _, err := ops.executor.Execute(nil, "pvremove", CommandArgs("-y", path), lhtypes.ExecuteDefaultTimeout); err != nil {
				logrus.WithError(err).Warnf("Failed to remove the physical volume label from %v while undoing disk creation", path)
			}
		}
		ops.forgetDevices(added)
	}
	for _, path := range paths {
		newlyAdded, err := ops.addDevice(path)
		if err != nil {
			rollback()
			return nil, false, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		if newlyAdded {
			added = append(added, path)
		}
	}

	pvs, err := ops.queryPhysicalVolumes("")
	if err != nil {
		rollback()
		return nil, false, grpcstatus.Errorf(grpccodes.Internal, "failed to inspect the physical volumes of %v: %v", joinDevicePaths(devices), err)
	}
	vgName, err := commonVGName(pvs, devices)
	if err != nil {
		rollback()
		return nil, false, grpcstatus.Error(grpccodes.FailedPrecondition, err.Error())
	}
	var toCreate, members []string
	for _, device := range devices {
		switch pv := findPV(pvs, device); {
		case pv == nil:
			toCreate = append(toCreate, device.path)
		case pv.VGName == "":
			// A physical volume left behind by an interrupted creation; it is
			// reused instead of being labelled again.
		default:
			members = append(members, device.path)
		}
	}

	if vgName != "" {
		// Devices that back the volume group are never dropped from the
		// devices file, whether or not this call listed them.
		added = slices.DeleteFunc(added, func(path string) bool { return !slices.Contains(toCreate, path) })
		existing, err := requireVG(ops.executor, vgName)
		if err != nil {
			rollback()
			return nil, false, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		if len(members) != len(devices) {
			rollback()
			return nil, false, grpcstatus.Errorf(grpccodes.FailedPrecondition, "devices %v already belong to volume group %v, but %v do not; devices cannot be added to a registered volume group", strings.Join(members, DiskPathSeparator), vgName, strings.Join(toCreate, DiskPathSeparator))
		}
		if existing.createdUntagged() {
			if _, err := ops.executor.Execute(nil, "vgchange", CommandArgs("--addtag", VGTag, vgName), lhtypes.ExecuteDefaultTimeout); err != nil {
				return nil, false, grpcstatus.Errorf(grpccodes.Internal, "failed to tag volume group %v: %v", vgName, err)
			}
			existing.Tags = append(existing.Tags, VGTag)
			logrus.WithField("vgName", vgName).Info("Tagged a volume group created before volume groups were tagged")
		}
		if !existing.managedByLonghorn() {
			// Keeping the devices listed would make the foreign volume group
			// and its logical volumes visible to every later LVM command.
			ops.forgetDevices(paths)
			return nil, false, grpcstatus.Errorf(grpccodes.FailedPrecondition, "devices %v belong to volume group %v, which Longhorn does not manage", joinDevicePaths(devices), vgName)
		}
		return existing, true, nil
	}

	// Nothing on these devices belongs to a volume group yet. Refuse any
	// device that is not empty before touching the others.
	for _, path := range toCreate {
		if err := ops.verifyDeviceEmpty(path); err != nil {
			rollback()
			return nil, false, grpcstatus.Error(grpccodes.InvalidArgument, err.Error())
		}
	}
	for _, path := range toCreate {
		if _, err := ops.executor.Execute(nil, "pvcreate", CommandArgs(path), lhtypes.ExecuteDefaultTimeout); err != nil {
			rollback()
			return nil, false, grpcstatus.Errorf(grpccodes.Internal, "failed to create physical volume on %v: %v", path, err)
		}
		created = append(created, path)
	}

	vgName = newVGName()
	args := []string{"--addtag", VGTag, vgName}
	if _, err := ops.executor.Execute(nil, "vgcreate", CommandArgs(append(args, paths...)...), lhtypes.ExecuteDefaultTimeout); err != nil {
		rollback()
		return nil, false, grpcstatus.Errorf(grpccodes.Internal, "failed to create volume group %v on %v: %v", vgName, joinDevicePaths(devices), err)
	}
	vg, err = requireVG(ops.executor, vgName)
	if err != nil {
		return nil, false, grpcstatus.Errorf(grpccodes.Internal, "volume group %v is absent after creation: %v", vgName, err)
	}
	logrus.WithFields(logrus.Fields{
		"devices": paths,
		"vgName":  vgName,
	}).Info("Created volume group")
	return vg, false, nil
}

// adoptVolumeGroup takes over an existing volume group that holds no logical
// volumes, importing its devices into the devices file and tagging it. A
// volume group Longhorn already manages is returned as it is.
func (ops Disk) adoptVolumeGroup(name string) (vg *lvmVG, existed bool, err error) {
	vg, err = getVG(ops.executor, name)
	if err != nil {
		return nil, false, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	imported := false
	if vg == nil {
		if _, err := ops.executor.Execute(nil, "vgimportdevices", CommandArgs(name), lhtypes.ExecuteDefaultTimeout); err != nil {
			return nil, false, grpcstatus.Errorf(grpccodes.InvalidArgument, "disk path %v is neither a block device nor a volume group: %v", name, err)
		}
		imported = true
		vg, err = getVG(ops.executor, name)
		if err != nil {
			return nil, false, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		if vg == nil {
			return nil, false, grpcstatus.Errorf(grpccodes.InvalidArgument, "disk path %v is neither a block device nor a volume group", name)
		}
	}
	if vg.managedByLonghorn() {
		return vg, true, nil
	}

	undoImport := func() {
		if !imported {
			return
		}
		pvs, err := ops.listVGPhysicalVolumes(name)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to list the physical volumes of volume group %v while undoing its import", name)
			return
		}
		paths := make([]string, 0, len(pvs))
		for _, pv := range pvs {
			paths = append(paths, pv.Path)
		}
		ops.forgetDevices(paths)
	}

	if vg.LVCount > 0 {
		lvs, err := queryLogicalVolumes(ops.executor, "vg_name="+name)
		names := make([]string, 0, len(lvs))
		if err == nil {
			for _, lv := range lvs {
				names = append(names, lv.Name)
			}
		}
		undoImport()
		return nil, false, grpcstatus.Errorf(grpccodes.FailedPrecondition, "volume group %v contains logical volumes %v; only an empty volume group can become a local data engine disk", name, names)
	}
	if _, err := ops.executor.Execute(nil, "vgchange", CommandArgs("--addtag", VGTag, name), lhtypes.ExecuteDefaultTimeout); err != nil {
		undoImport()
		return nil, false, grpcstatus.Errorf(grpccodes.Internal, "failed to tag volume group %v: %v", name, err)
	}
	logrus.WithFields(logrus.Fields{
		"imported": imported,
		"vgName":   name,
	}).Info("Adopted volume group")
	return vg, false, nil
}

func (ops Disk) Delete(req *rpc.DiskDeleteRequest) (*emptypb.Empty, error) {
	// The UUID is the disk's identity; the path only locates a disk that was
	// registered without ever becoming ready, and may name devices that no
	// longer exist.
	vg, dp, err := ops.resolveDiskForDeletion(req)
	if err != nil {
		return nil, err
	}
	if vg == nil {
		// Nothing of the disk exists in LVM. Drop whatever the devices file
		// still lists for it, so a later registration starts clean.
		if dp != nil {
			ops.forgetDevices(dp.devicePaths())
		}
		logrus.WithFields(logrus.Fields{
			"diskName": req.DiskName,
			"diskPath": req.DiskPath,
			"diskUUID": req.DiskUuid,
		}).Info("Nothing left to remove for the LVM disk")
		return &emptypb.Empty{}, nil
	}
	if !vg.managedByLonghorn() {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "volume group %v is not managed by Longhorn, refusing to remove it", vg.Name)
	}

	lvs, err := queryLogicalVolumes(ops.executor, "vg_name="+vg.Name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	for _, lv := range lvs {
		if !lv.IsThinPool() {
			return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "cannot remove disk %v while volume group %v contains logical volume %v", req.DiskName, vg.Name, lv.Name)
		}
	}
	for _, lv := range lvs {
		// Only the managed thin pool is left, holding no thin volume any more.
		if _, err := ops.executor.Execute(nil, "lvremove", CommandArgs("-y", vg.Name+"/"+lv.Name), lhtypes.ExecuteDefaultTimeout); err != nil {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to remove empty thin pool %v/%v: %v", vg.Name, lv.Name, err)
		}
	}
	pvs, err := ops.listVGPhysicalVolumes(vg.Name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}

	if _, err := ops.executor.Execute(nil, "vgremove", CommandArgs("-y", vg.Name), lhtypes.ExecuteDefaultTimeout); err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to remove volume group %v: %v", vg.Name, err)
	}
	for _, pv := range pvs {
		if _, err := ops.executor.Execute(nil, "pvremove", CommandArgs("-y", pv.Path), lhtypes.ExecuteDefaultTimeout); err != nil {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to remove physical volume %v: %v", pv.Path, err)
		}
		if err := ops.removeDevice(pv.Path); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
	}
	logrus.WithFields(logrus.Fields{
		"diskName": req.DiskName,
		"diskPath": req.DiskPath,
		"vgName":   vg.Name,
		"vgUUID":   vg.UUID,
	}).Info("Deleted LVM disk")
	return &emptypb.Empty{}, nil
}

// resolveDiskForDeletion finds the volume group a deletion request refers to:
// by UUID when the request carries one, otherwise by path. It returns a nil
// volume group when nothing is left to remove, with the parsed path when the
// path could be parsed.
func (ops Disk) resolveDiskForDeletion(req *rpc.DiskDeleteRequest) (*lvmVG, *diskPath, error) {
	var dp *diskPath
	if req.DiskPath != "" {
		parsed, err := ops.parseDiskPath(req.DiskPath)
		if err != nil {
			if req.DiskUuid == "" {
				return nil, nil, grpcstatus.Error(grpccodes.InvalidArgument, err.Error())
			}
			logrus.WithError(err).Debugf("Ignoring the path of disk %v, which is deleted by UUID %v", req.DiskName, req.DiskUuid)
		} else {
			dp = parsed
		}
	}
	if req.DiskUuid == "" {
		if dp == nil {
			return nil, nil, nil
		}
		vg, err := ops.resolveVG(dp)
		if err != nil {
			return nil, nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		return vg, dp, nil
	}
	vg, err := getVGByUUID(ops.executor, req.DiskUuid)
	if err != nil {
		return nil, nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if vg != nil || dp == nil {
		return vg, dp, nil
	}
	// The disk's volume group is gone. Its devices may meanwhile back another
	// volume group, which must not lose its devices-file entries.
	other, err := ops.resolveVG(dp)
	if err != nil {
		return nil, nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if other != nil {
		return nil, nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "disk %v has UUID %v but its path %v belongs to volume group %v (%v)", req.DiskName, req.DiskUuid, req.DiskPath, other.Name, other.UUID)
	}
	return nil, dp, nil
}

func (ops Disk) Get(req *rpc.DiskGetRequest) (*rpc.Disk, error) {
	dp, err := ops.parseDiskPath(req.DiskPath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, err.Error())
	}
	vg, err := ops.resolveVG(dp)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if vg == nil || !vg.managedByLonghorn() {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find a managed volume group for %v", req.DiskPath)
	}
	return ops.getDiskInfo(vg, req.DiskName, req.DiskPath)
}

// Metrics reports the I/O of the disk's volume group as the sum over its
// physical volumes' block devices.
func (ops Disk) Metrics(req *rpc.DiskGetRequest) (*rpc.DiskMetricsGetReply, error) {
	dp, err := ops.parseDiskPath(req.DiskPath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, err.Error())
	}
	vg, pvs, err := ops.resolveDisk(dp)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if vg == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find a volume group for %v", req.DiskPath)
	}
	total := &enginerpc.Metrics{}
	var readLatency, writeLatency latencyAverage
	sampled := 0
	for _, pv := range pvs {
		metrics, err := ops.metricsSampler.Sample("disk:"+vg.UUID+":"+pv.Path, pv.Path)
		if err != nil {
			// A physical volume whose device is gone must not hide the others.
			logrus.WithError(err).Warnf("Skipping the metrics of physical volume %v of volume group %v", pv.Path, vg.Name)
			continue
		}
		sampled++
		total.ReadThroughput += metrics.ReadThroughput
		total.WriteThroughput += metrics.WriteThroughput
		total.ReadIOPS += metrics.ReadIOPS
		total.WriteIOPS += metrics.WriteIOPS
		readLatency.add(metrics.ReadLatency, metrics.ReadIOPS)
		writeLatency.add(metrics.WriteLatency, metrics.WriteIOPS)
	}
	if sampled == 0 && len(pvs) > 0 {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "none of the physical volumes of volume group %v could be sampled", vg.Name)
	}
	total.ReadLatency = readLatency.value()
	total.WriteLatency = writeLatency.value()
	return &rpc.DiskMetricsGetReply{Metrics: total}, nil
}

// latencyAverage averages the latencies of several devices, weighted by their
// operation rates; when every rate rounds to zero it falls back to the plain
// average of the latencies seen, so a slow idle disk still reports one.
type latencyAverage struct {
	weighted, weight, plain, samples uint64
}

func (a *latencyAverage) add(latency, ops uint64) {
	if latency == 0 {
		return
	}
	a.weighted += latency * ops
	a.weight += ops
	a.plain += latency
	a.samples++
}

func (a *latencyAverage) value() uint64 {
	if a.weight > 0 {
		return a.weighted / a.weight
	}
	if a.samples > 0 {
		return a.plain / a.samples
	}
	return 0
}

// verifyDeviceEmpty rejects devices that carry an existing filesystem,
// partition table, or other signature.
func (ops Disk) verifyDeviceEmpty(devPath string) error {
	if _, _, found, err := ops.deviceNumbers(devPath); err != nil {
		return err
	} else if !found {
		return fmt.Errorf("device %v does not exist", devPath)
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
	Path   string
	VGName string
	// Major and Minor are the device numbers LVM reports for the PV; -1 when
	// the PV's device is missing.
	Major int64
	Minor int64
}

// lvmPVFields is the pvs report the PV helpers parse; parsePhysicalVolume
// follows its order.
const lvmPVFields = "pv_name,vg_name,pv_major,pv_minor"

// findPV returns the PV on a device, matched by device numbers: the caller's
// path may be an alias such as /dev/disk/by-id/..., while LVM reports pv_name
// as its preferred name (usually the kernel name).
func findPV(pvs []lvmPV, device diskDevice) *lvmPV {
	for i := range pvs {
		if pvs[i].Major == device.major && pvs[i].Minor == device.minor {
			return &pvs[i]
		}
	}
	return nil
}

func parsePhysicalVolume(output string) (*lvmPV, error) {
	fields := strings.Split(strings.TrimSpace(output), ";")
	if len(fields) != 4 {
		return nil, fmt.Errorf("unexpected physical-volume output: %v", output)
	}
	return &lvmPV{
		Path:   strings.TrimSpace(fields[0]),
		VGName: strings.TrimSpace(fields[1]),
		Major:  parseDeviceNumber(fields[2]),
		Minor:  parseDeviceNumber(fields[3]),
	}, nil
}

// parseDeviceNumber returns -1 for the blank or negative number LVM reports
// when a PV's device is missing, so such a PV never matches a real device.
func parseDeviceNumber(field string) int64 {
	number, err := strconv.ParseInt(strings.TrimSpace(field), 10, 64)
	if err != nil || number < 0 {
		return -1
	}
	return number
}

// queryPhysicalVolumes lists the PVs visible through the devices file, all of
// them when selection is empty.
func (ops Disk) queryPhysicalVolumes(selection string) ([]lvmPV, error) {
	args := []string{"--noheadings", "--separator", ";"}
	if selection != "" {
		args = append(args, "--select", selection)
	}
	args = append(args, "-o", lvmPVFields)
	output, err := ops.executor.Execute(nil, "pvs", CommandArgs(args...), lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return nil, err
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

func (ops Disk) listVGPhysicalVolumes(vgName string) ([]lvmPV, error) {
	pvs, err := ops.queryPhysicalVolumes("vg_name=" + vgName)
	if err != nil {
		return nil, fmt.Errorf("failed to list physical volumes in volume group %v: %w", vgName, err)
	}
	return pvs, nil
}

// growThinPool extends the managed thin pool into the capacity of the volume
// group beyond the fixed reserve, for instance after the administrator
// extended an adopted volume group, and updates the pool's size.
func growThinPool(executor lhexec.ExecuteInterface, pool *LogicalVolume) error {
	vg, err := requireVG(executor, pool.VGName)
	if err != nil {
		return err
	}
	extendSize := vg.growableSize()
	if extendSize <= 0 {
		return nil
	}
	if _, err := executor.Execute(nil, "lvextend", CommandArgs(
		"-y", "-L", fmt.Sprintf("+%vb", extendSize), pool.VGName+"/"+pool.Name), lhtypes.ExecuteDefaultTimeout); err != nil {
		return fmt.Errorf("failed to extend thin pool %v/%v by %v bytes: %w", pool.VGName, pool.Name, extendSize, err)
	}
	pool.Size += extendSize
	logrus.WithFields(logrus.Fields{
		"extendSize": extendSize,
		"vgName":     pool.VGName,
	}).Info("Extended local data engine thin pool into new volume group capacity")
	return nil
}

// getDiskInfo reports a volume group as a disk. Capacity the administrator
// added to the group is reported as part of the thin pool; the pool itself
// grows into it when the next thin volume is created.
func (ops Disk) getDiskInfo(vg *lvmVG, diskName, diskPath string) (*rpc.Disk, error) {
	pool, err := GetLogicalVolume(ops.executor, vg.Name, ThinPoolName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to inspect thin pool in volume group %v: %v", vg.Name, err)
	}

	totalSize, freeSize := vg.Size, vg.Free
	if pool != nil {
		if !pool.IsThinPool() {
			return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "logical volume %v/%v is not the expected thin pool", vg.Name, ThinPoolName)
		}
		growable := max(vg.growableSize(), 0)
		totalSize = pool.Size + growable
		allocatedSize := int64(float64(pool.Size) * pool.DataUsagePercentage / 100)
		freeSize = max(totalSize-allocatedSize, 0)
		// TODO: Monitor thin-pool metadata usage and either stop scheduling near
		// exhaustion or expand metadata from the VG capacity reserved outside the pool.
	}

	return &rpc.Disk{
		Id:          vg.UUID,
		Uuid:        vg.UUID,
		Name:        diskName,
		Path:        diskPath,
		Type:        rpc.DiskType_lvm.String(),
		TotalSize:   totalSize,
		FreeSize:    freeSize,
		TotalBlocks: totalSize / vg.ExtentSize,
		FreeBlocks:  freeSize / vg.ExtentSize,
		BlockSize:   vg.ExtentSize,
		State:       lvmDiskStateReady,
	}, nil
}

// addDevice adds a device to the devices file; a device already listed there
// is left as it is.
func (ops Disk) addDevice(devicePath string) (added bool, err error) {
	if _, err := ops.executor.Execute(nil, "lvmdevices", CommandArgs("--adddev", devicePath), lhtypes.ExecuteDefaultTimeout); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "already") {
			return false, nil
		}
		return false, fmt.Errorf("failed to add device %v to LVM devices file %v: %v", devicePath, DevicesFileName, err)
	}
	logrus.WithFields(logrus.Fields{
		"devicePath":  devicePath,
		"devicesFile": DevicesFilePath,
	}).Info("Added device to LVM devices file")
	return true, nil
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

// forgetDevices removes devices from the devices file while undoing a failed
// or empty registration; a device that is not listed is not an error here.
func (ops Disk) forgetDevices(devicePaths []string) {
	for _, devicePath := range devicePaths {
		if err := ops.removeDevice(devicePath); err != nil {
			logrus.WithError(err).Debugf("Device %v was not removed from the LVM devices file", devicePath)
		}
	}
}
