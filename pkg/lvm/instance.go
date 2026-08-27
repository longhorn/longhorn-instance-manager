package lvm

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	lhexec "github.com/longhorn/go-common-libs/exec"
	lhtypes "github.com/longhorn/go-common-libs/types"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

// lvmEngineTagPrefix tags a replica LV with the engine instance attached to
// it, so that all local instance state survives an instance manager restart.
const lvmEngineTagPrefix = "longhorn-engine="

// Instance manages local data engine instances. A replica
// instance is an active LVM LV inside the disk's VG; an engine instance is
// an attachment record, not a process: an LVM tag "longhorn-engine=<name>" on
// the replica LV whose endpoint is the LV device path. There is no data-path
// process.
//
// An engine must be a named, monitorable, independently-revocable state
// entity, stored in a medium whose lifetime matches the data path's — the
// process table for v1, spdk_tgt memory for v2, LVM metadata here. The local
// data path is a kernel dm device that survives instance manager restarts, so
// engine state must too, or InstanceList would report a healthy attachment as
// dead. It must be a state slot separate from LV activation because detach
// stops the engine first and the replica only after the engine reports
// stopped; deriving one from the other deadlocks detach.
type Instance struct {
	executor lhexec.ExecuteInterface
	// deviceStatus is replaceable by unit tests. Production uses it only after
	// activation to ensure the host has exposed a usable block device.
	deviceStatus func(string) (exists, blockDevice bool, err error)
	// notify signals the instance watch stream after a successful state
	// change; LVM has no daemon to emit events, so the ops report their own.
	notify func()
}

func NewInstance(notify func()) Instance {
	return Instance{executor: NewExecutor(), notify: notify}
}

func (ops Instance) notifyChanged() {
	if ops.notify != nil {
		ops.notify()
	}
}

func (ops Instance) Create(req *rpc.InstanceCreateRequest) (*rpc.InstanceResponse, error) {
	spec := req.Spec.LocalInstanceSpec

	switch req.Spec.Type {
	case types.InstanceTypeReplica:
		if spec == nil || spec.DiskName == "" || spec.DiskUuid == "" || spec.Size == 0 {
			return nil, grpcstatus.Error(grpccodes.InvalidArgument, "disk name, disk UUID and size are required for local data engine replica creation")
		}
		mode := spec.ProvisioningMode
		if mode == "" {
			mode = ProvisioningModeThick
		}
		return ops.replicaCreate(req.Spec.Name, spec.DiskUuid, int64(spec.Size), mode)
	case types.InstanceTypeEngine:
		if spec == nil || spec.ReplicaName == "" {
			return nil, grpcstatus.Error(grpccodes.InvalidArgument, "replica name is required for local data engine engine creation")
		}
		return ops.engineCreate(req.Spec.Name, spec.ReplicaName)
	default:
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "local data engine instance type %v is not supported", req.Spec.Type)
	}
}

func (ops Instance) replicaCreate(name, diskUUID string, size int64, mode string) (*rpc.InstanceResponse, error) {
	if !IsProvisioningMode(mode) {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "invalid local provisioning mode %q", mode)
	}
	vgName, err := VGNameForPVUUID(ops.executor, diskUUID)
	if err != nil {
		if grpcstatus.Code(err) != grpccodes.Unknown {
			return nil, err
		}
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}

	lv, err := ops.findLV(name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if lv == nil {
		// With udev disabled by CommandArgs, lvcreate synchronously creates the
		// device node and clears signatures before returning.
		args := []string{"-y", "-n", name}
		if IsThinProvisioningMode(mode) {
			if _, err := GetOrCreateThinPool(ops.executor, vgName); err != nil {
				return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
			}
			// LVM does not support --setautoactivation for thin volumes.
			args = append(args, "-V", fmt.Sprintf("%vb", size), "-T", vgName+"/"+ThinPoolName)
		} else {
			// Keep host boot-time autoactivation away from thick Longhorn LVs:
			// detached replicas must not appear running after a node reboot.
			args = append(args, "--setautoactivation", "n")
			pool, err := GetLogicalVolume(ops.executor, vgName, ThinPoolName)
			if err != nil {
				return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
			}
			if pool != nil {
				if err := RemoveThinPoolIfUnused(ops.executor, vgName); err != nil {
					return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
				}
				pool, err = GetLogicalVolume(ops.executor, vgName, ThinPoolName)
				if err != nil {
					return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
				}
				if pool != nil {
					return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "cannot create thick logical volume while thin pool %v/%v contains thin volumes", vgName, ThinPoolName)
				}
			}
			args = append(args, "-L", fmt.Sprintf("%vb", size), vgName)
		}
		if _, err := ops.executor.Execute(nil, "lvcreate",
			CommandArgs(args...), lhtypes.ExecuteDefaultTimeout); err != nil {
			if IsThinProvisioningMode(mode) {
				if cleanupErr := RemoveThinPoolIfUnused(ops.executor, vgName); cleanupErr != nil {
					return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to create logical volume %v/%v: %v; failed to remove the empty thin pool: %v", vgName, name, err, cleanupErr)
				}
			}
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to create logical volume %v/%v: %v", vgName, name, err)
		}
	} else if lv.VGName != vgName {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "logical volume %v already exists in volume group %v", name, lv.VGName)
	} else {
		if IsThinProvisioningMode(mode) {
			if !lv.IsThinVolume() {
				return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "logical volume %v/%v is thick, not %v", vgName, name, mode)
			}
			if _, err := GetOrCreateThinPool(ops.executor, vgName); err != nil {
				return nil, grpcstatus.Error(grpccodes.FailedPrecondition, err.Error())
			}
		} else if lv.IsThinVolume() {
			return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "logical volume %v/%v is thin, not %v", vgName, name, mode)
		}
		if !lv.Active {
			if err := ops.activateLV(vgName, name); err != nil {
				return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
			}
		}
	}
	readyLV, err := ops.confirmLV(vgName, name, true, size, true)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	ops.notifyChanged()
	logrus.WithFields(logrus.Fields{
		"devicePath":  readyLV.Path,
		"replicaName": name,
		"size":        readyLV.Size,
		"vgName":      readyLV.VGName,
	}).Info("Started local replica")

	return localReplicaInstanceResponse(name, types.ProcessStateRunning), nil
}

func (ops Instance) engineCreate(name, replicaName string) (*rpc.InstanceResponse, error) {
	// The engine attachment must be globally unique: a stale record on another
	// LV would make InstanceList report two engines under the same name,
	// racing over the endpoint.
	attached, err := ops.findEngineAttachment(name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if attached != nil && attached.Name != replicaName {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "engine %v is already attached to replica logical volume %v", name, attached.Name)
	}

	lv, err := ops.findLV(replicaName)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if lv == nil {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "cannot find replica logical volume %v for engine %v", replicaName, name)
	}
	engineName := localLVEngineName(lv)
	if engineName != "" && engineName != name {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "replica logical volume %v is already attached to engine %v", replicaName, engineName)
	}
	if !lv.Active {
		if err := ops.activateLV(lv.VGName, lv.Name); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
	}
	lv, err = ops.confirmLV(lv.VGName, lv.Name, true, lv.Size, true)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if engineName == "" {
		if err := ops.markEngineAttached(lv, name); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
	}
	ops.notifyChanged()
	logrus.WithFields(logrus.Fields{
		"devicePath":  lv.Path,
		"engineName":  name,
		"replicaName": replicaName,
	}).Info("Started local engine")

	return localEngineInstanceResponse(name, types.ProcessStateRunning, lv.Path), nil
}

func (ops Instance) Delete(req *rpc.InstanceDeleteRequest) (*rpc.InstanceResponse, error) {
	// Replica instance: the LV itself.
	lv, err := ops.findLV(req.Name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if lv != nil {
		if req.DiskUuid != "" {
			vgName, err := VGNameForPVUUID(ops.executor, req.DiskUuid)
			if err != nil {
				if grpcstatus.Code(err) != grpccodes.Unknown {
					return nil, err
				}
				return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
			}
			if vgName != lv.VGName {
				return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "logical volume %v belongs to volume group %v, not the volume group %v for disk UUID %v", req.Name, lv.VGName, vgName, req.DiskUuid)
			}
		}
		if req.CleanupRequired {
			if _, err := ops.executor.Execute(nil, "lvremove", CommandArgs("-y", lv.VGName+"/"+lv.Name), lhtypes.ExecuteDefaultTimeout); err != nil {
				remaining, inspectErr := ops.getLV(lv.VGName, lv.Name)
				if inspectErr != nil || remaining != nil {
					return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to remove logical volume %v/%v: %v", lv.VGName, lv.Name, err)
				}
			}
			if err := ops.confirmLVAbsent(lv.VGName, lv.Name); err != nil {
				return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
			}
			if lv.IsThinVolume() {
				if err := RemoveThinPoolIfUnused(ops.executor, lv.VGName); err != nil {
					return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
				}
			}
		} else {
			// Deactivate only; the data is preserved for the next attach.
			if _, err := ops.executor.Execute(nil, "lvchange", CommandArgs("-an", lv.VGName+"/"+lv.Name), lhtypes.ExecuteDefaultTimeout); err != nil {
				remaining, inspectErr := ops.getLV(lv.VGName, lv.Name)
				if inspectErr != nil || remaining == nil || remaining.Active {
					return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to deactivate logical volume %v/%v: %v", lv.VGName, lv.Name, err)
				}
			}
			if _, err := ops.confirmLV(lv.VGName, lv.Name, false, 0, false); err != nil {
				return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
			}
		}
		ops.notifyChanged()
		log := logrus.WithFields(logrus.Fields{
			"replicaName": req.Name,
			"vgName":      lv.VGName,
		})
		if req.CleanupRequired {
			log.Info("Deleted local replica")
		} else {
			log.Info("Stopped local replica")
		}
		return localReplicaInstanceResponse(req.Name, types.ProcessStateStopped), nil
	}

	// Engine instance: the engine attachment record on a replica LV.
	lv, err = ops.findEngineAttachment(req.Name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if lv != nil {
		if err := ops.clearEngineAttachment(lv, req.Name); err != nil {
			return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
		}
		ops.notifyChanged()
		logrus.WithFields(logrus.Fields{
			"engineName":  req.Name,
			"replicaName": lv.Name,
		}).Info("Stopped local engine")
		return localEngineInstanceResponse(req.Name, types.ProcessStateStopped, ""), nil
	}

	// Nothing found: the instance is already gone.
	return localReplicaInstanceResponse(req.Name, types.ProcessStateStopped), nil
}

func (ops Instance) Get(req *rpc.InstanceGetRequest) (*rpc.InstanceResponse, error) {
	lv, err := ops.findLV(req.Name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if lv != nil && lv.Active {
		return localReplicaInstanceResponse(lv.Name, types.ProcessStateRunning), nil
	}

	lv, err = ops.findEngineAttachment(req.Name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if lv != nil && lv.Active {
		return localEngineInstanceResponse(req.Name, types.ProcessStateRunning, lv.Path), nil
	}

	return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find local data engine instance %v", req.Name)
}

func (ops Instance) List(instances map[string]*rpc.InstanceResponse) error {
	lvs, err := ops.listLVs()
	if err != nil {
		return grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	for _, lv := range lvs {
		if !lv.Active || lv.IsThinPool() {
			continue
		}
		instances[lv.Name] = localReplicaInstanceResponse(lv.Name, types.ProcessStateRunning)
		if engineName := localLVEngineName(&lv); engineName != "" {
			instances[engineName] = localEngineInstanceResponse(engineName, types.ProcessStateRunning, lv.Path)
		}
	}
	return nil
}

func (ops Instance) Replace(req *rpc.InstanceReplaceRequest) (*rpc.InstanceResponse, error) {
	if req.Spec == nil || req.Spec.Type != types.InstanceTypeReplica || req.Spec.LocalInstanceSpec == nil {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "a local replica specification is required for local data engine instance replacement")
	}
	spec := req.Spec.LocalInstanceSpec
	if req.Spec.Name == "" || spec.DiskName == "" || spec.DiskUuid == "" || spec.Size == 0 {
		return nil, grpcstatus.Error(grpccodes.InvalidArgument, "name, disk name, disk UUID and size are required for local data engine replica expansion")
	}

	vgName, err := VGNameForPVUUID(ops.executor, spec.DiskUuid)
	if err != nil {
		if grpcstatus.Code(err) != grpccodes.Unknown {
			return nil, err
		}
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}

	lv, err := ops.findLV(req.Spec.Name)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	if lv == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find replica logical volume %v", req.Spec.Name)
	}
	if lv.VGName != vgName {
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "logical volume %v belongs to volume group %v, not %v", lv.Name, lv.VGName, vgName)
	}
	targetSize := int64(spec.Size)
	if targetSize < lv.Size {
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "cannot shrink logical volume %v/%v from %v to %v bytes", vgName, lv.Name, lv.Size, targetSize)
	}
	if targetSize > lv.Size {
		if _, err := ops.executor.Execute(nil, "lvextend",
			CommandArgs("-y", "-L", fmt.Sprintf("%vb", targetSize), vgName+"/"+lv.Name), lhtypes.ExecuteDefaultTimeout); err != nil {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to expand logical volume %v/%v to %v bytes: %v", vgName, lv.Name, targetSize, err)
		}
	}

	resizedLV, err := ops.confirmLV(vgName, lv.Name, lv.Active, targetSize, lv.Active)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	ops.notifyChanged()
	logrus.WithFields(logrus.Fields{
		"replicaName": resizedLV.Name,
		"size":        resizedLV.Size,
		"vgName":      resizedLV.VGName,
	}).Info("Expanded local replica")
	state := types.ProcessStateStopped
	if resizedLV.Active {
		state = types.ProcessStateRunning
	}
	return localReplicaInstanceResponse(resizedLV.Name, state), nil
}

// listLVs returns all LVs in volume groups managed by the local data engine.
func (ops Instance) listLVs() ([]LogicalVolume, error) {
	return ListLogicalVolumes(ops.executor)
}

// getLV queries one LV without turning its absence into an LVM command error.
func (ops Instance) getLV(vgName, lvName string) (*LogicalVolume, error) {
	return GetLogicalVolume(ops.executor, vgName, lvName)
}

func localLVEngineName(lv *LogicalVolume) string {
	for _, tag := range lv.Tags {
		if strings.HasPrefix(tag, lvmEngineTagPrefix) {
			return strings.TrimPrefix(tag, lvmEngineTagPrefix)
		}
	}
	return ""
}

func (ops Instance) activateLV(vgName, lvName string) error {
	if _, err := ops.executor.Execute(nil, "lvchange", CommandArgs("-ay", vgName+"/"+lvName), lhtypes.ExecuteDefaultTimeout); err != nil {
		lv, inspectErr := ops.getLV(vgName, lvName)
		if inspectErr != nil || lv == nil || !lv.Active {
			return fmt.Errorf("failed to activate logical volume %v/%v: %v", vgName, lvName, err)
		}
	}
	return nil
}

func (ops Instance) confirmLV(vgName, lvName string, active bool, minimumSize int64, requireDevice bool) (*LogicalVolume, error) {
	lv, err := ops.getLV(vgName, lvName)
	if err != nil {
		return nil, err
	}
	if lv == nil {
		return nil, fmt.Errorf("logical volume %v/%v does not exist after the LVM operation completed", vgName, lvName)
	}
	if lv.Active != active {
		return nil, fmt.Errorf("logical volume %v/%v active state is %v, expected %v", vgName, lvName, lv.Active, active)
	}
	if lv.Size < minimumSize {
		return nil, fmt.Errorf("logical volume %v/%v size is %v, expected at least %v bytes", vgName, lvName, lv.Size, minimumSize)
	}
	if !requireDevice {
		return lv, nil
	}
	devicePath := lv.Path
	if devicePath == "" {
		devicePath = fmt.Sprintf("/dev/%s/%s", vgName, lvName)
	}
	deviceExists, blockDevice, err := ops.inspectDevice(devicePath)
	if err != nil {
		return nil, err
	}
	if !deviceExists || !blockDevice {
		return nil, fmt.Errorf("logical volume device %v is not an available block device", devicePath)
	}
	return lv, nil
}

func (ops Instance) confirmLVAbsent(vgName, lvName string) error {
	lv, err := ops.getLV(vgName, lvName)
	if err != nil {
		return err
	}
	if lv != nil {
		return fmt.Errorf("logical volume %v/%v still exists after removal", vgName, lvName)
	}
	return nil
}

// inspectDevice returns whether path exists and whether its resolved target is
// a block device. The two results distinguish a missing path from a dangling
// device symlink.
func (ops Instance) inspectDevice(path string) (bool, bool, error) {
	if ops.deviceStatus != nil {
		return ops.deviceStatus(path)
	}
	inspectPath := path
	// The IM bind-mounts host /dev onto /dev. Inspect the same device through
	// the existing /host mount so this check uses the host's persistent view.
	if strings.HasPrefix(path, "/dev/") {
		inspectPath = "/host" + path
	}
	if _, err := os.Lstat(inspectPath); os.IsNotExist(err) {
		return false, false, nil
	} else if err != nil {
		return false, false, fmt.Errorf("failed to inspect logical volume device path %v: %v", path, err)
	}
	info, err := os.Stat(inspectPath)
	if os.IsNotExist(err) {
		return true, false, nil
	}
	if err != nil {
		return true, false, fmt.Errorf("failed to resolve logical volume device %v: %v", path, err)
	}
	blockDevice := info.Mode()&os.ModeDevice != 0 && info.Mode()&os.ModeCharDevice == 0
	return true, blockDevice, nil
}

func (ops Instance) findLV(name string) (*LogicalVolume, error) {
	lvs, err := ops.listLVs()
	if err != nil {
		return nil, err
	}
	for i := range lvs {
		if lvs[i].Name == name {
			return &lvs[i], nil
		}
	}
	return nil, nil
}

// findEngineAttachment returns the replica LV the engine is attached to, or
// nil if the engine does not exist.
func (ops Instance) findEngineAttachment(engineName string) (*LogicalVolume, error) {
	lvs, err := ops.listLVs()
	if err != nil {
		return nil, err
	}
	for i := range lvs {
		if localLVEngineName(&lvs[i]) == engineName {
			return &lvs[i], nil
		}
	}
	return nil, nil
}

// markEngineAttached persists the engine attachment record on the replica LV.
func (ops Instance) markEngineAttached(lv *LogicalVolume, engineName string) error {
	if _, err := ops.executor.Execute(nil, "lvchange",
		CommandArgs("--addtag", lvmEngineTagPrefix+engineName, lv.VGName+"/"+lv.Name), lhtypes.ExecuteDefaultTimeout); err != nil {
		return fmt.Errorf("failed to tag logical volume %v/%v for engine %v: %v", lv.VGName, lv.Name, engineName, err)
	}
	return nil
}

// clearEngineAttachment removes the engine attachment record from the replica LV.
func (ops Instance) clearEngineAttachment(lv *LogicalVolume, engineName string) error {
	if _, err := ops.executor.Execute(nil, "lvchange",
		CommandArgs("--deltag", lvmEngineTagPrefix+engineName, lv.VGName+"/"+lv.Name), lhtypes.ExecuteDefaultTimeout); err != nil {
		return fmt.Errorf("failed to untag logical volume %v/%v for engine %v: %v", lv.VGName, lv.Name, engineName, err)
	}
	return nil
}

func localReplicaInstanceResponse(name, state string) *rpc.InstanceResponse {
	return &rpc.InstanceResponse{
		Spec: &rpc.InstanceSpec{
			Name:       name,
			Type:       types.InstanceTypeReplica,
			DataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
		},
		Status: &rpc.InstanceStatus{
			State:      state,
			Conditions: make(map[string]bool),
		},
	}
}

func localEngineInstanceResponse(name, state, endpoint string) *rpc.InstanceResponse {
	return &rpc.InstanceResponse{
		Spec: &rpc.InstanceSpec{
			Name:       name,
			Type:       types.InstanceTypeEngine,
			DataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
		},
		Status: &rpc.InstanceStatus{
			State:      state,
			Endpoint:   endpoint,
			Conditions: make(map[string]bool),
		},
	}
}
