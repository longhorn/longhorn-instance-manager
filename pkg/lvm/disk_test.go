package lvm

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCreateDevicesFileIfMissing(t *testing.T) {
	path := filepath.Join(t.TempDir(), DevicesFileName)

	created, err := createDevicesFileIfMissing(path)
	if err != nil {
		t.Fatalf("failed to initialize devices file: %v", err)
	}
	if !created {
		t.Fatal("expected devices file to be reported as newly created")
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat devices file: %v", err)
	}
	if info.Size() != 0 {
		t.Fatalf("new devices file size = %v, want 0", info.Size())
	}

	const existingContent = "existing LVM devices file\n"
	if err := os.WriteFile(path, []byte(existingContent), 0600); err != nil {
		t.Fatalf("failed to populate devices file: %v", err)
	}
	created, err = createDevicesFileIfMissing(path)
	if err != nil {
		t.Fatalf("failed to reinitialize devices file: %v", err)
	}
	if created {
		t.Fatal("expected existing devices file to be reused")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read devices file: %v", err)
	}
	if string(content) != existingContent {
		t.Fatalf("devices file was modified: %q", content)
	}
}

func TestCommandArgsDisableUdev(t *testing.T) {
	args := strings.Join(CommandArgs("command-argument"), " ")
	for _, expected := range []string{
		"--devicesfile longhorn.devices",
		`external_device_info_source = "none"`,
		"udev_sync = 0",
		"udev_rules = 0",
		"command-argument",
	} {
		if !strings.Contains(args, expected) {
			t.Fatalf("LVM arguments %q do not contain %q", args, expected)
		}
	}
}

type fakeExecutor struct {
	outputs         map[string]string
	outputSequences map[string][]string
	errs            map[string]error
	calls           []string
}

func (e *fakeExecutor) Execute(envs []string, binary string, args []string, timeout time.Duration) (string, error) {
	cmd := binary + " " + strings.Join(args, " ")
	e.calls = append(e.calls, cmd)
	bestPrefix := ""
	for prefix := range e.errs {
		if strings.HasPrefix(cmd, prefix) && len(prefix) > len(bestPrefix) {
			bestPrefix = prefix
		}
	}
	if bestPrefix != "" {
		return "", e.errs[bestPrefix]
	}
	bestPrefix = ""
	for prefix := range e.outputSequences {
		if strings.HasPrefix(cmd, prefix) && len(prefix) > len(bestPrefix) {
			bestPrefix = prefix
		}
	}
	if bestPrefix != "" && len(e.outputSequences[bestPrefix]) > 0 {
		output := e.outputSequences[bestPrefix][0]
		e.outputSequences[bestPrefix] = e.outputSequences[bestPrefix][1:]
		return output, nil
	}
	bestPrefix = ""
	for prefix := range e.outputs {
		if strings.HasPrefix(cmd, prefix) && len(prefix) > len(bestPrefix) {
			bestPrefix = prefix
		}
	}
	if bestPrefix != "" {
		return e.outputs[bestPrefix], nil
	}
	if binary == "lvs" {
		return `{"report":[{"lv":[]}]}`, nil
	}
	return "", nil
}

func (e *fakeExecutor) ExecuteWithStdin(binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	return "", nil
}

func (e *fakeExecutor) ExecuteWithStdinPipe(binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	return "", nil
}

func TestLVMDiskGet(t *testing.T) {
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmPVGetPrefix("/dev/sdb"):              lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
			lvmPVListPrefix("longhorn-vg-a7f3c921"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
			lvmVGStatPrefix("longhorn-vg-a7f3c921"): "107374182400;53687091200;4194304\n",
		},
	}
	ops := Disk{executor: executor}

	disk, err := ops.Get(&rpc.DiskGetRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if err != nil {
		t.Fatalf("DiskGet failed: %v", err)
	}
	if disk.Uuid != "PV-1" || disk.TotalSize != 107374182400 || disk.FreeSize != 53687091200 || disk.BlockSize != 4194304 {
		t.Fatalf("unexpected disk info: %+v", disk)
	}
	// The Longhorn disk name is reported, not the VG name, so the manager
	// never sees (and cannot re-prefix) the VG name.
	if disk.Name != "disk-1" || disk.State != lvmDiskStateReady {
		t.Fatalf("unexpected disk name or state: %+v", disk)
	}
}

func TestLVMDiskGetNotFound(t *testing.T) {
	executor := &fakeExecutor{}
	ops := Disk{executor: executor}

	_, err := ops.Get(&rpc.DiskGetRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("DiskGet error code = %v, want %v: %v", status.Code(err), codes.NotFound, err)
	}
}

func TestLVMDiskDelete(t *testing.T) {
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmPVGetPrefix("/dev/sdb"):              lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
			lvmPVListPrefix("longhorn-vg-a7f3c921"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
		},
	}
	ops := Disk{executor: executor}

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskType: rpc.DiskType_lvm,
		DiskName: "disk-1",
		DiskUuid: "PV-1",
		DiskPath: "/dev/sdb",
	}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("vgremove", "-y", "longhorn-vg-a7f3c921")) {
		t.Fatalf("expected vgremove to run, calls: %v", executor.calls)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdb")) {
		t.Fatalf("expected disk removal from the devices file, calls: %v", executor.calls)
	}
}

func TestLVMDiskDeleteRefusesUUIDMismatch(t *testing.T) {
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmPVGetPrefix("/dev/sdb"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
		},
	}
	ops := Disk{executor: executor}

	_, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskType: rpc.DiskType_lvm,
		DiskName: "disk-1",
		DiskUuid: "GhIjKl-5678",
		DiskPath: "/dev/sdb",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskDelete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgremove") {
		t.Fatalf("vgremove must not run on UUID mismatch, calls: %v", executor.calls)
	}
}

func slicesContainPrefix(calls []string, prefix string) bool {
	for _, call := range calls {
		if strings.HasPrefix(call, prefix) {
			return true
		}
	}
	return false
}

func lvmCommandPrefix(binary string, args ...string) string {
	return binary + " " + strings.Join(CommandArgs(args...), " ")
}

func TestLVMDiskCreateAdoptsIndependentPerDiskVGs(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVGetPrefix("/dev/sdb"):       lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmPVGetPrefix("/dev/sdc"):       lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-b", true),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmPVListPrefix("longhorn-vg-b"): lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-b", true),
		lvmVGStatPrefix("longhorn-vg-a"): "107374182400;53687091200;4194304\n",
		lvmVGStatPrefix("longhorn-vg-b"): "214748364800;161061273600;4194304\n",
	}}
	ops := Disk{executor: executor}

	first, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName: "disk-1", DiskPath: "/dev/sdb",
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_DISK,
	})
	if err != nil {
		t.Fatalf("failed to adopt first disk: %v", err)
	}
	second, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName: "disk-2", DiskPath: "/dev/sdc",
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_DISK,
	})
	if err != nil {
		t.Fatalf("failed to adopt second disk: %v", err)
	}

	if first.Name != "disk-1" || first.Uuid != "PV-1" {
		t.Fatalf("unexpected first disk: %+v", first)
	}
	if second.Name != "disk-2" || second.Uuid != "PV-2" {
		t.Fatalf("unexpected second disk: %+v", second)
	}
	if first.Uuid == second.Uuid {
		t.Fatalf("per-disk VGs must have distinct UUIDs: %+v %+v", first, second)
	}
	if slicesContainPrefix(executor.calls, "vgextend") {
		t.Fatalf("a per-disk layout must not extend an existing VG: %v", executor.calls)
	}
}

func TestLVMDiskCreateResumesAfterVGCreation(t *testing.T) {
	getPV := lvmPVGetPrefix("/dev/sdb")
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", false),
			lvmVGStatPrefix("longhorn-vg-a"): "107374182400;53687091200;4194304\n",
		},
		outputSequences: map[string][]string{
			getPV: {
				lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", false),
				lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
			},
		},
	}
	ops := Disk{executor: executor}

	diskInfo, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName: "disk-1", DiskPath: "/dev/sdb",
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_DISK,
	})
	if err != nil {
		t.Fatalf("failed to resume disk creation: %v", err)
	}
	if diskInfo.Uuid != "PV-1" || diskInfo.TotalSize == 0 {
		t.Fatalf("unexpected disk info: %+v", diskInfo)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("pvchange", "--addtag", lvmRepresentativePVTag, "/dev/sdb")) {
		t.Fatalf("expected missing representative marker to be restored: %v", executor.calls)
	}
	if slicesContainPrefix(executor.calls, "pvcreate") || slicesContainPrefix(executor.calls, "vgcreate") || slicesContainPrefix(executor.calls, "vgextend") {
		t.Fatalf("completed PV and VG steps must not be repeated: %v", executor.calls)
	}
}

func TestLVMDiskCreatePropagatesPVInspectionFailure(t *testing.T) {
	executor := &fakeExecutor{errs: map[string]error{
		lvmPVGetPrefix("/dev/sdb"): fmt.Errorf("devices file is unavailable"),
	}}
	ops := Disk{executor: executor}

	_, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName: "disk-1", DiskPath: "/dev/sdb",
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_DISK,
	})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "devices file is unavailable") {
		t.Fatalf("unexpected DiskCreate error: %v", err)
	}
	if slicesContainPrefix(executor.calls, "pvcreate") {
		t.Fatalf("PV creation must not run after an inspection failure: %v", executor.calls)
	}
}

func TestLVMDiskCreateExtendsPerNodeVG(t *testing.T) {
	getSecondPV := lvmPVGetPrefix("/dev/sdc")
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmCommandPrefix("vgs", "--noheadings", "-o", "vg_name"):                  "longhorn-vg-a\n",
			lvmCommandPrefix("vgs", "--noheadings", "-o", "vg_uuid", "longhorn-vg-a"): "VG-1\n",
			lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
				lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
			lvmVGStatPrefix("longhorn-vg-a"): "214748364800;161061273600;4194304\n",
		},
		outputSequences: map[string][]string{
			getSecondPV: {
				lvmPVLine("/dev/sdc", "PV-2", "", false),
				lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
			},
		},
	}
	ops := Disk{executor: executor}

	diskInfo, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName:      "disk-2",
		DiskPath:      "/dev/sdc",
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_NODE,
	})
	if err != nil {
		t.Fatalf("failed to extend per-node VG: %v", err)
	}
	if diskInfo.Name != "disk-2" || diskInfo.Uuid != "PV-2" || diskInfo.TotalSize != 0 || diskInfo.FreeSize != 0 {
		t.Fatalf("unexpected pooled member info: %+v", diskInfo)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("vgextend", "longhorn-vg-a", "/dev/sdc")) {
		t.Fatalf("expected vgextend, calls: %v", executor.calls)
	}
	if slicesContainPrefix(executor.calls, "vgcreate") {
		t.Fatalf("must not create another VG in per-node mode: %v", executor.calls)
	}
}

func TestLVMDiskDeleteRefusesMemberWhenVGContainsLV(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVGetPrefix("/dev/sdc"): lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
		lvmLVListPrefix("longhorn-vg-a"): lvmLVReport("longhorn-vg-a", "replica-1"),
	}}
	ops := Disk{executor: executor}

	_, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskName: "disk-2", DiskUuid: "PV-2", DiskPath: "/dev/sdc",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskDelete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgreduce") {
		t.Fatalf("must not remove a member while the VG contains LVs: %v", executor.calls)
	}
}

func TestLVMDiskDeleteRemovesEmptyMember(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVGetPrefix("/dev/sdc"): lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
	}}
	ops := Disk{executor: executor}

	_, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskName: "disk-2", DiskUuid: "PV-2", DiskPath: "/dev/sdc",
	})
	if err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("vgreduce", "longhorn-vg-a", "/dev/sdc")) ||
		!slicesContainPrefix(executor.calls, lvmCommandPrefix("pvremove", "-y", "/dev/sdc")) {
		t.Fatalf("expected empty member removal: %v", executor.calls)
	}
}

func TestLVMDiskDeleteRefusesSinglePVWhenVGContainsLV(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVGetPrefix("/dev/sdb"):       lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmLVListPrefix("longhorn-vg-a"): lvmLVReport("longhorn-vg-a", "replica-1"),
	}}
	ops := Disk{executor: executor}

	_, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskName: "disk-1", DiskUuid: "PV-1", DiskPath: "/dev/sdb",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskDelete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgremove") || slicesContainPrefix(executor.calls, "pvremove") {
		t.Fatalf("must not remove a single-PV VG while it contains LVs: %v", executor.calls)
	}
}

func TestLVMDiskDeleteRefusesRepresentativeWithMembers(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVGetPrefix("/dev/sdb"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
	}}
	ops := Disk{executor: executor}

	_, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskName: "disk-1", DiskUuid: "PV-1", DiskPath: "/dev/sdb",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskDelete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgreduce") || slicesContainPrefix(executor.calls, "pvremove") {
		t.Fatalf("representative disk must remain unchanged: %v", executor.calls)
	}
}

func TestLVMDiskCreateRequiresStorageLayout(t *testing.T) {
	executor := &fakeExecutor{}
	ops := Disk{executor: executor}
	_, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("DiskCreate error code = %v, want %v: %v", status.Code(err), codes.InvalidArgument, err)
	}
	// The layout is checked before any LVM command or device access, so the
	// outcome does not depend on the host the test runs on.
	if len(executor.calls) != 0 {
		t.Fatalf("no LVM command must run for an unsupported layout: %v", executor.calls)
	}
}

func TestLVMDiskCreateRejectsForeignVG(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVGetPrefix("/dev/sdb"): lvmPVLine("/dev/sdb", "PV-1", "rhel", false),
	}}
	ops := Disk{executor: executor}

	_, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName: "disk-1", DiskPath: "/dev/sdb",
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_NODE,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskCreate error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdb")) {
		t.Fatalf("a device with a foreign VG must leave the devices file again: %v", executor.calls)
	}
	for _, forbidden := range []string{"pvchange", "vgextend", "vgcreate", "lvextend", "lvcreate"} {
		if slicesContainPrefix(executor.calls, forbidden) {
			t.Fatalf("%v must not run on a VG Longhorn does not manage: %v", forbidden, executor.calls)
		}
	}
}

func TestLVMDiskCreateForgetsDeviceWhenVGCreationFails(t *testing.T) {
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmPVGetPrefix("/dev/sdb"): lvmPVLine("/dev/sdb", "PV-1", "", false),
		},
		errs: map[string]error{
			lvmCommandPrefix("vgcreate"): fmt.Errorf("vgcreate exploded"),
		},
	}
	ops := Disk{executor: executor}

	_, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName: "disk-1", DiskPath: "/dev/sdb",
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_DISK,
	})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "vgcreate exploded") {
		t.Fatalf("unexpected DiskCreate error: %v", err)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdb")) {
		t.Fatalf("a device that never joined a Longhorn VG must leave the devices file: %v", executor.calls)
	}
}

func TestLVMDiskCreateKeepsDeviceAfterJoiningVG(t *testing.T) {
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmCommandPrefix("vgs", "--noheadings", "-o", "vg_name"): "longhorn-vg-a\n",
			lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
				lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
			lvmLVListPrefix("longhorn-vg-a"): lvmLVReport("longhorn-vg-a", ThinPoolName),
		},
		outputSequences: map[string][]string{
			lvmPVGetPrefix("/dev/sdc"): {
				lvmPVLine("/dev/sdc", "PV-2", "", false),
				lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
			},
		},
	}
	ops := Disk{executor: executor}

	// The VG got extended, then the thin pool inspection finds an unexpected LV
	// under the pool name. The device is now a VG member, so it must stay in
	// the devices file for the retry to resume from.
	_, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName: "disk-2", DiskPath: "/dev/sdc",
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_NODE,
	})
	if err == nil {
		t.Fatal("expected DiskCreate to fail on the unexpected thin pool")
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("vgextend", "longhorn-vg-a", "/dev/sdc")) {
		t.Fatalf("expected vgextend, calls: %v", executor.calls)
	}
	if slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdc")) {
		t.Fatalf("a VG member must not be removed from the devices file: %v", executor.calls)
	}
}

func lvmPVGetPrefix(path string) string {
	return lvmCommandPrefix("pvs", "--noheadings", "--separator", ";", "--select", "pv_name="+path, "-o", "pv_name,pv_uuid,vg_name,pv_tags")
}

func lvmPVListPrefix(vgName string) string {
	return lvmCommandPrefix("pvs", "--noheadings", "--separator", ";", "--select", "vg_name="+vgName, "-o", "pv_name,pv_uuid,vg_name,pv_tags")
}

func lvmLVListPrefix(vgName string) string {
	return lvmCommandPrefix("lvs", "--reportformat", "json", "--units", "b", "--nosuffix", "--select", "vg_name="+vgName, "-o", lvmLogicalVolumeFields)
}

func lvmLVReport(vgName, lvName string) string {
	return fmt.Sprintf(`{"report":[{"lv":[{"vg_name":%q,"lv_name":%q,"lv_size":"1073741824","lv_active":"active","lv_path":%q,"lv_tags":"","lv_attr":"-wi-a-----","pool_lv":"","data_percent":""}]}]}`,
		vgName, lvName, "/dev/"+vgName+"/"+lvName)
}

func lvmVGStatPrefix(vgName string) string {
	return lvmCommandPrefix("vgs", "--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "-o", "vg_size,vg_free,vg_extent_size", vgName)
}

func lvmPVLine(path, pvUUID, vgName string, representative bool) string {
	tags := ""
	if representative {
		tags += lvmRepresentativePVTag
	}
	return fmt.Sprintf("%s;%s;%s;%s\n", path, pvUUID, vgName, tags)
}
