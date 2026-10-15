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
			lvmPVListAllPrefix():                    lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
			lvmPVListPrefix("longhorn-vg-a7f3c921"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
			lvmVGStatPrefix("longhorn-vg-a7f3c921"): "107374182400;53687091200;4194304\n",
		},
	}
	ops := testDisk(executor)

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
	ops := testDisk(executor)

	_, err := ops.Get(&rpc.DiskGetRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("DiskGet error code = %v, want %v: %v", status.Code(err), codes.NotFound, err)
	}
}

func TestLVMDiskDelete(t *testing.T) {
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmPVListAllPrefix():                    lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
			lvmPVListPrefix("longhorn-vg-a7f3c921"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
		},
	}
	ops := testDisk(executor)

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
			lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a7f3c921", true),
		},
	}
	ops := testDisk(executor)

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
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-b", true),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmPVListPrefix("longhorn-vg-b"): lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-b", true),
		lvmVGStatPrefix("longhorn-vg-a"): "107374182400;53687091200;4194304\n",
		lvmVGStatPrefix("longhorn-vg-b"): "214748364800;161061273600;4194304\n",
	}}
	ops := testDisk(executor)

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
	getPV := lvmPVListAllPrefix()
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
	ops := testDisk(executor)

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
		lvmPVListAllPrefix(): fmt.Errorf("devices file is unavailable"),
	}}
	ops := testDisk(executor)

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
	getSecondPV := lvmPVListAllPrefix()
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
				lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) + lvmPVLine("/dev/sdc", "PV-2", "", false),
				lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) + lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
			},
		},
	}
	ops := testDisk(executor)

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
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
		lvmLVListPrefix("longhorn-vg-a"): lvmLVReport("longhorn-vg-a", "replica-1"),
	}}
	ops := testDisk(executor)

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
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
	}}
	ops := testDisk(executor)

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
		lvmPVListAllPrefix():             lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmLVListPrefix("longhorn-vg-a"): lvmLVReport("longhorn-vg-a", "replica-1"),
	}}
	ops := testDisk(executor)

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
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
	}}
	ops := testDisk(executor)

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
	ops := testDisk(executor)
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
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "rhel", false),
	}}
	ops := testDisk(executor)

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
			lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "", false),
		},
		errs: map[string]error{
			lvmCommandPrefix("vgcreate"): fmt.Errorf("vgcreate exploded"),
		},
	}
	ops := testDisk(executor)

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
			lvmPVListAllPrefix(): {
				lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) + lvmPVLine("/dev/sdc", "PV-2", "", false),
				lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) + lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-a", false),
			},
		},
	}
	ops := testDisk(executor)

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

// testDevices maps the paths the tests use to the device numbers a stat
// would report on the host: two disks, each reachable through an alias.
var testDevices = map[string][2]int64{
	"/dev/sdb":                           {8, 16},
	"/dev/disk/by-id/scsi-3600062b2":     {8, 16},
	"/dev/sdc":                           {8, 32},
	"/dev/disk/by-path/pci-0000:05:00.0": {8, 32},
	"/dev/mapper/mpatha":                 {253, 3},
	"/dev/dm-3":                          {253, 3},
}

func fakeDeviceNumbers(devPath string) (int64, int64, bool, error) {
	numbers, ok := testDevices[devPath]
	if !ok {
		return 0, 0, false, nil
	}
	return numbers[0], numbers[1], true, nil
}

func testDisk(executor *fakeExecutor) Disk {
	return Disk{executor: executor, deviceNumbers: fakeDeviceNumbers}
}

// lvmPVListAllPrefix is the pvs listing getPV matches devices against.
func lvmPVListAllPrefix() string {
	return lvmCommandPrefix("pvs", "--noheadings", "--separator", ";", "-o", lvmPVFields)
}

func lvmPVListPrefix(vgName string) string {
	return lvmCommandPrefix("pvs", "--noheadings", "--separator", ";", "--select", "vg_name="+vgName, "-o", lvmPVFields)
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

// lvmPVLine renders one pvs report line for a PV on the kernel device path,
// with the device numbers testDevices assigns to it.
func lvmPVLine(path, pvUUID, vgName string, representative bool) string {
	tags := ""
	if representative {
		tags += lvmRepresentativePVTag
	}
	numbers, ok := testDevices[path]
	if !ok {
		panic("lvmPVLine: unknown test device " + path)
	}
	return fmt.Sprintf("  %s;%s;%s;%s;%d;%d\n", path, pvUUID, vgName, tags, numbers[0], numbers[1])
}

func TestLVMDiskGetPVResolvesDeviceAliases(t *testing.T) {
	// LVM reports the kernel name; the manager registers the disk by whatever
	// alias the operator chose. The two must meet on the device numbers.
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			lvmPVLine("/dev/sdc", "PV-2", "longhorn-vg-b", true) +
			lvmPVLine("/dev/mapper/mpatha", "PV-3", "longhorn-vg-c", true),
	}}
	ops := testDisk(executor)

	for path, uuid := range map[string]string{
		"/dev/sdb":                           "PV-1",
		"/dev/disk/by-id/scsi-3600062b2":     "PV-1",
		"/dev/disk/by-path/pci-0000:05:00.0": "PV-2",
		"/dev/mapper/mpatha":                 "PV-3",
		"/dev/dm-3":                          "PV-3",
	} {
		pv, err := ops.getPV(path)
		if err != nil {
			t.Fatalf("getPV(%v) failed: %v", path, err)
		}
		if pv == nil || pv.UUID != uuid {
			t.Fatalf("getPV(%v) = %+v, want PV %v", path, pv, uuid)
		}
	}
	if !slicesContainPrefix(executor.calls, lvmPVListAllPrefix()) || slicesContainPrefix(executor.calls, lvmCommandPrefix("pvs", "--noheadings", "--separator", ";", "--select", "pv_name=")) {
		t.Fatalf("PVs must be listed and matched by device numbers, not selected by name: %v", executor.calls)
	}
}

func TestLVMDiskGetPVIgnoresOtherAndMissingDevices(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		// The second line is a PV whose device disappeared: pvs reports it
		// as [unknown] without device numbers.
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true) +
			"  [unknown];PV-9;longhorn-vg-z;;-1;-1\n",
	}}
	ops := testDisk(executor)

	pv, err := ops.getPV("/dev/sdc")
	if err != nil || pv != nil {
		t.Fatalf("getPV of a device without a PV = %+v, %v; want nil, nil", pv, err)
	}
	pv, err = ops.getPV("/dev/disk/by-id/gone")
	if err != nil || pv != nil {
		t.Fatalf("getPV of a missing path = %+v, %v; want nil, nil", pv, err)
	}
}

func TestLVMDiskCreateByDeviceAlias(t *testing.T) {
	// The reconcile after a failed first attempt: pvcreate and vgcreate ran on
	// the kernel device while the request names the disk by its by-id path.
	// The lookup must find the PV through the alias and adopt it instead of
	// treating the device as new, and the disk must report the caller's path.
	alias := "/dev/disk/by-id/scsi-3600062b2"
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", false),
			lvmVGStatPrefix("longhorn-vg-a"): "107374182400;53687091200;4194304\n",
		},
		outputSequences: map[string][]string{
			lvmPVListAllPrefix(): {
				lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", false),
				lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
			},
		},
	}
	ops := testDisk(executor)

	diskInfo, err := ops.Create(&rpc.DiskCreateRequest{
		DiskName: "disk-1", DiskPath: alias,
		StorageLayout: rpc.LVMStorageLayout_LVM_STORAGE_LAYOUT_PER_DISK,
	})
	if err != nil {
		t.Fatalf("failed to create disk by alias: %v", err)
	}
	if diskInfo.Uuid != "PV-1" || diskInfo.Path != alias || diskInfo.TotalSize == 0 {
		t.Fatalf("unexpected disk info: %+v", diskInfo)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--adddev", alias)) {
		t.Fatalf("expected the alias to be added to the devices file: %v", executor.calls)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("pvchange", "--addtag", lvmRepresentativePVTag, "/dev/sdb")) {
		t.Fatalf("expected the representative marker on the PV LVM reports: %v", executor.calls)
	}
	for _, forbidden := range []string{"wipefs", "pvcreate", "vgcreate", "vgextend", "lvmdevices --deldev"} {
		if slicesContainPrefix(executor.calls, forbidden) {
			t.Fatalf("%v must not run when the alias resolves to an existing PV: %v", forbidden, executor.calls)
		}
	}
}

func TestLVMDiskDeleteByDeviceAlias(t *testing.T) {
	alias := "/dev/disk/by-id/scsi-3600062b2"
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():             lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
	}}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskType: rpc.DiskType_lvm, DiskName: "disk-1", DiskUuid: "PV-1", DiskPath: alias,
	}); err != nil {
		t.Fatalf("DiskDelete by alias failed: %v", err)
	}
	for _, expected := range []string{
		lvmCommandPrefix("vgremove", "-y", "longhorn-vg-a"),
		lvmCommandPrefix("pvremove", "-y", alias),
		lvmCommandPrefix("lvmdevices", "--deldev", alias),
	} {
		if !slicesContainPrefix(executor.calls, expected) {
			t.Fatalf("expected %q, calls: %v", expected, executor.calls)
		}
	}
}

func TestLVMDiskDeleteByAliasRefusesUUIDMismatch(t *testing.T) {
	// Before the device-number lookup, a by-id path made Delete miss the PV
	// and return success, leaving the VG on the disk. The PV must be found and
	// the UUID check must still guard it.
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "PV-1", "longhorn-vg-a", true),
	}}
	ops := testDisk(executor)

	_, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskName: "disk-1", DiskUuid: "PV-other", DiskPath: "/dev/disk/by-id/scsi-3600062b2",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskDelete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgremove") || slicesContainPrefix(executor.calls, "lvmdevices --deldev") {
		t.Fatalf("nothing may be removed on a UUID mismatch: %v", executor.calls)
	}
}

func TestLookupDeviceNumbers(t *testing.T) {
	if _, _, found, err := lookupDeviceNumbers(filepath.Join(t.TempDir(), "missing")); err != nil || found {
		t.Fatalf("missing path: found=%v err=%v, want not found without error", found, err)
	}
	regular := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(regular, nil, 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := lookupDeviceNumbers(regular); err == nil {
		t.Fatal("a regular file must be rejected as a device")
	}
}
