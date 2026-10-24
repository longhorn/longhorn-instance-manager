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

// testDevices maps the paths the tests use to the device numbers a stat
// would report on the host: two disks, each reachable through an alias, and
// a multipath device.
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

func lvmPVListAllPrefix() string {
	return lvmCommandPrefix("pvs", "--noheadings", "--separator", ";", "-o", lvmPVFields)
}

func lvmPVListPrefix(vgName string) string {
	return lvmCommandPrefix("pvs", "--noheadings", "--separator", ";", "--select", "vg_name="+vgName, "-o", lvmPVFields)
}

func lvmVGByUUIDPrefix(vgUUID string) string {
	return lvmCommandPrefix("vgs", "--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "--select", "vg_uuid="+vgUUID, "-o", lvmVGFields)
}

func lvmVGListPrefix(vgName string) string {
	return lvmCommandPrefix("vgs", "--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "--select", "vg_name="+vgName, "-o", lvmVGFields)
}

func lvmLVListPrefix(vgName string) string {
	return lvmCommandPrefix("lvs", "--reportformat", "json", "--units", "b", "--nosuffix", "--select", "vg_name="+vgName, "-o", lvmLogicalVolumeFields)
}

func lvmLVReport(vgName, lvName string) string {
	return fmt.Sprintf(`{"report":[{"lv":[{"vg_name":%q,"lv_name":%q,"lv_size":"1073741824","lv_active":"active","lv_path":%q,"lv_tags":"","lv_attr":"-wi-a-----","pool_lv":"","data_percent":""}]}]}`,
		vgName, lvName, "/dev/"+vgName+"/"+lvName)
}

// lvmPVLine renders one pvs report line for a PV on the kernel device path,
// with the device numbers testDevices assigns to it.
func lvmPVLine(path, vgName string) string {
	numbers, ok := testDevices[path]
	if !ok {
		panic("lvmPVLine: unknown test device " + path)
	}
	return fmt.Sprintf("  %s;%s;%d;%d\n", path, vgName, numbers[0], numbers[1])
}

// lvmVGLine renders one vgs report line: a 100 GiB volume group with half of
// it free and 4 MiB extents.
func lvmVGLine(name, vgUUID string, tags string, lvCount int) string {
	return fmt.Sprintf("  %s;%s;%s;%d;107374182400;53687091200;4194304\n", name, vgUUID, tags, lvCount)
}

func TestParseDiskPath(t *testing.T) {
	ops := testDisk(&fakeExecutor{})
	for _, tc := range []struct {
		path    string
		devices []string
		vgName  string
		wantErr bool
	}{
		{path: "/dev/sdb", devices: []string{"/dev/sdb"}},
		{path: "/dev/sdb;/dev/sdc", devices: []string{"/dev/sdb", "/dev/sdc"}},
		{path: " /dev/sdb ; /dev/disk/by-path/pci-0000:05:00.0 ;", devices: []string{"/dev/sdb", "/dev/disk/by-path/pci-0000:05:00.0"}},
		{path: "data-vg", vgName: "data-vg"},
		{path: "/dev/data-vg", vgName: "data-vg"},
		{path: "", wantErr: true},
		{path: ";", wantErr: true},
		{path: "/dev/sdb;data-vg", wantErr: true},
		{path: "/dev/", wantErr: true},
		{path: "/mnt/disk", wantErr: true},
	} {
		dp, err := ops.parseDiskPath(tc.path)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("parseDiskPath(%q) = %+v, want an error", tc.path, dp)
			}
			continue
		}
		if err != nil {
			t.Fatalf("parseDiskPath(%q) failed: %v", tc.path, err)
		}
		if strings.Join(dp.devicePaths(), ";") != strings.Join(tc.devices, ";") || dp.vgName != tc.vgName {
			t.Fatalf("parseDiskPath(%q) = devices %v vg %q, want devices %v vg %q", tc.path, dp.devicePaths(), dp.vgName, tc.devices, tc.vgName)
		}
	}
}

func TestLVMDiskGet(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():             lvmPVLine("/dev/sdb", "longhorn-vg-a"),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
	}}
	ops := testDisk(executor)

	diskInfo, err := ops.Get(&rpc.DiskGetRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if err != nil {
		t.Fatalf("DiskGet failed: %v", err)
	}
	if diskInfo.Uuid != "VG-1" || diskInfo.Id != "VG-1" || diskInfo.Name != "disk-1" || diskInfo.Path != "/dev/sdb" {
		t.Fatalf("unexpected disk identity: %+v", diskInfo)
	}
	if diskInfo.TotalSize != 107374182400 || diskInfo.FreeSize != 53687091200 || diskInfo.BlockSize != 4194304 || diskInfo.TotalBlocks != 25600 {
		t.Fatalf("unexpected disk capacity: %+v", diskInfo)
	}
	if diskInfo.State != lvmDiskStateReady || diskInfo.Type != rpc.DiskType_lvm.String() {
		t.Fatalf("unexpected disk state or type: %+v", diskInfo)
	}
}

func TestLVMDiskGetByVolumeGroupName(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGListPrefix("data-vg"): lvmVGLine("data-vg", "VG-9", VGTag, 2),
	}}
	ops := testDisk(executor)

	diskInfo, err := ops.Get(&rpc.DiskGetRequest{DiskName: "disk-1", DiskPath: "/dev/data-vg"})
	if err != nil {
		t.Fatalf("DiskGet failed: %v", err)
	}
	if diskInfo.Uuid != "VG-9" || diskInfo.Path != "/dev/data-vg" {
		t.Fatalf("unexpected disk info: %+v", diskInfo)
	}
	if slicesContainPrefix(executor.calls, "pvs") {
		t.Fatalf("a volume group name needs no physical volume lookup: %v", executor.calls)
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

func TestLVMDiskGetRefusesForeignVolumeGroup(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():    lvmPVLine("/dev/sdb", "rhel"),
		lvmVGListPrefix("rhel"): lvmVGLine("rhel", "VG-rhel", "", 3),
	}}
	ops := testDisk(executor)

	_, err := ops.Get(&rpc.DiskGetRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("DiskGet error code = %v, want %v: %v", status.Code(err), codes.NotFound, err)
	}
}

func TestLVMDiskCreateFromEmptyDevices(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix(): "",
		// The group vgcreate makes, whatever random name it gets.
		lvmCommandPrefix("vgs", "--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "--select", "vg_name=longhorn-vg-"): lvmVGLine("longhorn-vg-x", "VG-1", VGTag, 0),
	}}
	ops := testDisk(executor)

	diskInfo, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb;/dev/sdc"})
	if err != nil {
		t.Fatalf("DiskCreate failed: %v", err)
	}
	if diskInfo.Uuid != "VG-1" || diskInfo.Path != "/dev/sdb;/dev/sdc" || diskInfo.TotalSize == 0 {
		t.Fatalf("unexpected disk info: %+v", diskInfo)
	}
	for _, expected := range []string{
		lvmCommandPrefix("lvmdevices", "--adddev", "/dev/sdb"),
		lvmCommandPrefix("lvmdevices", "--adddev", "/dev/sdc"),
		"wipefs --noheadings --parsable /dev/sdb",
		"wipefs --noheadings --parsable /dev/sdc",
		lvmCommandPrefix("pvcreate", "/dev/sdb"),
		lvmCommandPrefix("pvcreate", "/dev/sdc"),
		lvmCommandPrefix("vgcreate", "--addtag", VGTag, "longhorn-vg-"),
	} {
		if !slicesContainPrefix(executor.calls, expected) {
			t.Fatalf("expected %q, calls: %v", expected, executor.calls)
		}
	}
	for _, call := range executor.calls {
		if strings.HasPrefix(call, "vgcreate") && !strings.HasSuffix(call, " /dev/sdb /dev/sdc") {
			t.Fatalf("vgcreate must span both devices: %v", call)
		}
	}
	for _, forbidden := range []string{"vgextend", "vgimportdevices", "lvmdevices --deldev", "pvremove"} {
		if slicesContainPrefix(executor.calls, lvmCommandPrefix(forbidden)) || slicesContainPrefix(executor.calls, forbidden) {
			t.Fatalf("%v must not run on a clean creation: %v", forbidden, executor.calls)
		}
	}
}

func TestLVMDiskCreateIsIdempotent(t *testing.T) {
	// Both devices already form one tagged VG, for instance after the node
	// resource was recreated: nothing is labelled or created again, and the
	// LVs of attached engines are brought back.
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "longhorn-vg-a") +
			lvmPVLine("/dev/sdc", "longhorn-vg-a"),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 1),
	}}
	ops := testDisk(executor)

	diskInfo, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/disk/by-id/scsi-3600062b2;/dev/sdc"})
	if err != nil {
		t.Fatalf("DiskCreate failed: %v", err)
	}
	if diskInfo.Uuid != "VG-1" {
		t.Fatalf("unexpected disk info: %+v", diskInfo)
	}
	for _, forbidden := range []string{"wipefs", "pvcreate", "vgcreate", "vgextend", "vgchange"} {
		if slicesContainPrefix(executor.calls, forbidden) {
			t.Fatalf("%v must not run when the devices already form the disk: %v", forbidden, executor.calls)
		}
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvs", "--reportformat", "json")) {
		t.Fatalf("expected the attached logical volumes to be looked up for reactivation: %v", executor.calls)
	}
}

func TestLVMDiskCreateRejectsNonEmptyDevice(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():                      "",
		"wipefs --noheadings --parsable /dev/sdc": "/dev/sdc;0x438;ext4;;\n",
	}}
	ops := testDisk(executor)

	_, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb;/dev/sdc"})
	if status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "/dev/sdc") {
		t.Fatalf("DiskCreate error = %v, want InvalidArgument naming /dev/sdc", err)
	}
	if slicesContainPrefix(executor.calls, "pvcreate") || slicesContainPrefix(executor.calls, "vgcreate") {
		t.Fatalf("no device may be labelled when one of them is not empty: %v", executor.calls)
	}
	for _, device := range []string{"/dev/sdb", "/dev/sdc"} {
		if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", device)) {
			t.Fatalf("expected %v to leave the devices file again: %v", device, executor.calls)
		}
	}
}

func TestLVMDiskCreateRejectsDeviceInForeignVG(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():    lvmPVLine("/dev/sdb", "rhel"),
		lvmVGListPrefix("rhel"): lvmVGLine("rhel", "VG-rhel", "", 3),
	}}
	ops := testDisk(executor)

	_, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "rhel") {
		t.Fatalf("DiskCreate error = %v, want FailedPrecondition naming the volume group", err)
	}
	for _, forbidden := range []string{"pvcreate", "vgcreate", "vgchange", "vgextend"} {
		if slicesContainPrefix(executor.calls, forbidden) {
			t.Fatalf("%v must not run on a VG Longhorn does not manage: %v", forbidden, executor.calls)
		}
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdb")) {
		t.Fatalf("the device of a foreign volume group must leave the devices file again: %v", executor.calls)
	}
}

func TestLVMDiskCreateRejectsAddingDevicesToRegisteredVG(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():             lvmPVLine("/dev/sdb", "longhorn-vg-a"),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
	}}
	ops := testDisk(executor)

	_, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb;/dev/sdc"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskCreate error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgextend") || slicesContainPrefix(executor.calls, "pvcreate") {
		t.Fatalf("a registered volume group must not grow: %v", executor.calls)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdc")) {
		t.Fatalf("the device that did not join must leave the devices file: %v", executor.calls)
	}
	if slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdb")) {
		t.Fatalf("the member of the registered volume group must stay in the devices file: %v", executor.calls)
	}
}

func TestLVMDiskCreateUndoesPartialCreation(t *testing.T) {
	executor := &fakeExecutor{
		outputs: map[string]string{lvmPVListAllPrefix(): ""},
		errs:    map[string]error{lvmCommandPrefix("vgcreate"): fmt.Errorf("vgcreate exploded")},
	}
	ops := testDisk(executor)

	_, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb;/dev/sdc"})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "vgcreate exploded") {
		t.Fatalf("unexpected DiskCreate error: %v", err)
	}
	for _, device := range []string{"/dev/sdb", "/dev/sdc"} {
		if !slicesContainPrefix(executor.calls, lvmCommandPrefix("pvremove", "-y", device)) {
			t.Fatalf("expected the physical volume label on %v to be removed again: %v", device, executor.calls)
		}
		if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", device)) {
			t.Fatalf("expected %v to leave the devices file again: %v", device, executor.calls)
		}
	}
}

func TestLVMDiskCreateReusesOrphanPV(t *testing.T) {
	// A PV left behind by an interrupted creation is not labelled again.
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", ""),
		lvmCommandPrefix("vgs", "--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "--select", "vg_name=longhorn-vg-"): lvmVGLine("longhorn-vg-x", "VG-1", VGTag, 0),
	}}
	ops := testDisk(executor)

	if _, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"}); err != nil {
		t.Fatalf("DiskCreate failed: %v", err)
	}
	if slicesContainPrefix(executor.calls, "pvcreate") || slicesContainPrefix(executor.calls, "wipefs") {
		t.Fatalf("an existing physical volume must be reused: %v", executor.calls)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("vgcreate", "--addtag", VGTag)) {
		t.Fatalf("expected vgcreate: %v", executor.calls)
	}
}

func TestLVMDiskCreateAdoptsEmptyVolumeGroup(t *testing.T) {
	executor := &fakeExecutor{
		outputSequences: map[string][]string{
			lvmVGListPrefix("data-vg"): {
				"", // not visible until its devices are imported
				lvmVGLine("data-vg", "VG-9", "", 0),
				lvmVGLine("data-vg", "VG-9", VGTag, 0),
			},
		},
	}
	ops := testDisk(executor)

	diskInfo, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "data-vg"})
	if err != nil {
		t.Fatalf("DiskCreate failed: %v", err)
	}
	if diskInfo.Uuid != "VG-9" || diskInfo.Path != "data-vg" {
		t.Fatalf("unexpected disk info: %+v", diskInfo)
	}
	for _, expected := range []string{
		lvmCommandPrefix("vgimportdevices", "data-vg"),
		lvmCommandPrefix("vgchange", "--addtag", VGTag, "data-vg"),
	} {
		if !slicesContainPrefix(executor.calls, expected) {
			t.Fatalf("expected %q, calls: %v", expected, executor.calls)
		}
	}
	for _, forbidden := range []string{"pvcreate", "vgcreate", "wipefs", "lvmdevices"} {
		if slicesContainPrefix(executor.calls, forbidden) {
			t.Fatalf("%v must not run when adopting a volume group: %v", forbidden, executor.calls)
		}
	}
}

func TestLVMDiskCreateAdoptsManagedVolumeGroupAgain(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGListPrefix("data-vg"): lvmVGLine("data-vg", "VG-9", VGTag, 1),
	}}
	ops := testDisk(executor)

	if _, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/data-vg"}); err != nil {
		t.Fatalf("DiskCreate failed: %v", err)
	}
	for _, forbidden := range []string{"vgimportdevices", "vgchange"} {
		if slicesContainPrefix(executor.calls, forbidden) {
			t.Fatalf("%v must not run for a volume group Longhorn already manages: %v", forbidden, executor.calls)
		}
	}
}

func TestLVMDiskCreateRejectsVolumeGroupWithLVs(t *testing.T) {
	executor := &fakeExecutor{
		outputs: map[string]string{
			lvmLVListPrefix("data-vg"): lvmLVReport("data-vg", "home"),
			lvmPVListPrefix("data-vg"): lvmPVLine("/dev/sdc", "data-vg"),
		},
		outputSequences: map[string][]string{
			lvmVGListPrefix("data-vg"): {"", lvmVGLine("data-vg", "VG-9", "", 1)},
		},
	}
	ops := testDisk(executor)

	_, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "data-vg"})
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "home") {
		t.Fatalf("DiskCreate error = %v, want FailedPrecondition naming the logical volume", err)
	}
	if slicesContainPrefix(executor.calls, "vgchange") {
		t.Fatalf("a volume group with logical volumes must not be tagged: %v", executor.calls)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdc")) {
		t.Fatalf("the imported devices must leave the devices file again: %v", executor.calls)
	}
}

func TestLVMDiskCreateRejectsUnknownPath(t *testing.T) {
	executor := &fakeExecutor{errs: map[string]error{
		lvmCommandPrefix("vgimportdevices", "no-such-vg"): fmt.Errorf("Volume group \"no-such-vg\" not found"),
	}}
	ops := testDisk(executor)

	_, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "no-such-vg"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("DiskCreate error code = %v, want %v: %v", status.Code(err), codes.InvalidArgument, err)
	}

	_, err = ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb;data-vg"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("DiskCreate error code = %v, want %v for a mixed path: %v", status.Code(err), codes.InvalidArgument, err)
	}
}

func TestLVMDiskDelete(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGByUUIDPrefix("VG-1"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
		lvmPVListAllPrefix(): lvmPVLine("/dev/sdb", "longhorn-vg-a") +
			lvmPVLine("/dev/sdc", "longhorn-vg-a"),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "longhorn-vg-a") +
			lvmPVLine("/dev/sdc", "longhorn-vg-a"),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
	}}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{
		DiskType: rpc.DiskType_lvm, DiskName: "disk-1", DiskUuid: "VG-1", DiskPath: "/dev/disk/by-id/scsi-3600062b2;/dev/sdc",
	}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	for _, expected := range []string{
		lvmCommandPrefix("vgremove", "-y", "longhorn-vg-a"),
		lvmCommandPrefix("pvremove", "-y", "/dev/sdb"),
		lvmCommandPrefix("pvremove", "-y", "/dev/sdc"),
		lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdb"),
		lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdc"),
	} {
		if !slicesContainPrefix(executor.calls, expected) {
			t.Fatalf("expected %q, calls: %v", expected, executor.calls)
		}
	}
}

func TestLVMDiskDeleteAdoptedVolumeGroup(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGByUUIDPrefix("VG-9"):  lvmVGLine("data-vg", "VG-9", VGTag, 0),
		lvmPVListPrefix("data-vg"): lvmPVLine("/dev/sdc", "data-vg"),
		lvmVGListPrefix("data-vg"): lvmVGLine("data-vg", "VG-9", VGTag, 0),
	}}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskUuid: "VG-9", DiskPath: "data-vg"}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	for _, expected := range []string{
		lvmCommandPrefix("vgremove", "-y", "data-vg"),
		lvmCommandPrefix("pvremove", "-y", "/dev/sdc"),
		lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdc"),
	} {
		if !slicesContainPrefix(executor.calls, expected) {
			t.Fatalf("expected %q, calls: %v", expected, executor.calls)
		}
	}
}

func TestLVMDiskDeleteRefusesUUIDMismatch(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():             lvmPVLine("/dev/sdb", "longhorn-vg-a"),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
	}}
	ops := testDisk(executor)

	_, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskUuid: "VG-other", DiskPath: "/dev/disk/by-id/scsi-3600062b2"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskDelete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgremove") || slicesContainPrefix(executor.calls, "pvremove") || slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev")) {
		t.Fatalf("nothing may be removed on a UUID mismatch: %v", executor.calls)
	}
}

func TestLVMDiskDeleteRefusesVolumeGroupWithLVs(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGByUUIDPrefix("VG-1"):        lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 1),
		lvmPVListAllPrefix():             lvmPVLine("/dev/sdb", "longhorn-vg-a"),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 1),
		lvmLVListPrefix("longhorn-vg-a"): lvmLVReport("longhorn-vg-a", "replica-1"),
	}}
	ops := testDisk(executor)

	_, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskUuid: "VG-1", DiskPath: "/dev/sdb"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskDelete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgremove") || slicesContainPrefix(executor.calls, "pvremove") {
		t.Fatalf("a volume group with logical volumes must stay: %v", executor.calls)
	}
}

func TestLVMDiskDeleteRefusesForeignVolumeGroup(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGByUUIDPrefix("VG-rhel"): lvmVGLine("rhel", "VG-rhel", "", 0),
		lvmPVListAllPrefix():         lvmPVLine("/dev/sdb", "rhel"),
		lvmVGListPrefix("rhel"):      lvmVGLine("rhel", "VG-rhel", "", 0),
	}}
	ops := testDisk(executor)

	_, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskUuid: "VG-rhel", DiskPath: "/dev/sdb"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DiskDelete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if slicesContainPrefix(executor.calls, "vgremove") || slicesContainPrefix(executor.calls, "pvremove") {
		t.Fatalf("a volume group Longhorn does not manage must stay: %v", executor.calls)
	}
}

func TestLVMDiskDeleteWithoutVolumeGroupForgetsDevices(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{lvmPVListAllPrefix(): ""}}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskPath: "/dev/sdb;/dev/sdc"}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	for _, device := range []string{"/dev/sdb", "/dev/sdc"} {
		if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvmdevices", "--deldev", device)) {
			t.Fatalf("expected %v to leave the devices file: %v", device, executor.calls)
		}
	}
	if slicesContainPrefix(executor.calls, "vgremove") || slicesContainPrefix(executor.calls, "pvremove") {
		t.Fatalf("nothing else may run without a volume group: %v", executor.calls)
	}
}

func TestLVMDiskCreatePropagatesPVInspectionFailure(t *testing.T) {
	executor := &fakeExecutor{errs: map[string]error{
		lvmPVListAllPrefix(): fmt.Errorf("devices file is unavailable"),
	}}
	ops := testDisk(executor)

	_, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "devices file is unavailable") {
		t.Fatalf("unexpected DiskCreate error: %v", err)
	}
	if slicesContainPrefix(executor.calls, "pvcreate") {
		t.Fatalf("pvcreate must not run when the PV inspection fails: %v", executor.calls)
	}
}

func TestVGNameForDiskUUID(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGByUUIDPrefix("VG-1"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
	}}
	name, err := VGNameForDiskUUID(executor, "VG-1")
	if err != nil || name != "longhorn-vg-a" {
		t.Fatalf("VGNameForDiskUUID = %q, %v; want longhorn-vg-a", name, err)
	}
	if _, err := VGNameForDiskUUID(executor, "VG-missing"); err == nil {
		t.Fatal("an unknown volume group UUID must be an error")
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

func TestLVMDiskDeleteWithoutPathOrUUIDIsNoop(t *testing.T) {
	// A disk that never became ready is deleted by the manager with the
	// empty path and UUID of its status.
	executor := &fakeExecutor{}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1"}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	if len(executor.calls) != 0 {
		t.Fatalf("no LVM command may run without a path or UUID: %v", executor.calls)
	}
}

func TestLVMDiskDeleteByUUIDOnly(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGByUUIDPrefix("VG-1"):        lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "longhorn-vg-a"),
	}}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskUuid: "VG-1"}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	for _, expected := range []string{
		lvmCommandPrefix("vgremove", "-y", "longhorn-vg-a"),
		lvmCommandPrefix("pvremove", "-y", "/dev/sdb"),
		lvmCommandPrefix("lvmdevices", "--deldev", "/dev/sdb"),
	} {
		if !slicesContainPrefix(executor.calls, expected) {
			t.Fatalf("expected %q, calls: %v", expected, executor.calls)
		}
	}
}

func TestLVMDiskDeleteByUUIDIsIdempotent(t *testing.T) {
	// The volume group is already gone; deleting the disk again must succeed
	// without touching anything.
	executor := &fakeExecutor{}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskUuid: "VG-gone"}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	if slicesContainPrefix(executor.calls, "vgremove") || slicesContainPrefix(executor.calls, "pvremove") || slicesContainPrefix(executor.calls, "lvmdevices") {
		t.Fatalf("nothing may be removed when the volume group is gone: %v", executor.calls)
	}
}

func TestLVMDiskDeleteIgnoresVanishedDevicePath(t *testing.T) {
	// The devices the disk was registered with are gone, but the volume
	// group still exists on others and the UUID finds it.
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGByUUIDPrefix("VG-1"):        lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 0),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdc", "longhorn-vg-a"),
	}}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskUuid: "VG-1", DiskPath: "/dev/disk/by-id/gone"}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("vgremove", "-y", "longhorn-vg-a")) {
		t.Fatalf("the volume group found by UUID must be removed: %v", executor.calls)
	}
}

func TestLVMDiskDeleteRemovesEmptyThinPool(t *testing.T) {
	pool := fmt.Sprintf(`{"report":[{"lv":[{"vg_name":"longhorn-vg-a","lv_name":%q,"lv_size":"53687091200","lv_active":"active","lv_path":"","lv_tags":"","lv_attr":"twi-a-tz--","pool_lv":"","data_percent":"0.00"}]}]}`, ThinPoolName)
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGByUUIDPrefix("VG-1"):        lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 1),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 1),
		lvmPVListPrefix("longhorn-vg-a"): lvmPVLine("/dev/sdb", "longhorn-vg-a"),
		lvmLVListPrefix("longhorn-vg-a"): pool,
	}}
	ops := testDisk(executor)

	if _, err := ops.Delete(&rpc.DiskDeleteRequest{DiskName: "disk-1", DiskUuid: "VG-1", DiskPath: "/dev/sdb"}); err != nil {
		t.Fatalf("DiskDelete failed: %v", err)
	}
	for _, expected := range []string{
		lvmCommandPrefix("lvremove", "-y", "longhorn-vg-a/"+ThinPoolName),
		lvmCommandPrefix("vgremove", "-y", "longhorn-vg-a"),
	} {
		if !slicesContainPrefix(executor.calls, expected) {
			t.Fatalf("expected %q, calls: %v", expected, executor.calls)
		}
	}
}

func TestLVMDiskGetReportsGrowableCapacity(t *testing.T) {
	// A 10 GiB pool, half used, in a 100 GiB group with 50 GiB free: the pool
	// can take 45 GiB more, so the disk reports 55 GiB without touching it.
	pool := fmt.Sprintf(`{"report":[{"lv":[{"vg_name":"longhorn-vg-a","lv_name":%q,"lv_size":"10737418240","lv_active":"active","lv_path":"","lv_tags":"","lv_attr":"twi-a-tz--","pool_lv":"","data_percent":"50.00"}]}]}`, ThinPoolName)
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():             lvmPVLine("/dev/sdb", "longhorn-vg-a"),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 1),
		lvmCommandPrefix("lvs", "--reportformat", "json", "--units", "b", "--nosuffix", "--select", "vg_name=longhorn-vg-a && lv_name="+ThinPoolName, "-o", lvmLogicalVolumeFields): pool,
	}}
	ops := testDisk(executor)

	diskInfo, err := ops.Get(&rpc.DiskGetRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if err != nil {
		t.Fatalf("DiskGet failed: %v", err)
	}
	if slicesContainPrefix(executor.calls, "lvextend") {
		t.Fatalf("DiskGet must not modify the thin pool: %v", executor.calls)
	}
	if diskInfo.TotalSize != 59055800320 || diskInfo.FreeSize != 59055800320-5368709120 {
		t.Fatalf("unexpected capacity with growable volume group space: %+v", diskInfo)
	}
}

func TestGetOrCreateThinPoolGrowsIntoFreeCapacity(t *testing.T) {
	pool := fmt.Sprintf(`{"report":[{"lv":[{"vg_name":"longhorn-vg-a","lv_name":%q,"lv_size":"10737418240","lv_active":"active","lv_path":"","lv_tags":"","lv_attr":"twi-a-tz--","pool_lv":"","data_percent":"0.00"}]}]}`, ThinPoolName)
	executor := &fakeExecutor{outputs: map[string]string{
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", VGTag, 1),
		lvmCommandPrefix("lvs", "--reportformat", "json", "--units", "b", "--nosuffix", "--select", "vg_name=longhorn-vg-a && lv_name="+ThinPoolName, "-o", lvmLogicalVolumeFields): pool,
	}}

	grown, err := GetOrCreateThinPool(executor, "longhorn-vg-a")
	if err != nil {
		t.Fatalf("GetOrCreateThinPool failed: %v", err)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("lvextend", "-y", "-L", "+48318382080b", "longhorn-vg-a/"+ThinPoolName)) {
		t.Fatalf("the pool was not extended into the free capacity: %v", executor.calls)
	}
	if grown.Size != 10737418240+48318382080 {
		t.Fatalf("pool size after growth = %v", grown.Size)
	}
}

func TestLVMDiskCreateTagsUntaggedVolumeGroup(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		lvmPVListAllPrefix():             lvmPVLine("/dev/sdb", "longhorn-vg-a"),
		lvmVGListPrefix("longhorn-vg-a"): lvmVGLine("longhorn-vg-a", "VG-1", "", 0),
	}}
	ops := testDisk(executor)

	diskInfo, err := ops.Create(&rpc.DiskCreateRequest{DiskName: "disk-1", DiskPath: "/dev/sdb"})
	if err != nil {
		t.Fatalf("DiskCreate failed: %v", err)
	}
	if !slicesContainPrefix(executor.calls, lvmCommandPrefix("vgchange", "--addtag", VGTag, "longhorn-vg-a")) {
		t.Fatalf("a volume group Longhorn created before tagging must be tagged: %v", executor.calls)
	}
	if diskInfo.Uuid != "VG-1" {
		t.Fatalf("unexpected disk identity: %+v", diskInfo)
	}
}
