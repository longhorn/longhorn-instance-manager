package lvm

import (
	"strings"
	"testing"
	"time"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeInstanceExecutor struct {
	outputs map[string]string
	calls   []string
	onCall  func(string)
}

func (e *fakeInstanceExecutor) Execute(envs []string, binary string, args []string, timeout time.Duration) (string, error) {
	cmd := binary + " " + strings.Join(args, " ")
	e.calls = append(e.calls, cmd)
	if e.onCall != nil {
		e.onCall(cmd)
	}
	bestPrefix := ""
	bestOutput := ""
	for prefix, output := range e.outputs {
		if strings.HasPrefix(cmd, prefix) && len(prefix) > len(bestPrefix) {
			bestPrefix = prefix
			bestOutput = output
		}
	}
	if bestPrefix == "" && binary == "lvs" {
		return lvsEmptyReport, nil
	}
	if bestPrefix == "" && binary == "pvs" {
		switch {
		case strings.Contains(cmd, "pv_uuid=AbCdEf-1234"):
			return "longhorn-disk-1\n", nil
		case strings.Contains(cmd, "pv_uuid=GhIjKl-5678"):
			return "longhorn-other\n", nil
		}
	}
	return bestOutput, nil
}

func (e *fakeInstanceExecutor) ExecuteWithStdin(binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	return "", nil
}

func (e *fakeInstanceExecutor) ExecuteWithStdinPipe(binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	return "", nil
}

func (e *fakeInstanceExecutor) calledWithPrefix(prefix string) bool {
	for _, cmd := range e.calls {
		if strings.HasPrefix(cmd, prefix) {
			return true
		}
	}
	return false
}

const (
	lvsEmptyReport     = `{"report":[{"lv":[]}]}`
	lvsActiveReplica   = `{"report":[{"lv":[{"vg_name":"longhorn-disk-1","lv_name":"vol-1-r-abc","lv_size":"1073741824","lv_active":"active","lv_path":"/dev/longhorn-disk-1/vol-1-r-abc","lv_tags":""}]}]}`
	lvsInactiveReplica = `{"report":[{"lv":[{"vg_name":"longhorn-disk-1","lv_name":"vol-1-r-abc","lv_size":"1073741824","lv_active":"","lv_path":"/dev/longhorn-disk-1/vol-1-r-abc","lv_tags":""}]}]}`
	lvsTaggedReplica   = `{"report":[{"lv":[{"vg_name":"longhorn-disk-1","lv_name":"vol-1-r-abc","lv_size":"1073741824","lv_active":"active","lv_path":"/dev/longhorn-disk-1/vol-1-r-abc","lv_tags":"longhorn-engine=vol-1-e-0"}]}]}`
)

var lvsGetReplicaPrefix = lvmCommandPrefix("lvs", "--reportformat", "json", "--units", "b", "--nosuffix", "--select", "vg_name=longhorn-disk-1 && lv_name=vol-1-r-abc")

func TestLocalReplicaInstanceCreate(t *testing.T) {
	executor := &fakeInstanceExecutor{outputs: map[string]string{
		"vgs":               "  AbCdEf-1234\n",
		lvsGetReplicaPrefix: lvsActiveReplica,
	}}
	notified := 0
	ops := Instance{
		executor: executor,
		notify:   func() { notified++ },
		deviceStatus: func(string) (bool, bool, error) {
			return true, true, nil
		},
	}

	resp, err := ops.Create(&rpc.InstanceCreateRequest{
		Spec: &rpc.InstanceSpec{
			Name:       "vol-1-r-abc",
			Type:       "replica",
			DataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
			LocalInstanceSpec: &rpc.LocalInstanceSpec{
				Size:     1073741824,
				DiskName: "disk-1",
				DiskUuid: "AbCdEf-1234",
			},
		},
	})
	if err != nil {
		t.Fatalf("InstanceCreate failed: %v", err)
	}
	if resp.Status.State != "running" {
		t.Fatalf("unexpected state %v", resp.Status.State)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvcreate", "-y", "-n", "vol-1-r-abc", "--setautoactivation", "n", "-L", "1073741824b", "longhorn-disk-1")) {
		t.Fatalf("lvcreate not called as expected: %v", executor.calls)
	}
	if executor.calledWithPrefix("lvchange") || executor.calledWithPrefix("wipefs") {
		t.Fatalf("lvcreate should synchronously activate and initialize the LV without follow-up operations: %v", executor.calls)
	}
	if notified != 1 {
		t.Fatalf("expected one watch notification, got %v", notified)
	}
}

func TestLocalReplicaInstanceCreateThin(t *testing.T) {
	poolQueryPrefix := lvmCommandPrefix("lvs", "--reportformat", "json", "--units", "b", "--nosuffix", "--select", "vg_name=longhorn-disk-1 && lv_name=longhorn-thin-pool")
	poolReport := `{"report":[{"lv":[{"vg_name":"longhorn-disk-1","lv_name":"longhorn-thin-pool","lv_size":"101005852672","lv_active":"active","lv_path":"/dev/longhorn-disk-1/longhorn-thin-pool","lv_tags":"","lv_attr":"twi-a-tz--","pool_lv":"","data_percent":"0.00"}]}]}`
	thinReplicaReport := `{"report":[{"lv":[{"vg_name":"longhorn-disk-1","lv_name":"vol-1-r-abc","lv_size":"1073741824","lv_active":"active","lv_path":"/dev/longhorn-disk-1/vol-1-r-abc","lv_tags":"","lv_attr":"Vwi-a-t---","pool_lv":"longhorn-thin-pool","data_percent":"3.125"}]}]}`
	executor := &fakeInstanceExecutor{outputs: map[string]string{
		"vgs": lvsEmptyReport,
		lvmCommandPrefix("vgs", "--noheadings", "-o", "vg_uuid", "longhorn-disk-1"):                                                                  "AbCdEf-1234\n",
		lvmCommandPrefix("vgs", "--noheadings", "--units", "b", "--nosuffix", "--separator", ";", "-o", "vg_free,vg_extent_size", "longhorn-disk-1"): "106374021120;4194304\n",
		poolQueryPrefix:     lvsEmptyReport,
		lvsGetReplicaPrefix: lvsEmptyReport,
	}}
	executor.onCall = func(command string) {
		if strings.HasPrefix(command, lvmCommandPrefix("lvcreate", "-y", "--type", "thin-pool")) {
			executor.outputs[poolQueryPrefix] = poolReport
		}
		if strings.Contains(command, " -V 1073741824b -T longhorn-disk-1/longhorn-thin-pool") {
			executor.outputs[lvsGetReplicaPrefix] = thinReplicaReport
		}
	}
	ops := Instance{
		executor: executor,
		deviceStatus: func(string) (bool, bool, error) {
			return true, true, nil
		},
	}

	_, err := ops.Create(&rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{
		Name: "vol-1-r-abc", Type: "replica", DataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
		LocalInstanceSpec: &rpc.LocalInstanceSpec{
			Size: 1073741824, DiskName: "disk-1", DiskUuid: "AbCdEf-1234",
			ProvisioningMode: ProvisioningModeThin,
		},
	}})
	if err != nil {
		t.Fatalf("thin InstanceCreate failed: %v; calls: %v", err, executor.calls)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvcreate", "-y", "--type", "thin-pool", "-n", "longhorn-thin-pool")) {
		t.Fatalf("thin pool was not created: %v", executor.calls)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvcreate", "-y", "--type", "thin-pool", "-n", "longhorn-thin-pool", "-L", "101003034624b", "--zero", "y", "longhorn-disk-1")) {
		t.Fatalf("thin pool was not created with zeroing enabled: %v", executor.calls)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvcreate", "-y", "-n", "vol-1-r-abc", "-V", "1073741824b", "-T", "longhorn-disk-1/longhorn-thin-pool")) {
		t.Fatalf("thin LV was not created as expected: %v", executor.calls)
	}
	if executor.calledWithPrefix(lvmCommandPrefix("lvcreate", "-y", "-n", "vol-1-r-abc", "--setautoactivation")) {
		t.Fatalf("thin LV creation must not use unsupported --setautoactivation: %v", executor.calls)
	}
}

func TestLocalReplicaInstanceCreateUsesPVUUID(t *testing.T) {
	replica := strings.ReplaceAll(lvsActiveReplica, "longhorn-disk-1", "longhorn-other")
	executor := &fakeInstanceExecutor{outputs: map[string]string{"lvs": replica}}
	inspectedPath := ""
	ops := Instance{
		executor: executor,
		deviceStatus: func(path string) (bool, bool, error) {
			inspectedPath = path
			return true, true, nil
		},
	}

	_, err := ops.Create(&rpc.InstanceCreateRequest{
		Spec: &rpc.InstanceSpec{
			Name:       "vol-1-r-abc",
			Type:       "replica",
			DataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
			LocalInstanceSpec: &rpc.LocalInstanceSpec{
				Size:     1073741824,
				DiskName: "disk-1",
				DiskUuid: "GhIjKl-5678",
			},
		},
	})
	if err != nil {
		t.Fatalf("InstanceCreate failed: %v", err)
	}
	if inspectedPath != "/dev/longhorn-other/vol-1-r-abc" {
		t.Fatalf("unexpected logical volume path %v", inspectedPath)
	}
}

func TestLocalReplicaInstanceCreateRequiresImmediateConfirmation(t *testing.T) {
	executor := &fakeInstanceExecutor{outputs: map[string]string{"vgs": "  AbCdEf-1234\n"}}
	ops := Instance{executor: executor}

	_, err := ops.Create(&rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{
		Name: "vol-1-r-abc",
		Type: "replica",
		LocalInstanceSpec: &rpc.LocalInstanceSpec{
			Size: 1073741824, DiskName: "disk-1", DiskUuid: "AbCdEf-1234",
		},
	}})
	if status.Code(err) != codes.Internal {
		t.Fatalf("create error code = %v, want %v: %v", status.Code(err), codes.Internal, err)
	}
	lvsCalls := 0
	for _, call := range executor.calls {
		if strings.HasPrefix(call, "lvs ") {
			lvsCalls++
		}
	}
	if lvsCalls != 3 {
		t.Fatalf("expected one existence query, one thin-pool guard, and one post-create confirmation without polling, got %v calls: %v", lvsCalls, executor.calls)
	}
}

func TestLocalReplicaInstanceCreateAdoptsExistingLV(t *testing.T) {
	executor := &fakeInstanceExecutor{outputs: map[string]string{
		"vgs":               "  AbCdEf-1234\n",
		"lvs":               lvsActiveReplica,
		lvsGetReplicaPrefix: lvsActiveReplica,
	}}
	ops := Instance{
		executor: executor,
		deviceStatus: func(string) (bool, bool, error) {
			return true, true, nil
		},
	}

	_, err := ops.Create(&rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{
		Name: "vol-1-r-abc",
		Type: "replica",
		LocalInstanceSpec: &rpc.LocalInstanceSpec{
			Size: 1073741824, DiskName: "disk-1", DiskUuid: "AbCdEf-1234",
		},
	}})
	if err != nil {
		t.Fatalf("InstanceCreate failed: %v", err)
	}
	if executor.calledWithPrefix("lvcreate") {
		t.Fatalf("existing LV must not be recreated: %v", executor.calls)
	}
}

func TestLocalEngineInstanceCreate(t *testing.T) {
	executor := &fakeInstanceExecutor{outputs: map[string]string{"lvs": lvsActiveReplica}}
	ops := Instance{executor: executor, deviceStatus: func(string) (bool, bool, error) { return true, true, nil }}

	resp, err := ops.Create(&rpc.InstanceCreateRequest{
		Spec: &rpc.InstanceSpec{
			Name:       "vol-1-e-0",
			Type:       "engine",
			DataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
			LocalInstanceSpec: &rpc.LocalInstanceSpec{
				Size:        1073741824,
				ReplicaName: "vol-1-r-abc",
			},
		},
	})
	if err != nil {
		t.Fatalf("InstanceCreate failed: %v", err)
	}
	if resp.Status.State != "running" || resp.Status.Endpoint != "/dev/longhorn-disk-1/vol-1-r-abc" {
		t.Fatalf("unexpected engine response: %+v", resp.Status)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvchange", "--addtag", "longhorn-engine=vol-1-e-0", "longhorn-disk-1/vol-1-r-abc")) {
		t.Fatalf("addtag not called as expected: %v", executor.calls)
	}

	// Missing replica LV fails the engine creation.
	ops = Instance{executor: &fakeInstanceExecutor{outputs: map[string]string{}}}
	_, err = ops.Create(&rpc.InstanceCreateRequest{
		Spec: &rpc.InstanceSpec{
			Name:       "vol-1-e-0",
			Type:       "engine",
			DataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
			LocalInstanceSpec: &rpc.LocalInstanceSpec{
				ReplicaName: "vol-1-r-abc",
			},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("engine create error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
}

func TestLocalInstanceDelete(t *testing.T) {
	// Detach: deactivate only.
	executor := &fakeInstanceExecutor{outputs: map[string]string{
		"lvs":               lvsActiveReplica,
		lvsGetReplicaPrefix: lvsInactiveReplica,
	}}
	ops := Instance{executor: executor, deviceStatus: func(string) (bool, bool, error) {
		t.Fatal("detach must rely on LVM state without inspecting the device path")
		return false, false, nil
	}}
	if _, err := ops.Delete(&rpc.InstanceDeleteRequest{Name: "vol-1-r-abc"}); err != nil {
		t.Fatalf("InstanceDelete failed: %v", err)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvchange", "-an", "longhorn-disk-1/vol-1-r-abc")) || executor.calledWithPrefix("lvremove") {
		t.Fatalf("expected deactivate without removal: %v", executor.calls)
	}

	// Deletion: remove the LV after the disk UUID matches the VG.
	executor = &fakeInstanceExecutor{outputs: map[string]string{
		"lvs":               lvsActiveReplica,
		lvsGetReplicaPrefix: lvsEmptyReport,
		"vgs":               "  AbCdEf-1234\n",
	}}
	ops = Instance{executor: executor, deviceStatus: func(string) (bool, bool, error) {
		t.Fatal("delete must rely on LVM state without inspecting the device path")
		return false, false, nil
	}}
	if _, err := ops.Delete(&rpc.InstanceDeleteRequest{Name: "vol-1-r-abc", DiskUuid: "AbCdEf-1234", CleanupRequired: true}); err != nil {
		t.Fatalf("InstanceDelete with cleanup failed: %v", err)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvremove", "-y", "longhorn-disk-1/vol-1-r-abc")) {
		t.Fatalf("expected lvremove: %v", executor.calls)
	}

	// Deletion with a mismatching disk UUID is refused.
	executor = &fakeInstanceExecutor{outputs: map[string]string{"lvs": lvsActiveReplica, "vgs": "  AbCdEf-1234\n"}}
	ops = Instance{executor: executor}
	_, err := ops.Delete(&rpc.InstanceDeleteRequest{Name: "vol-1-r-abc", DiskUuid: "GhIjKl-5678", CleanupRequired: true})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("delete error code = %v, want %v: %v", status.Code(err), codes.FailedPrecondition, err)
	}
	if executor.calledWithPrefix("lvremove") {
		t.Fatalf("lvremove must not run on UUID mismatch: %v", executor.calls)
	}

	// Engine instance: remove the tag, keep the LV.
	executor = &fakeInstanceExecutor{outputs: map[string]string{"lvs": lvsTaggedReplica}}
	ops = Instance{executor: executor}
	if _, err := ops.Delete(&rpc.InstanceDeleteRequest{Name: "vol-1-e-0"}); err != nil {
		t.Fatalf("InstanceDelete of engine failed: %v", err)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvchange", "--deltag", "longhorn-engine=vol-1-e-0", "longhorn-disk-1/vol-1-r-abc")) || executor.calledWithPrefix("lvremove") {
		t.Fatalf("expected deltag without removal: %v", executor.calls)
	}

	// Missing instance: idempotent success.
	ops = Instance{executor: &fakeInstanceExecutor{outputs: map[string]string{}}}
	if _, err := ops.Delete(&rpc.InstanceDeleteRequest{Name: "gone", CleanupRequired: true}); err != nil {
		t.Fatalf("InstanceDelete of missing instance failed: %v", err)
	}
}

func TestLocalInstanceList(t *testing.T) {
	executor := &fakeInstanceExecutor{outputs: map[string]string{
		"lvs": `{"report":[{"lv":[
			{"vg_name":"longhorn-disk-1","lv_name":"vol-1-r-abc","lv_size":"1073741824","lv_active":"active","lv_path":"/dev/longhorn-disk-1/vol-1-r-abc","lv_tags":"longhorn-engine=vol-1-e-0"},
			{"vg_name":"longhorn-disk-1","lv_name":"vol-2-r-def","lv_size":"1073741824","lv_active":"","lv_path":"/dev/longhorn-disk-1/vol-2-r-def","lv_tags":""}
		]}]}`,
	}}
	ops := Instance{executor: executor, deviceStatus: func(string) (bool, bool, error) {
		t.Fatal("InstanceList must rely on LVM state without inspecting the device path")
		return false, false, nil
	}}

	instances := map[string]*rpc.InstanceResponse{}
	if err := ops.List(instances); err != nil {
		t.Fatalf("InstanceList failed: %v", err)
	}
	if len(instances) != 2 {
		t.Fatalf("expected replica and engine instances, got %v", instances)
	}
	if _, ok := instances["vol-1-r-abc"]; !ok {
		t.Fatalf("expected replica instance in %v", instances)
	}
	engine, ok := instances["vol-1-e-0"]
	if !ok || engine.Status.Endpoint != "/dev/longhorn-disk-1/vol-1-r-abc" {
		t.Fatalf("expected engine instance with endpoint in %v", instances)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvs", "--reportformat", "json")) {
		t.Fatalf("lvs did not use the Longhorn devices file: %v", executor.calls)
	}
}

func TestLocalInstanceGetNotFound(t *testing.T) {
	ops := Instance{executor: &fakeInstanceExecutor{outputs: map[string]string{}}}
	_, err := ops.Get(&rpc.InstanceGetRequest{Name: "gone"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("InstanceGet error code = %v, want %v: %v", status.Code(err), codes.NotFound, err)
	}
}

func TestLocalReplicaInstanceExpand(t *testing.T) {
	resizedReplica := strings.Replace(lvsActiveReplica, `"lv_size":"1073741824"`, `"lv_size":"2147483648"`, 1)
	executor := &fakeInstanceExecutor{outputs: map[string]string{
		"vgs":               "AbCdEf-1234\n",
		"lvs":               lvsActiveReplica,
		lvsGetReplicaPrefix: lvsActiveReplica,
	}}
	executor.onCall = func(cmd string) {
		if strings.HasPrefix(cmd, "lvextend ") {
			executor.outputs["lvs"] = resizedReplica
			executor.outputs[lvsGetReplicaPrefix] = resizedReplica
		}
	}
	notified := 0
	ops := Instance{
		executor: executor,
		notify:   func() { notified++ },
		deviceStatus: func(string) (bool, bool, error) {
			return true, true, nil
		},
	}

	resp, err := ops.Replace(&rpc.InstanceReplaceRequest{Spec: &rpc.InstanceSpec{
		Name:       "vol-1-r-abc",
		Type:       "replica",
		DataEngine: rpc.DataEngine_DATA_ENGINE_LOCAL,
		LocalInstanceSpec: &rpc.LocalInstanceSpec{
			Size:     2147483648,
			DiskName: "disk-1",
			DiskUuid: "AbCdEf-1234",
		},
	}})
	if err != nil {
		t.Fatalf("InstanceReplace failed: %v", err)
	}
	if resp.Status.State != "running" {
		t.Fatalf("unexpected state %v", resp.Status.State)
	}
	if !executor.calledWithPrefix(lvmCommandPrefix("lvextend", "-y", "-L", "2147483648b", "longhorn-disk-1/vol-1-r-abc")) {
		t.Fatalf("lvextend not called as expected: %v", executor.calls)
	}
	if notified != 1 {
		t.Fatalf("expected one watch notification, got %v", notified)
	}
}

func TestLocalReplicaInstanceExpandRejectsShrink(t *testing.T) {
	executor := &fakeInstanceExecutor{outputs: map[string]string{
		"vgs": "AbCdEf-1234\n",
		"lvs": lvsActiveReplica,
	}}
	ops := Instance{executor: executor}

	_, err := ops.Replace(&rpc.InstanceReplaceRequest{Spec: &rpc.InstanceSpec{
		Name: "vol-1-r-abc",
		Type: "replica",
		LocalInstanceSpec: &rpc.LocalInstanceSpec{
			Size:     536870912,
			DiskName: "disk-1",
			DiskUuid: "AbCdEf-1234",
		},
	}})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("InstanceReplace error code = %v, want %v: %v", status.Code(err), codes.InvalidArgument, err)
	}
	if executor.calledWithPrefix("lvextend") {
		t.Fatalf("lvextend must not run for a shrink: %v", executor.calls)
	}
}
