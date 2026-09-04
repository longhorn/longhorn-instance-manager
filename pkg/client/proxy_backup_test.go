package client

import (
	"reflect"
	"testing"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

func TestConvertProxyResponseToBackupRestoreStatus(t *testing.T) {
	recv := &rpc.EngineBackupRestoreStatusProxyResponse{
		Status: map[string]*rpc.EngineBackupRestoreStatus{
			"tcp://10.0.0.1:20001": {
				IsRestoring:            true,
				LastRestored:           "backup-1",
				CurrentRestoringBackup: "backup-2",
				Progress:               42,
				Error:                  "replica error",
				Filename:               "volume-head-001.img",
				State:                  "in_progress",
				BackupUrl:              "s3://bucket@region/path",
			},
		},
		EngineError: "engine-level restore error",
	}

	resp := convertProxyResponseToBackupRestoreStatus(recv)

	want := &BackupRestoreStatusResponse{
		Status: map[string]*BackupRestoreStatus{
			"tcp://10.0.0.1:20001": {
				IsRestoring:            true,
				LastRestored:           "backup-1",
				CurrentRestoringBackup: "backup-2",
				Progress:               42,
				Error:                  "replica error",
				Filename:               "volume-head-001.img",
				State:                  "in_progress",
				BackupURL:              "s3://bucket@region/path",
			},
		},
		EngineError: "engine-level restore error",
	}
	if !reflect.DeepEqual(resp, want) {
		t.Errorf("conversion mismatch:\ngot  %+v\nwant %+v", resp, want)
	}
}

// TestConvertProxyResponseToBackupRestoreStatusFieldCoverage guards against
// silent field drops: if a field is added to either struct without extending
// the conversion, this test fails and points at the missing field.
func TestConvertProxyResponseToBackupRestoreStatusFieldCoverage(t *testing.T) {
	if got, want := reflect.TypeOf(BackupRestoreStatus{}).NumField(), 8; got != want {
		t.Errorf("BackupRestoreStatus has %d fields, conversion and tests cover %d; update convertProxyResponseToBackupRestoreStatus and this test", got, want)
	}
	if got, want := reflect.TypeOf(BackupRestoreStatusResponse{}).NumField(), 2; got != want {
		t.Errorf("BackupRestoreStatusResponse has %d fields, conversion and tests cover %d; update convertProxyResponseToBackupRestoreStatus and this test", got, want)
	}
}

func TestConvertProxyResponseToBackupRestoreStatusEmpty(t *testing.T) {
	resp := convertProxyResponseToBackupRestoreStatus(&rpc.EngineBackupRestoreStatusProxyResponse{})
	if resp.EngineError != "" {
		t.Errorf("expected empty EngineError, got %q", resp.EngineError)
	}
	if len(resp.Status) != 0 {
		t.Errorf("expected empty status map, got %+v", resp.Status)
	}
	if resp.Status == nil {
		t.Error("expected non-nil status map")
	}
}
