package proxy

import (
	"os"
	"strings"
	"testing"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	spdktypes "github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

// TestSetEnv_RejectsUnsafeLoaderKeys is the direct regression test for
// GHSA-wgh7-5vxp-4qr4.
func TestSetEnv_RejectsUnsafeLoaderKeys(t *testing.T) {
	unsafeKeys := []string{
		"LD_PRELOAD",
		"LD_LIBRARY_PATH",
		"LD_AUDIT",
		"PATH",
		"GOTRACEBACK",
		"PYTHONPATH",
		"NSS_WRAPPER_PASSWD",
	}

	for _, key := range unsafeKeys {
		t.Run(key, func(t *testing.T) {
			origVal, origSet := os.LookupEnv(key)
			if err := os.Unsetenv(key); err != nil {
				t.Fatalf("precondition: unable to clear %s: %v", key, err)
			}
			t.Cleanup(func() {
				if origSet {
					_ = os.Setenv(key, origVal)
				} else {
					_ = os.Unsetenv(key)
				}
			})

			err := setEnv([]string{key + "=/tmp/evil.so"})
			if err == nil {
				t.Fatalf("setEnv(%s=...) returned nil error; expected allowlist rejection", key)
			}
			if !strings.Contains(err.Error(), "not permitted") {
				t.Fatalf("setEnv(%s=...) returned %q; expected 'not permitted by the backup env allowlist'", key, err)
			}
			if got, ok := os.LookupEnv(key); ok {
				t.Fatalf("setEnv rejected %s but it is now set to %q; process state must not change on rejection", key, got)
			}
		})
	}
}

func TestSetEnv_RejectsMalformedEntries(t *testing.T) {
	cases := []struct {
		name string
		env  string
	}{
		{"no separator", "LDPRELOAD"},
		{"empty key", "=value"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := setEnv([]string{tc.env}); err == nil {
				t.Fatalf("setEnv(%q) returned nil error; expected rejection", tc.env)
			}
		})
	}
}

func TestSetEnv_AcceptsAllowlistedCredentials(t *testing.T) {
	allowlisted := []string{
		"AWS_ACCESS_KEY_ID",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_ENDPOINTS",
		"AWS_CERT",
		"VIRTUAL_HOSTED_STYLE",
		"CIFS_USERNAME",
		"CIFS_PASSWORD",
		"AZBLOB_ACCOUNT_NAME",
		"AZBLOB_ACCOUNT_KEY",
		"AZBLOB_ENDPOINT",
		"AZBLOB_CERT",
		"HTTPS_PROXY",
		"HTTP_PROXY",
		"NO_PROXY",
	}

	saved := make(map[string]*string, len(allowlisted))
	for _, k := range allowlisted {
		if v, ok := os.LookupEnv(k); ok {
			s := v
			saved[k] = &s
		} else {
			saved[k] = nil
		}
	}
	t.Cleanup(func() {
		for k, v := range saved {
			if v == nil {
				_ = os.Unsetenv(k)
			} else {
				_ = os.Setenv(k, *v)
			}
		}
	})

	envs := make([]string, 0, len(allowlisted))
	for _, k := range allowlisted {
		envs = append(envs, k+"=proxy-allowlist-test")
	}
	if err := setEnv(envs); err != nil {
		t.Fatalf("setEnv(allowlisted credentials) returned %v; expected nil", err)
	}
	for _, k := range allowlisted {
		if got := os.Getenv(k); got != "proxy-allowlist-test" {
			t.Fatalf("setEnv did not export %s correctly; got %q", k, got)
		}
	}
}

func TestSetEnv_AtomicOnReject(t *testing.T) {
	const safeKey = "AWS_ACCESS_KEY_ID"
	const unsafeKey = "LD_PRELOAD"
	const marker = "proxy-atomicity-marker"

	origSafe, safeSet := os.LookupEnv(safeKey)
	origUnsafe, unsafeSet := os.LookupEnv(unsafeKey)
	_ = os.Unsetenv(safeKey)
	_ = os.Unsetenv(unsafeKey)
	t.Cleanup(func() {
		if safeSet {
			_ = os.Setenv(safeKey, origSafe)
		} else {
			_ = os.Unsetenv(safeKey)
		}
		if unsafeSet {
			_ = os.Setenv(unsafeKey, origUnsafe)
		} else {
			_ = os.Unsetenv(unsafeKey)
		}
	})

	err := setEnv([]string{
		safeKey + "=" + marker,
		unsafeKey + "=/tmp/evil.so",
	})
	if err == nil {
		t.Fatalf("setEnv with unsafe entry returned nil; expected rejection")
	}
	if got, ok := os.LookupEnv(safeKey); ok {
		t.Fatalf("setEnv returned an error but still exported %s=%q; must be atomic", safeKey, got)
	}
	if _, ok := os.LookupEnv(unsafeKey); ok {
		t.Fatalf("setEnv leaked the unsafe key %s into the environment", unsafeKey)
	}
}

func TestValidateBackupEnv_TableDriven(t *testing.T) {
	cases := []struct {
		name    string
		envs    []string
		wantErr bool
	}{
		{"empty slice", nil, false},
		{"single allowlisted", []string{"AWS_ACCESS_KEY_ID=x"}, false},
		{"allow empty value", []string{"HTTP_PROXY="}, false},
		{"LD_PRELOAD rejected", []string{"LD_PRELOAD=/tmp/x.so"}, true},
		{"mixed good and bad rejected", []string{"AWS_ACCESS_KEY_ID=x", "PATH=/bin"}, true},
		{"malformed no equals rejected", []string{"NOEQUALS"}, true},
		{"empty key rejected", []string{"=value"}, true},
		{"case-sensitive: aws_access_key_id rejected", []string{"aws_access_key_id=x"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateBackupEnv(tc.envs)
			if tc.wantErr && err == nil {
				t.Fatalf("validateBackupEnv(%v) returned nil; expected error", tc.envs)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("validateBackupEnv(%v) returned %v; expected nil", tc.envs, err)
			}
		})
	}
}

func TestFindV2BackupReplicaAddressesSortsRWReplicas(t *testing.T) {
	const (
		firstRWAddress  = "10.0.0.1:10000"
		secondRWAddress = "10.0.0.2:10000"
		nonRWAddress    = "10.0.0.3:10000"
	)
	replicaModeMap := map[string]spdktypes.Mode{
		"replica-2": spdktypes.ModeRW,
		"replica-3": spdktypes.ModeWO,
		"replica-1": spdktypes.ModeRW,
	}
	replicaAddressMap := map[string]string{
		"replica-1": firstRWAddress,
		"replica-2": secondRWAddress,
		"replica-3": nonRWAddress,
	}

	replicaAddresses := findV2BackupReplicaAddresses(replicaModeMap, replicaAddressMap)
	if len(replicaAddresses) != 2 {
		t.Fatalf("findV2BackupReplicaAddresses returned %v, expected two addresses", replicaAddresses)
	}
	if replicaAddresses[0] != firstRWAddress || replicaAddresses[1] != secondRWAddress {
		t.Fatalf("findV2BackupReplicaAddresses returned %v, expected [%s %s]", replicaAddresses, firstRWAddress, secondRWAddress)
	}
}

func TestFindV2BackupReplicaAddressesSkipsUnavailableRWReplicas(t *testing.T) {
	const availableAddress = "10.0.0.2:10000"
	replicaModeMap := map[string]spdktypes.Mode{
		"replica-1": spdktypes.ModeRW,
		"replica-2": spdktypes.ModeRW,
	}
	replicaAddressMap := map[string]string{
		"replica-1": "",
		"replica-2": availableAddress,
	}

	replicaAddresses := findV2BackupReplicaAddresses(replicaModeMap, replicaAddressMap)
	if len(replicaAddresses) != 1 || replicaAddresses[0] != availableAddress {
		t.Fatalf("findV2BackupReplicaAddresses returned %v, expected [%s]", replicaAddresses, availableAddress)
	}
}

func TestFindV2BackupReplicaAddressesReturnsEmptyWhenUnavailable(t *testing.T) {
	replicaModeMap := map[string]spdktypes.Mode{
		"replica-rw-1": spdktypes.ModeRW,
		"replica-rw-2": spdktypes.ModeRW,
		"replica-wo":   spdktypes.ModeWO,
	}
	replicaAddressMap := map[string]string{
		"replica-rw-1": "",
		"replica-wo":   "10.0.0.3:10000",
	}

	replicaAddresses := findV2BackupReplicaAddresses(replicaModeMap, replicaAddressMap)
	if len(replicaAddresses) != 0 {
		t.Fatalf("findV2BackupReplicaAddresses returned %v, expected no addresses", replicaAddresses)
	}
}

type fakeV2BackupStatusClient struct {
	responses map[string]*spdkrpc.BackupStatusResponse
	errors    map[string]error
	probed    []string
}

func (c *fakeV2BackupStatusClient) EngineBackupStatus(_, _, replicaAddress string) (*spdkrpc.BackupStatusResponse, error) {
	c.probed = append(c.probed, replicaAddress)
	return c.responses[replicaAddress], c.errors[replicaAddress]
}

func TestGetV2BackupStatusFromPartiallyBrokenReplicas(t *testing.T) {
	const (
		firstAddress  = "10.0.0.1:10000"
		secondAddress = "10.0.0.2:10000"
	)
	tests := []struct {
		name        string
		expectedErr error
	}{
		{
			name:        "NotFound",
			expectedErr: grpcstatus.Error(grpccodes.NotFound, "backup not found"),
		},
		{
			name:        "Unavailable",
			expectedErr: grpcstatus.Error(grpccodes.Unavailable, "replica unavailable"),
		},
		{
			name: "nil-status",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			expectedStatus := &spdkrpc.BackupStatusResponse{
				BackupUrl: "s3://backup-target/backup-1",
				Progress:  100,
				State:     "complete",
			}
			client := &fakeV2BackupStatusClient{
				responses: map[string]*spdkrpc.BackupStatusResponse{
					secondAddress: expectedStatus,
				},
				errors: map[string]error{
					firstAddress: tt.expectedErr,
				},
			}

			status, replicaAddress, err := getV2BackupStatusFromReplicas(client, "backup-1", "engine-1",
				[]string{firstAddress, secondAddress})
			if err != nil {
				t.Fatalf("getV2BackupStatusFromReplicas returned error: %v", err)
			}
			if status != expectedStatus || replicaAddress != secondAddress {
				t.Fatalf("getV2BackupStatusFromReplicas returned status %v and address %q, expected status %v and address %q",
					status, replicaAddress, expectedStatus, secondAddress)
			}
			if len(client.probed) != 2 || client.probed[0] != firstAddress || client.probed[1] != secondAddress {
				t.Fatalf("getV2BackupStatusFromReplicas probed %v, expected [%s %s]", client.probed, firstAddress, secondAddress)
			}
		})
	}
}

func TestGetV2BackupStatusFromReplicasPreservesFirstNonNotFoundError(t *testing.T) {
	const (
		firstAddress  = "10.0.0.1:10000"
		secondAddress = "10.0.0.2:10000"
	)
	firstErr := grpcstatus.Error(grpccodes.Unavailable, "replica unavailable")
	client := &fakeV2BackupStatusClient{
		responses: map[string]*spdkrpc.BackupStatusResponse{},
		errors: map[string]error{
			firstAddress:  firstErr,
			secondAddress: grpcstatus.Error(grpccodes.Internal, "replica failed"),
		},
	}

	_, _, err := getV2BackupStatusFromReplicas(client, "backup-1", "engine-1",
		[]string{firstAddress, secondAddress})
	if err != firstErr {
		t.Fatalf("getV2BackupStatusFromReplicas returned %v, expected first error %v", err, firstErr)
	}
	if len(client.probed) != 2 || client.probed[0] != firstAddress || client.probed[1] != secondAddress {
		t.Fatalf("getV2BackupStatusFromReplicas probed %v, expected [%s %s]", client.probed, firstAddress, secondAddress)
	}
}
