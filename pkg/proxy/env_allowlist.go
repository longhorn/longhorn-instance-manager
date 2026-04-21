package proxy

import (
	"fmt"
	"strings"

	btypes "github.com/longhorn/backupstore/types"
)

// backupEnvAllowlist enumerates every environment variable key that the proxy
// is permitted to forward from untrusted gRPC callers into the instance-manager
// process. Every entry corresponds to a credential or proxy setting consumed
// by github.com/longhorn/backupstore when talking to the configured backup
// target (S3, CIFS, Azure Blob).
//
// Anything outside this set — in particular the loader variables LD_PRELOAD,
// LD_LIBRARY_PATH, LD_AUDIT, as well as PATH, GOTRACEBACK, PYTHONPATH, NSS_* —
// is rejected. A caller that reaches the proxy gRPC port must not be able to
// influence how subsequent exec.Command invocations resolve binaries or load
// shared libraries into the (root, privileged) instance-manager container.
//
// See GHSA-wgh7-5vxp-4qr4.
var backupEnvAllowlist = map[string]struct{}{
	// S3 / AWS
	btypes.AWSAccessKey:       {},
	btypes.AWSSecretKey:       {},
	btypes.AWSEndPoint:        {},
	btypes.AWSCert:            {},
	btypes.VirtualHostedStyle: {},

	// CIFS
	btypes.CIFSUsername: {},
	btypes.CIFSPassword: {},

	// Azure Blob
	btypes.AZBlobAccountName: {},
	btypes.AZBlobAccountKey:  {},
	btypes.AZBlobEndpoint:    {},
	btypes.AZBlobCert:        {},

	// Outbound proxy (required so backupstore HTTP clients reach the target)
	btypes.HTTPSProxy: {},
	btypes.HTTPProxy:  {},
	btypes.NOProxy:    {},
}

// isBackupEnvAllowed reports whether key is safe to propagate into the
// instance-manager process environment.
func isBackupEnvAllowed(key string) bool {
	_, ok := backupEnvAllowlist[key]
	return ok
}

// validateBackupEnv returns a non-nil error if any entry in envs is malformed
// or carries a key that is not a member of backupEnvAllowlist. It never
// mutates process state; callers must invoke it as a gate before calling
// os.Setenv, so that a single unsafe key rejects the whole request and no
// partial state leaks into the running process.
func validateBackupEnv(envs []string) error {
	for _, env := range envs {
		key, _, ok := strings.Cut(env, "=")
		if !ok {
			return fmt.Errorf("malformed env entry %q: expected KEY=VALUE", env)
		}
		if key == "" {
			return fmt.Errorf("malformed env entry %q: empty key", env)
		}
		if !isBackupEnvAllowed(key) {
			return fmt.Errorf("env key %q is not permitted by the backup env allowlist", key)
		}
	}
	return nil
}
