package lvm

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	lhtypes "github.com/longhorn/go-common-libs/types"
)

// Executor terminates the complete LVM process group on timeout so a
// timed-out command cannot keep a volume-group lock after the caller returns.
type Executor struct{}

func NewExecutor() *Executor {
	return &Executor{}
}

func (e *Executor) Execute(envs []string, binary string, args []string, timeout time.Duration) (string, error) {
	return e.execute(envs, binary, args, "", timeout)
}

func (e *Executor) ExecuteWithStdin(binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	return e.execute(nil, binary, args, stdinString, timeout)
}

func (e *Executor) ExecuteWithStdinPipe(binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	return e.execute(nil, binary, args, stdinString, timeout)
}

func (e *Executor) execute(envs []string, binary string, args []string, stdinString string, timeout time.Duration) (string, error) {
	ctx := context.Background()
	cancel := func() {}
	if timeout != lhtypes.ExecuteNoTimeout {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), envs...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	if stdinString != "" {
		cmd.Stdin = strings.NewReader(stdinString)
	}

	var output, stderr bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &stderr
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return "", fmt.Errorf("timeout executing: %v %v", binary, args)
	}
	if err != nil {
		return output.String(), fmt.Errorf("failed to execute: %v %v, output %s, stderr %s: %w",
			binary, args, output.String(), stderr.String(), err)
	}
	return output.String(), nil
}
