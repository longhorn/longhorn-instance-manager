package process

import (
	"io"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/sirupsen/logrus"
)

type Executor interface {
	NewCommand(name string, arg ...string) (Command, error)
}

type Command interface {
	Run() error
	SetOutput(io.Writer)
	IsRunning() bool
	Stop()
	StopWithSignal(signal syscall.Signal)
	Kill()
}

type BinaryExecutor struct{}

func (be *BinaryExecutor) NewCommand(name string, arg ...string) (Command, error) {
	return NewBinaryCommand(name, arg...)
}

type BinaryCommand struct {
	*sync.RWMutex
	*exec.Cmd
}

func NewBinaryCommand(binary string, arg ...string) (*BinaryCommand, error) {
	var err error

	binary, err = exec.LookPath(binary)
	if err != nil {
		return nil, err
	}

	binary, err = filepath.Abs(binary)
	if err != nil {
		return nil, err
	}

	cmd := exec.Command(binary, arg...)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Pdeathsig: syscall.SIGKILL,
	}
	return &BinaryCommand{
		Cmd:     cmd,
		RWMutex: &sync.RWMutex{},
	}, nil
}

func (bc *BinaryCommand) SetOutput(writer io.Writer) {
	bc.Lock()
	defer bc.Unlock()
	bc.Stdout = writer
	bc.Stderr = writer
}

func (bc *BinaryCommand) IsRunning() bool {
	bc.RLock()
	defer bc.RUnlock()
	return bc.Process != nil && bc.ProcessState == nil
}

func (bc *BinaryCommand) StopWithSignal(signal syscall.Signal) {
	bc.RLock()
	defer bc.RUnlock()
	if bc.Process != nil {
		if err := bc.Process.Signal(signal); err != nil {
			logrus.WithError(err).Error("failed to send signal to process")
		}
	}
}

func (bc *BinaryCommand) Stop() {
	bc.RLock()
	defer bc.RUnlock()
	if bc.Process != nil {
		if err := bc.Process.Signal(syscall.SIGINT); err != nil {
			logrus.WithError(err).Error("failed to send signal to process")
		}
	}
}

func (bc *BinaryCommand) Kill() {
	bc.RLock()
	defer bc.RUnlock()
	if bc.Process != nil {
		if err := bc.Process.Signal(syscall.SIGKILL); err != nil {
			logrus.WithError(err).Error("failed to send signal to process")
		}
	}
}

type MockExecutor struct {
	CreationHook func(cmd *MockCommand) (*MockCommand, error)
}

func (me *MockExecutor) NewCommand(name string, arg ...string) (Command, error) {
	cmd := NewMockCommand(name, arg...)
	if me.CreationHook == nil {
		return cmd, nil
	}
	return me.CreationHook(NewMockCommand(name, arg...))
}

type MockCommand struct {
	*sync.RWMutex

	Binary string
	Args   []string

	stopCh chan error

	isRunning bool
	stopped   bool
}

func NewMockCommand(name string, arg ...string) *MockCommand {
	return &MockCommand{
		RWMutex: &sync.RWMutex{},

		Binary: name,
		Args:   arg,

		stopCh: make(chan error),

		isRunning: false,
		stopped:   false,
	}
}

func (mc *MockCommand) Run() error {
	mc.Lock()
	mc.isRunning = true
	mc.Unlock()

	return <-mc.stopCh
}

func (mc *MockCommand) SetOutput(writer io.Writer) {
}

func (mc *MockCommand) IsRunning() bool {
	mc.RLock()
	defer mc.RUnlock()
	return mc.isRunning
}

func (mc *MockCommand) Stop() {
	mc.Lock()
	mc.stopped = true
	mc.Unlock()

	mc.stopCh <- nil
}

func (mc *MockCommand) StopWithSignal(signal syscall.Signal) {
	mc.Stop()
}

func (mc *MockCommand) Kill() {
}
