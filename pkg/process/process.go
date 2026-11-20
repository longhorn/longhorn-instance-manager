package process

import (
	"maps"
	"sync"
	"syscall"
	"time"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

type State string

const (
	StateStarting = State(types.ProcessStateStarting)
	StateRunning  = State(types.ProcessStateRunning)
	StateStopping = State(types.ProcessStateStopping)
	StateStopped  = State(types.ProcessStateStopped)
	StateError    = State(types.ProcessStateError)
)

type Process struct {
	Name      string
	Binary    string
	Args      []string
	PortCount int32
	PortArgs  []string

	UUID              string
	State             State
	ErrorMsg          string
	Conditions        map[string]bool
	PortStart         int32
	PortEnd           int32
	DeletionTimestamp *time.Time

	lock     *sync.RWMutex
	cmd      Command
	UpdateCh chan *Process

	logger *util.LonghornWriter

	// done is closed exactly once, when the process reaches a terminal state
	// (stopped or error). Callers that have to wait for a process to go away,
	// such as unregisterProcess, wait on it instead of polling a fixed window.
	done     chan struct{}
	doneOnce sync.Once
	// closeLoggerOnce makes sure the process log file is closed exactly once.
	closeLoggerOnce sync.Once

	// waitCount and waitInterval bound the graceful stop window. They default to
	// types.WaitCount / types.WaitInterval and tests shorten them.
	waitCount    int
	waitInterval time.Duration

	executor      Executor
	healthChecker HealthChecker
}

// gracefulStopWindow returns how long a process is given to exit after the stop
// signal before it is killed.
func (p *Process) gracefulStopWindow() (int, time.Duration) {
	count, interval := p.waitCount, p.waitInterval
	if count <= 0 {
		count = types.WaitCount
	}
	if interval <= 0 {
		interval = types.WaitInterval
	}
	return count, interval
}

// finish records the terminal state of the process and releases the waiters of
// p.done. Callers must not hold p.lock.
func (p *Process) finish(state State, errorMsg string) {
	p.lock.Lock()
	p.State = state
	p.ErrorMsg = errorMsg
	p.lock.Unlock()

	p.doneOnce.Do(func() {
		if p.done != nil {
			close(p.done)
		}
	})
}

// closeLogger closes the process log file. It is only safe to call once the
// child has exited, that is after cmd.Run() returned: cmd.Run() is what waits
// for the goroutine copying the child's stdout/stderr into the log writer. If
// the file is closed while that copier is still running, the copier reports
// "sync <log file>: file already closed" and cmd.Wait() returns that error
// instead of the exit status.
func (p *Process) closeLogger() {
	p.closeLoggerOnce.Do(func() {
		if err := p.logger.Close(); err != nil {
			logrus.WithError(err).Warnf("Process Manager: failed to close process %v logger", p.Name)
		}
	})
}

func (p *Process) Start() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	cmd, err := p.executor.NewCommand(p.Binary, p.Args...)
	if err != nil {
		p.State = StateError
		p.ErrorMsg = err.Error()
		p.closeLogger()
		p.doneOnce.Do(func() {
			if p.done != nil {
				close(p.done)
			}
		})
		return err
	}
	cmd.SetOutput(p.logger)
	p.cmd = cmd

	probeStopCh := make(chan struct{})
	go func() {
		runErr := cmd.Run()

		// cmd.Run() returns only after the child exited and the output copier is
		// done, so from here on nobody writes into the log file any more.
		p.closeLogger()

		close(probeStopCh)
		if runErr != nil {
			p.lock.RLock()
			stopRequested := p.DeletionTimestamp != nil
			p.lock.RUnlock()
			if stopRequested {
				// The exit status is the result of the stop (or of the kill that
				// follows the graceful window), not a process failure. Reporting it
				// as an error would also keep RPCResponse() logging that error on
				// every poll.
				logrus.Infof("Process Manager: process %v exited while stopping, error msg: %v", p.Name, runErr)
				p.finish(StateStopped, "")
			} else {
				logrus.Infof("Process Manager: process %v error out, error msg: %v", p.Name, runErr)
				p.finish(StateError, runErr.Error())
			}
			p.UpdateCh <- p
			return
		}
		logrus.Infof("Process Manager: process %v stopped", p.Name)
		p.finish(StateStopped, "")

		p.UpdateCh <- p
	}()

	go func() {
		if p.PortStart != 0 {
			address := util.GetURL("localhost", int(p.PortStart))
			if p.healthChecker.WaitForRunning(address, p.Name, probeStopCh) {
				p.lock.Lock()
				p.State = StateRunning
				p.lock.Unlock()
				p.UpdateCh <- p
				return
			}
			// fail to start the process, then try to stop it.
			if !p.IsStopped() {
				p.Stop()
			}
		} else {
			// Process Manager doesn't know the grpc address. directly set running state
			p.lock.Lock()
			p.State = StateRunning
			p.lock.Unlock()
			p.UpdateCh <- p
		}
	}()

	return nil
}

func (p *Process) RPCResponse() *rpc.ProcessResponse {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.ErrorMsg != "" {
		logrus.Warnf("Process update: %v: state %v: errorMsg: %v", p.Name, p.State, p.ErrorMsg)
	}
	return &rpc.ProcessResponse{
		Spec: &rpc.ProcessSpec{
			Name:      p.Name,
			Binary:    p.Binary,
			Args:      p.Args,
			PortCount: p.PortCount,
			PortArgs:  p.PortArgs,
		},

		Status: &rpc.ProcessStatus{
			State:     string(p.State),
			ErrorMsg:  p.ErrorMsg,
			PortStart: p.PortStart,
			PortEnd:   p.PortEnd,
			// Clone the conditions map. Returning the live map would let gRPC
			// marshal it after the lock is released, racing with the condition
			// updates done by the mount point check loop.
			Conditions: maps.Clone(p.Conditions),
			Uuid:       p.UUID,
		},
	}
}

func (p *Process) Stop() {
	p.StopWithSignal(syscall.SIGINT)
}

func (p *Process) StopWithSignal(signal syscall.Signal) {
	needStop, needTimeoutKill := false, false
	now := time.Now()

	p.lock.Lock()
	if p.State != StateStopping && p.State != StateStopped && p.State != StateError {
		p.State = StateStopping
		p.DeletionTimestamp = &now
		needStop = true
	}
	// Retry the deletion if the process is not stopped within the graceful window
	if p.DeletionTimestamp != nil && !needStop {
		count, interval := p.gracefulStopWindow()
		deleteTimeout := time.Duration(int64(interval) * int64(count))
		if p.DeletionTimestamp.Add(deleteTimeout).Before(now) {
			logrus.Infof("Process Manager: process %v deletion takes more than %vs, will retry it", p.Name, deleteTimeout.Seconds())
			needTimeoutKill = true
		}
	}
	p.lock.Unlock()

	if !needStop && !needTimeoutKill {
		// The first Stop call already sent the signal, escalates to Kill after the
		// graceful window, and the log file is closed once cmd.Run() returns.
		// Nothing else has to be done here.
		return
	}
	p.UpdateCh <- p

	p.lock.RLock()
	cmd := p.cmd
	p.lock.RUnlock()

	go func() {
		if cmd == nil || !cmd.IsRunning() {
			logrus.Errorf("Process Manager: cmd of %v is not running anymore, no need to stop", p.Name)
			if !p.IsStopped() {
				p.finish(StateStopped, "")
			}
			return
		}

		// no need for lock
		if needStop {
			count, interval := p.gracefulStopWindow()
			logrus.Infof("Process Manager: trying to stop process %v", p.Name)
			cmd.StopWithSignal(signal)
			for i := 0; i < count; i++ {
				if p.IsStopped() {
					return
				}
				logrus.Infof("Wait for process %v to shutdown", p.Name)
				time.Sleep(interval)
			}
			logrus.Warnf("Process Manager: cannot graceful stop process %v in %v, will kill the process", p.Name, time.Duration(count)*interval)
		} else if needTimeoutKill {
			logrus.Warnf("Process Manager: somehow timeout stopping process %v, will retry killing the process", p.Name)
		}
		cmd.Kill()
	}()
}

func (p *Process) IsStopped() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.State == StateStopped || p.State == StateError
}
