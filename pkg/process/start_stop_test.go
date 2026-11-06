package process

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

type commandReadyWriter struct {
	lock                sync.Mutex
	pending             string
	ready, signalled    chan struct{}
	readyOnce, stopOnce sync.Once
}

func (w *commandReadyWriter) Write(p []byte) (int, error) {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.pending += string(p)
	for {
		line, rest, complete := strings.Cut(w.pending, "\n")
		if !complete {
			break
		}
		w.pending = rest
		switch line {
		case "ready":
			w.readyOnce.Do(func() { close(w.ready) })
		case "signalled":
			w.stopOnce.Do(func() { close(w.signalled) })
		}
	}
	return len(p), nil
}

func TestHeldNativeChild(t *testing.T) {
	if os.Getenv("LONGHORN_START_STOP_HELD_CHILD") != "1" {
		return
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM)
	defer signal.Stop(signals)
	fmt.Println("ready")
	<-signals
	fmt.Println("signalled")
	if _, err := io.Copy(io.Discard, os.Stdin); err != nil {
		t.Fatal(err)
	}
}

func TestNativeCommandConcurrentRunPreservesRunningState(t *testing.T) {
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := NewBinaryCommand(binary, "-test.run=^TestHeldNativeChild$")
	if err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "LONGHORN_START_STOP_HELD_CHILD=1")
	reader, release, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close(); _ = release.Close() })
	cmd.Stdin = reader
	writer := &commandReadyWriter{ready: make(chan struct{}), signalled: make(chan struct{})}
	cmd.SetOutput(writer)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		_ = cmd.Run()
	}()
	t.Cleanup(func() {
		cmd.Kill()
		awaitStartStop(t, exited)
	})
	awaitStartStop(t, writer.ready)
	if err := cmd.Run(); err == nil {
		t.Fatal("second Run was accepted")
	}
	if !cmd.IsRunning() {
		t.Fatal("second Run cleared the first command's running state")
	}
	cmd.StopWithSignal(syscall.SIGTERM)
	awaitStartStop(t, writer.signalled)
	if !cmd.IsRunning() {
		t.Fatal("signal receipt was treated as actual process exit")
	}
	if err := cmd.Run(); err == nil {
		t.Fatal("second Run after Stop was accepted while shutdown remained pending")
	}
	if !cmd.IsRunning() {
		t.Fatal("second Run after Stop cleared the still-live command")
	}
	if err := release.Close(); err != nil {
		t.Fatal(err)
	}
	awaitStartStop(t, exited)
	if cmd.IsRunning() {
		t.Fatal("completed native Wait still reports running")
	}
}

func TestStartMarkerChild(t *testing.T) {
	marker := os.Getenv("LONGHORN_START_STOP_TEST_MARKER")
	if marker == "" {
		return
	}
	if err := os.WriteFile(marker, []byte("started"), 0600); err != nil {
		t.Fatal(err)
	}
}

func markerCommand(t *testing.T) (*BinaryCommand, string) {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := NewBinaryCommand(binary, "-test.run=^TestStartMarkerChild$")
	if err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "started")
	cmd.Env = append(os.Environ(), "LONGHORN_START_STOP_TEST_MARKER="+marker)
	return cmd, marker
}

func TestNativeCommandStopBeforeRun(t *testing.T) {
	for _, stop := range []string{"active", "stop", "signal", "kill"} {
		t.Run(stop, func(t *testing.T) {
			cmd, marker := markerCommand(t)
			switch stop {
			case "stop":
				cmd.Stop()
			case "signal":
				cmd.StopWithSignal(syscall.SIGINT)
			case "kill":
				cmd.Kill()
			}
			if err := cmd.Run(); err != nil {
				t.Fatal(err)
			}
			_, err := os.Stat(marker)
			if stop == "active" && err != nil {
				t.Fatalf("active control did not launch: %v", err)
			}
			if stop != "active" && !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("a stopped native command launched later: %v", err)
			}
		})
	}
}

type delayedNativeCommand struct {
	*BinaryCommand
	entered, release, checked, finished chan struct{}
	checkedOnce                         sync.Once
	releaseOnce                         sync.Once
}

func (c *delayedNativeCommand) Run() error {
	close(c.entered)
	<-c.release
	defer close(c.finished)
	return c.BinaryCommand.Run()
}

func (c *delayedNativeCommand) IsRunning() bool {
	running := c.BinaryCommand.IsRunning()
	c.checkedOnce.Do(func() { close(c.checked) })
	return running
}

type fixedStartExecutor struct{ cmd Command }

func (e fixedStartExecutor) NewCommand(string, ...string) (Command, error) { return e.cmd, nil }

type delayedHealthy struct{ entered, release chan struct{} }

func (h delayedHealthy) IsRunning(string) bool { return true }
func (h delayedHealthy) WaitForRunning(string, string, chan struct{}) bool {
	close(h.entered)
	<-h.release
	return true
}

type announcedCommand struct {
	started, stopped chan struct{}
	stopOnce         sync.Once
}

func (c *announcedCommand) Run() error          { close(c.started); <-c.stopped; return nil }
func (c *announcedCommand) SetOutput(io.Writer) {}
func (c *announcedCommand) IsRunning() bool {
	select {
	case <-c.started:
		select {
		case <-c.stopped:
			return false
		default:
			return true
		}
	default:
		return false
	}
}
func (c *announcedCommand) Stop()                         { c.stopOnce.Do(func() { close(c.stopped) }) }
func (c *announcedCommand) StopWithSignal(syscall.Signal) { c.Stop() }
func (c *announcedCommand) Kill()                         { c.Stop() }

func TestDelayedHealthCannotResurrectStoppedProcess(t *testing.T) {
	logger, err := util.NewLonghornWriter("late-health", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	health := delayedHealthy{entered: make(chan struct{}), release: make(chan struct{})}
	cmd := &announcedCommand{started: make(chan struct{}), stopped: make(chan struct{})}
	updates := make(chan *Process)
	p := &Process{Name: "late-health", PortStart: 12345, State: StateStarting, lock: &sync.RWMutex{}, logger: logger, executor: fixedStartExecutor{cmd}, healthChecker: health, UpdateCh: updates}
	var releaseOnce sync.Once
	started, stopStarted, received := false, false, 0
	stopReturned := make(chan struct{})
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(health.release) })
		cmd.Stop()
		if started {
			want := 2
			if stopStarted {
				want++
			}
			for received < want {
				select {
				case <-updates:
					received++
				case <-time.After(5 * time.Second):
					t.Error("held process worker did not finish during cleanup")
					return
				}
			}
		}
		if stopStarted {
			awaitStartStop(t, stopReturned)
		}
		_ = logger.Close()
	})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	started = true
	awaitStartStop(t, health.entered)
	awaitStartStop(t, cmd.started)
	stopStarted = true
	go func() {
		defer close(stopReturned)
		p.Stop()
	}()
	receiveUpdate := func() {
		select {
		case <-updates:
			received++
		case <-time.After(5 * time.Second):
			t.Fatal("process update did not complete")
		}
	}
	receiveUpdate() // Stop entered stopping before signaling the command.
	receiveUpdate() // Run has finished and published its terminal state.
	awaitStartStop(t, stopReturned)
	if !p.IsStopped() {
		t.Fatal("process did not stop before the delayed health result")
	}
	releaseOnce.Do(func() { close(health.release) })
	receiveUpdate()
	if !p.IsStopped() {
		t.Fatal("delayed health result resurrected the stopped process")
	}
}

func awaitStartStop(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("start/stop boundary did not complete")
	}
}

func TestProcessStopPreventsDelayedNativeRun(t *testing.T) {
	cmd, marker := markerCommand(t)
	held := &delayedNativeCommand{BinaryCommand: cmd, entered: make(chan struct{}), release: make(chan struct{}), checked: make(chan struct{}), finished: make(chan struct{})}
	logger, err := util.NewLonghornWriter("start-stop", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := &Process{Name: "start-stop", State: StateStarting, lock: &sync.RWMutex{}, logger: logger, executor: fixedStartExecutor{held}, UpdateCh: make(chan *Process, 10)}
	started := false
	t.Cleanup(func() {
		held.Kill()
		held.releaseOnce.Do(func() { close(held.release) })
		if started {
			awaitStartStop(t, held.finished)
		}
		_ = logger.Close()
	})
	if err := p.Start(); err != nil {
		t.Fatal(err)
	}
	started = true
	awaitStartStop(t, held.entered)
	p.Stop()
	awaitStartStop(t, held.checked)
	held.releaseOnce.Do(func() { close(held.release) })
	awaitStartStop(t, held.finished)
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Process.Stop allowed a delayed native launch: %v", err)
	}
}

func TestProcessDoesNotStartAfterStop(t *testing.T) {
	cmd, marker := markerCommand(t)
	logger, err := util.NewLonghornWriter("already-stopped", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	p := &Process{Name: "already-stopped", State: StateStopped, lock: &sync.RWMutex{}, logger: logger, executor: fixedStartExecutor{cmd}, UpdateCh: make(chan *Process, 10)}
	if err := p.Start(); err == nil {
		t.Fatal("Start accepted an already stopped process")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("terminal process launched: %v", err)
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
}
