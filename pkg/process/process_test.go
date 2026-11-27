package process

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	. "gopkg.in/check.v1"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

const (
	RetryCount        = 50
	RetryInterval     = 100 * time.Millisecond
	TestBinary        = "/engine-binaries/test/longhorn"
	TestBinaryMissing = "/engine-binaries/test-missing/longhorn"
	TestBinaryReplace = "/engine-binaries/test-replacement/longhorn"

	testChildModeArg     = "child-process-mode="
	testChildParentArg   = "child-process-parent="
	testChildModeReplica = "replica"
	testChildModeOutput  = "output-holder"
	testChildReadyLine   = "child process is ready"
	testChildLastLine    = "last output line of the stopped child process"
)

// TestHelperProcessChild is not a test. It is the child process started by
// TestProcessUnregisteredAfterGracefulStopWindow through os.Args[0].
func TestHelperProcessChild(t *testing.T) {
	mode, parent := "", ""
	for _, arg := range os.Args {
		if strings.HasPrefix(arg, testChildModeArg) {
			mode = strings.TrimPrefix(arg, testChildModeArg)
		}
		if strings.HasPrefix(arg, testChildParentArg) {
			parent = strings.TrimPrefix(arg, testChildParentArg)
		}
	}

	switch mode {
	case testChildModeReplica:
		// Like a replica: exit 0 on SIGINT. The output holder inherits stdout and
		// stderr, like the sync agent does, and outlives this process.
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT)
		holder := exec.Command(os.Args[0], "-test.run=^TestHelperProcessChild$", testChildModeArg+testChildModeOutput,
			testChildParentArg+strconv.Itoa(os.Getpid()))
		holder.Stdout = os.Stdout
		holder.Stderr = os.Stderr
		if err := holder.Start(); err != nil {
			syscall.Exit(1)
		}
		_, _ = fmt.Fprintln(os.Stdout, testChildReadyLine)
		<-sigCh
		// Not os.Exit(), which sleeps for a second in binaries built with -race.
		syscall.Exit(0)
	case testChildModeOutput:
		// Keep the output pipe open for longer than the graceful stop window after
		// the parent exited, then write to it. The parent pid is passed in, since
		// this process may only start after the parent exited.
		for strconv.Itoa(os.Getppid()) == parent {
			time.Sleep(10 * time.Millisecond)
		}
		time.Sleep(time.Second)
		_, _ = fmt.Fprintln(os.Stderr, testChildLastLine)
		syscall.Exit(0)
	}
}

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
	shutdownCh chan error
	pm         *Manager
	logDir     string
}

var _ = Suite(&TestSuite{})

type ProcessWatcher struct {
	grpc.ServerStream
}

func (pw *ProcessWatcher) Context() context.Context {
	return context.Background()
}

func (pw *ProcessWatcher) Send(resp *rpc.ProcessResponse) error {
	//Do nothing for now, just act as the receiving end
	return nil
}

func (s *TestSuite) SetUpSuite(c *C) {
	var err error

	logrus.SetLevel(logrus.DebugLevel)
	s.shutdownCh = make(chan error)

	s.logDir = os.TempDir()
	s.pm, err = NewManager(context.Background(), "10000-30000", s.logDir)
	c.Assert(err, IsNil)
	s.pm.Executor = &MockExecutor{
		CreationHook: func(cmd *MockCommand) (*MockCommand, error) {
			if cmd.Binary == TestBinaryMissing {
				return nil, exec.ErrNotFound
			}

			return cmd, nil
		},
	}
	s.pm.HealthChecker = &MockHealthChecker{}
}

func (s *TestSuite) TearDownSuite(c *C) {
	close(s.shutdownCh)
}

func (s *TestSuite) TestCRUD(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	pw := &ProcessWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := "test_crud_process-" + strconv.Itoa(i)
			go func() {
				err := s.pm.ProcessWatch(nil, pw)
				c.Assert(err, IsNil)
			}()

			createReq := &rpc.ProcessCreateRequest{
				Spec: createProcessSpec(name, TestBinary),
			}
			createResp, err := s.pm.ProcessCreate(context.TODO(), createReq)
			c.Assert(err, IsNil)
			c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateStopping)
			c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateStopped)
			c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateError)

			getResp, err := s.pm.ProcessGet(context.TODO(), &rpc.ProcessGetRequest{
				Name: name,
			})
			c.Assert(err, IsNil)
			c.Assert(getResp.Spec.Name, Equals, name)
			c.Assert(getResp.Status.State, Not(Equals), types.ProcessStateStopping)
			c.Assert(getResp.Status.State, Not(Equals), types.ProcessStateStopped)
			c.Assert(getResp.Status.State, Not(Equals), types.ProcessStateError)

			listResp, err := s.pm.ProcessList(context.TODO(), &rpc.ProcessListRequest{})
			c.Assert(err, IsNil)
			c.Assert(listResp.Processes[name], NotNil)
			c.Assert(listResp.Processes[name].Spec.Name, Equals, name)
			c.Assert(listResp.Processes[name].Status.State, Not(Equals), types.ProcessStateStopping)
			c.Assert(listResp.Processes[name].Status.State, Not(Equals), types.ProcessStateStopped)
			c.Assert(listResp.Processes[name].Status.State, Not(Equals), types.ProcessStateError)

			running := false
			for j := 0; j < RetryCount; j++ {
				getResp, err := s.pm.ProcessGet(context.TODO(), &rpc.ProcessGetRequest{
					Name: name,
				})
				c.Assert(err, IsNil)
				if getResp.Status.State == types.ProcessStateRunning {
					running = true
					break
				}
				time.Sleep(RetryInterval)
			}
			c.Assert(running, Equals, true)

			deleteReq := &rpc.ProcessDeleteRequest{
				Name: name,
			}
			deleteResp, err := s.pm.ProcessDelete(context.TODO(), deleteReq)
			c.Assert(err, IsNil)
			c.Assert(deleteResp.Deleted, Equals, true)
			c.Assert(deleteResp.Status.State, Not(Equals), types.ProcessStateStarting)
			c.Assert(deleteResp.Status.State, Not(Equals), types.ProcessStateRunning)
			c.Assert(deleteResp.Status.State, Not(Equals), types.ProcessStateError)
		}(i)
	}
	wg.Wait()
}

// https://github.com/longhorn/longhorn/issues/1113
func (s *TestSuite) TestProcessDeletion(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	pw := &ProcessWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			go func() {
				err := s.pm.ProcessWatch(nil, pw)
				c.Assert(err, IsNil)
			}()
			name := "test_process_deletion-" + strconv.Itoa(i)

			assertProcessCreation(c, s.pm, name, TestBinary)
			assertProcessDeletion(c, s.pm, name)

			deleted, err := waitForProcessListState(s.pm, func(processes map[string]*rpc.ProcessResponse) bool {
				_, exists := processes[name]
				return !exists
			})
			c.Assert(err, IsNil)
			c.Assert(deleted, Equals, true)

			// after previous command delete the process of the same name, creating the process again
			// and make sure it does run and exist for deletion later
			assertProcessCreation(c, s.pm, name, TestBinary)
			assertProcessDeletion(c, s.pm, name)
		}(i)
	}
	wg.Wait()
}

// there was a deadlock when the im.monitor is processing an element
// from the updateChannel, it will try to RLock, to evaluate the existing
// processes. This will deadlock, if during that time a process is
// being replaced, since as part of the replacement. The old process
// when being stopped will try to sent the updated process on the update channel
// while the other scope has acquired a WriteLock.
// Since the IM is waiting on the ReadLock acquisition, while inside of the channel receive
// this will be a total deadlock, since the channel receive can never finish therefore all
// additional sents will be blocked.
// https://github.com/longhorn/longhorn/issues/2697
func (s *TestSuite) TestProcessReplace(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	pw := &ProcessWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			go func() {
				err := s.pm.ProcessWatch(nil, pw)
				c.Assert(err, IsNil)
			}()
			name := "test_process_replace-" + strconv.Itoa(i)
			assertProcessCreation(c, s.pm, name, TestBinary)
			assertProcessReplace(c, s.pm, name, TestBinaryReplace)

			assertProcessDeletion(c, s.pm, name)
			deleted, err := waitForProcessListState(s.pm, func(processes map[string]*rpc.ProcessResponse) bool {
				_, exists := processes[name]
				return !exists
			})
			c.Assert(err, IsNil)
			c.Assert(deleted, Equals, true)
		}(i)
	}
	wg.Wait()
}

// there was a race in the process UpdateChannel assignment
// this lead to a deadlock when the binary couldn't be located.
// https://github.com/longhorn/longhorn/issues/2697
func (s *TestSuite) TestProcessReplaceMissingBinary(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	pw := &ProcessWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			go func() {
				err := s.pm.ProcessWatch(nil, pw)
				c.Assert(err, NotNil)
			}()
			name := "test_process_missing_binary_replace-" + strconv.Itoa(i)
			assertProcessCreation(c, s.pm, name, TestBinary)

			// replacement for a missing binary should error
			_, err := s.pm.ProcessReplace(context.TODO(), &rpc.ProcessReplaceRequest{
				Spec:            createProcessSpec(name, TestBinaryMissing),
				TerminateSignal: "SIGHUP",
			})
			c.Assert(err, NotNil)
		}(i)
	}
	wg.Wait()
}

func (s *TestSuite) TestProcessReplaceInitFailureClosesLogger(c *C) {
	if _, err := os.Stat("/proc/self/fd"); err != nil {
		c.Skip("/proc/self/fd is not available")
	}

	name := "test_process_replace_missing_process"
	logPath, err := filepath.Abs(filepath.Join(s.logDir, name+".log"))
	c.Assert(err, IsNil)

	baseline, err := countOpenFileDescriptors(logPath)
	c.Assert(err, IsNil)

	_, err = s.pm.ProcessReplace(context.TODO(), &rpc.ProcessReplaceRequest{
		Spec:            createProcessSpec(name, TestBinaryReplace),
		TerminateSignal: "SIGHUP",
	})
	c.Assert(err, NotNil)
	c.Assert(status.Code(err), Equals, codes.NotFound)

	after, err := countOpenFileDescriptors(logPath)
	c.Assert(err, IsNil)
	c.Assert(after, Equals, baseline)
	_, err = os.Stat(logPath)
	c.Assert(err, IsNil)
	c.Assert(os.Remove(logPath), IsNil)
}

// there was a nil pointer case, while updating a process that is being
// deleted, since when initially checked the process was still in the map
// but by the time new process has started the old process had been removed
func (s *TestSuite) TestProcessReplaceDuringDeletion(c *C) {
	count := 100
	wg := &sync.WaitGroup{}
	pw := &ProcessWatcher{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			go func() {
				err := s.pm.ProcessWatch(nil, pw)
				c.Assert(err, NotNil)
			}()
			name := "test_process_deletion_replace-" + strconv.Itoa(i)
			assertProcessCreation(c, s.pm, name, TestBinary)
			assertProcessDeletion(c, s.pm, name)

			// TODO: we should change the deletion handling in a future version
			// since deletion happens async this might error or not
			// depending on if the process has already been deleted from the map
			replaceResp, err := s.pm.ProcessReplace(context.TODO(), &rpc.ProcessReplaceRequest{
				Spec:            createProcessSpec(name, TestBinaryReplace),
				TerminateSignal: "SIGHUP",
			})
			if err != nil {
				c.Assert(err, NotNil)
				c.Assert(status.Code(err), Equals, codes.NotFound)
				return
			}

			c.Assert(err, IsNil)
			c.Assert(replaceResp, NotNil)

			// wait for the replacement process to enter running
			running, err := waitForProcessState(s.pm, name, func(process *rpc.ProcessResponse) bool {
				return process.Status.State == types.ProcessStateRunning
			})
			c.Assert(err, IsNil)
			c.Assert(running, Equals, true)

			// wait for the replacement process cleanup
			assertProcessDeletion(c, s.pm, name)
			deleted, err := waitForProcessListState(s.pm, func(processes map[string]*rpc.ProcessResponse) bool {
				_, exists := processes[name]
				return !exists
			})
			c.Assert(err, IsNil)
			c.Assert(deleted, Equals, true)
		}(i)
	}
	wg.Wait()
}

// TestRPCResponseConditionsNotSharedWithProcess verifies that the conditions map
// returned by RPCResponse is a copy. Otherwise, marshalling the response (which
// happens after the process lock is released) races with the condition updates
// done by the mount point check loop, which crashes the instance manager with
// "fatal error: concurrent map iteration and map write".
func (s *TestSuite) TestRPCResponseConditionsNotSharedWithProcess(c *C) {
	p := &Process{
		Name:       "test_process_rpc_response_conditions",
		State:      StateRunning,
		Conditions: make(map[string]bool),
		lock:       &sync.RWMutex{},
	}

	resp := p.RPCResponse()
	resp.Status.Conditions[types.EngineConditionFilesystemReadOnly] = true
	p.lock.RLock()
	_, exists := p.Conditions[types.EngineConditionFilesystemReadOnly]
	p.lock.RUnlock()
	c.Assert(exists, Equals, false)

	stopCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stopCh:
				return
			default:
			}
			// Mimic getProcessesToUpdateConditions.
			p.lock.Lock()
			p.Conditions[types.EngineConditionFilesystemReadOnly] = i%2 == 0
			p.lock.Unlock()
		}
	}()

	for i := 0; i < 10000; i++ {
		// Mimic gRPC marshalling the response after the handler returned.
		_, err := proto.Marshal(p.RPCResponse())
		c.Assert(err, IsNil)
	}
	close(stopCh)
	wg.Wait()
}

func assertProcessReplace(c *C, pm *Manager, name, binary string) {
	replaceReq := &rpc.ProcessReplaceRequest{
		Spec:            createProcessSpec(name, binary),
		TerminateSignal: "SIGHUP",
	}
	rsp, err := pm.ProcessReplace(context.TODO(), replaceReq)

	c.Assert(err, IsNil)
	c.Assert(rsp, NotNil)
}

func assertProcessCreation(c *C, pm *Manager, name, binary string) {
	createReq := &rpc.ProcessCreateRequest{
		Spec: createProcessSpec(name, binary),
	}

	createResp, err := pm.ProcessCreate(context.TODO(), createReq)
	c.Assert(err, IsNil)
	c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateStopping)
	c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateStopped)
	c.Assert(createResp.Status.State, Not(Equals), types.ProcessStateError)

	createResp, err = pm.ProcessCreate(context.TODO(), createReq)
	c.Assert(createResp, IsNil)
	c.Assert(err, NotNil)
	c.Assert(status.Code(err), Equals, codes.AlreadyExists)

	running, err := waitForProcessState(pm, name, func(process *rpc.ProcessResponse) bool {
		return process.Status.State == types.ProcessStateRunning
	})
	c.Assert(err, IsNil)
	c.Assert(running, Equals, true)
}

func assertProcessDeletion(c *C, pm *Manager, name string) {
	count := 2
	for j := 0; j < count; j++ {
		deleteReq := &rpc.ProcessDeleteRequest{
			Name: name,
		}
		deleteResp, err := pm.ProcessDelete(context.TODO(), deleteReq)
		if err == nil {
			c.Assert(deleteResp.Deleted, Equals, true)
		} else {
			c.Assert(status.Code(err), Equals, codes.NotFound)
		}
	}
}

func createProcessSpec(name, binary string) *rpc.ProcessSpec {
	return &rpc.ProcessSpec{
		Name:      name,
		Binary:    binary,
		Args:      []string{},
		PortCount: 1,
		PortArgs:  nil,
	}
}

func waitForProcessState(pm *Manager, name string, predicate func(process *rpc.ProcessResponse) bool) (bool, error) {
	for j := 0; j < RetryCount; j++ {
		getResp, err := pm.ProcessGet(context.TODO(), &rpc.ProcessGetRequest{
			Name: name,
		})

		if err != nil {
			return false, err
		}
		if predicate(getResp) {
			return true, nil
		}
		time.Sleep(RetryInterval)
	}

	return false, nil
}

func waitForProcessListState(pm *Manager, predicate func(processes map[string]*rpc.ProcessResponse) bool) (bool, error) {
	// TODO: after a delete operation, it's kinda unexpected one would expect that the process is either gone from the response list
	// 	or that it has some marker that it's in the process of deletion unfortunately the deletion marker is only on the rpc response
	// 	and not the process struct so there is no way for the process list to signal deletion
	for j := 0; j < RetryCount; j++ {
		listResp, err := pm.ProcessList(context.TODO(), &rpc.ProcessListRequest{})
		if err != nil {
			return false, err
		}
		if predicate(listResp.Processes) {
			return true, nil
		}
		time.Sleep(RetryInterval)
	}
	return false, nil
}

func countOpenFileDescriptors(targetPath string) (int, error) {
	targetPath, err := filepath.Abs(targetPath)
	if err != nil {
		return 0, err
	}

	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0, err
	}

	count := 0
	for _, entry := range entries {
		resolvedPath, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err != nil {
			continue
		}
		resolvedPath, err = filepath.Abs(resolvedPath)
		if err != nil {
			continue
		}
		if resolvedPath == targetPath {
			count++
		}
	}

	return count, nil
}

// https://github.com/longhorn/longhorn/issues/14509
// A process whose output is still written after the graceful stop window has to
// be unregistered, its ports released and its last output logged.
func (s *TestSuite) TestProcessUnregisteredAfterGracefulStopWindow(c *C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logDir := c.MkDir()
	// A single port, so that a new process only starts if the port is released.
	pm, err := NewManager(ctx, "23000-23000", logDir)
	c.Assert(err, IsNil)
	pm.waitCount = 5
	pm.waitInterval = 100 * time.Millisecond
	pm.Executor = &BinaryExecutor{}
	pm.HealthChecker = &MockHealthChecker{}

	spec := func(name string) *rpc.ProcessSpec {
		return &rpc.ProcessSpec{
			Name:      name,
			Binary:    os.Args[0],
			Args:      []string{"-test.run=^TestHelperProcessChild$", testChildModeArg + testChildModeReplica},
			PortCount: 1,
		}
	}
	// Wait for the child to handle SIGINT before deleting the process.
	waitForChildReady := func(name string) bool {
		for i := 0; i < RetryCount; i++ {
			content, _ := os.ReadFile(filepath.Join(logDir, name+".log"))
			if strings.Contains(string(content), testChildReadyLine) {
				return true
			}
			time.Sleep(RetryInterval)
		}
		return false
	}

	name := "test_process_unregistered_after_graceful_stop_window"
	logPath := filepath.Join(logDir, name+".log")
	_, err = pm.ProcessCreate(context.TODO(), &rpc.ProcessCreateRequest{Spec: spec(name)})
	c.Assert(err, IsNil)
	p := pm.findProcess(name)
	c.Assert(p, NotNil)
	c.Assert(waitForChildReady(name), Equals, true)

	// longhorn-manager keeps deleting an instance until it is gone.
	for i := 0; i < 2; i++ {
		_, err = pm.ProcessDelete(context.TODO(), &rpc.ProcessDeleteRequest{Name: name})
		c.Assert(err, IsNil)
	}
	deleted, err := waitForProcessListState(pm, func(processes map[string]*rpc.ProcessResponse) bool {
		_, exists := processes[name]
		return !exists
	})
	c.Assert(err, IsNil)
	c.Assert(deleted, Equals, true)

	// The process exited with 0, so it must not be in error state because its
	// last output could not be written into a closed log file.
	status := p.RPCResponse().Status
	c.Assert(status.State, Equals, types.ProcessStateStopped)
	c.Assert(status.ErrorMsg, Equals, "")
	content, err := os.ReadFile(logPath)
	c.Assert(err, IsNil)
	c.Assert(strings.Contains(string(content), testChildLastLine), Equals, true)
	openFiles, err := countOpenFileDescriptors(logPath)
	c.Assert(err, IsNil)
	c.Assert(openFiles, Equals, 0)

	secondName := name + "-second"
	resp, err := pm.ProcessCreate(context.TODO(), &rpc.ProcessCreateRequest{Spec: spec(secondName)})
	c.Assert(err, IsNil)
	c.Assert(resp.Status.PortStart, Equals, int32(23000))
	c.Assert(waitForChildReady(secondName), Equals, true)
	_, err = pm.ProcessDelete(context.TODO(), &rpc.ProcessDeleteRequest{Name: secondName})
	c.Assert(err, IsNil)
	deleted, err = waitForProcessListState(pm, func(processes map[string]*rpc.ProcessResponse) bool {
		_, exists := processes[secondName]
		return !exists
	})
	c.Assert(err, IsNil)
	c.Assert(deleted, Equals, true)
}
