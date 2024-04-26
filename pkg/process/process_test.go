package process

import (
	"context"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
	"time"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	. "gopkg.in/check.v1"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

const (
	RetryCount        = 50
	RetryInterval     = 100 * time.Millisecond
	TestBinary        = "/engine-binaries/test/longhorn"
	TestBinaryMissing = "/engine-binaries/test-missing/longhorn"
	TestBinaryReplace = "/engine-binaries/test-replacement/longhorn"
)

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
