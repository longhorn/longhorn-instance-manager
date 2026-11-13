package process

import (
	"context"
	"errors"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

type createPreparationHook struct {
	message          string
	entered, release chan struct{}
	once             sync.Once
}

func TestProcessCreateCanceledWhileRegistrationWaits(t *testing.T) {
	testContext, stopTest := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopTest()
	managerContext, stopManager := context.WithCancel(context.Background())
	t.Cleanup(stopManager)
	pm, err := NewManager(managerContext, "10000-10010", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var startupAttempts atomic.Int32
	pm.Executor = &MockExecutor{CreationHook: func(*MockCommand) (*MockCommand, error) {
		startupAttempts.Add(1)
		return nil, errors.New("synthetic startup boundary")
	}}
	requestContext, stopRequest := context.WithCancel(testContext)
	completed := make(chan error, 1)
	pm.lock.RLock()
	var release sync.Once
	unlock := func() { release.Do(pm.lock.RUnlock) }
	joined := false
	name := "canceled-registration"
	t.Cleanup(func() {
		stopRequest()
		unlock()
		if !joined {
			select {
			case err := <-completed:
				if status.Code(err) != codes.Canceled {
					t.Errorf("pending creation was not canceled during cleanup: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("creation did not exit after releasing registration")
			}
		}
		if p := pm.findProcess(name); p != nil {
			_, _ = pm.ProcessDelete(context.Background(), &rpc.ProcessDeleteRequest{Name: name, Uuid: p.UUID})
		}
	})
	go func() {
		_, err := pm.ProcessCreate(requestContext, &rpc.ProcessCreateRequest{Spec: &rpc.ProcessSpec{Name: name, Binary: "/synthetic/longhorn", PortCount: 2}})
		completed <- err
	}()
	// A held reader prevents writer acquisition. Writer preference makes a
	// failed TryRLock prove registration is pending, without a scheduling sleep.
	for pm.lock.TryRLock() {
		pm.lock.RUnlock()
		if testContext.Err() != nil {
			t.Fatal("registration never waited for the manager lock")
		}
		runtime.Gosched()
	}
	stopRequest()
	unlock()
	select {
	case err := <-completed:
		joined = true
		if status.Code(err) != codes.Canceled || startupAttempts.Load() != 0 {
			t.Fatalf("cancellation while waiting for registration was lost: err=%v attempts=%d", err, startupAttempts.Load())
		}
	case <-testContext.Done():
		t.Fatal("registration did not complete")
	}
	if pm.findProcess(name) != nil {
		t.Fatal("canceled registration published a process")
	}
	if count, err := countOpenFileDescriptors(filepath.Join(pm.logsDir, name+".log")); err != nil || count != 0 {
		t.Fatalf("canceled registration left a log descriptor: count=%d err=%v", count, err)
	}
}

func (h *createPreparationHook) Levels() []logrus.Level { return []logrus.Level{logrus.InfoLevel} }
func (h *createPreparationHook) Fire(entry *logrus.Entry) error {
	if entry.Message == h.message {
		h.once.Do(func() { close(h.entered) })
		<-h.release
	}
	return nil
}

func TestProcessCreateHonorsCancellationBeforeRegistration(t *testing.T) {
	for _, timing := range []string{"before request", "during preparation", "active control"} {
		t.Run(timing, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pm, err := NewManager(ctx, "10000-10010", t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			var startupAttempts atomic.Int32
			pm.Executor = &MockExecutor{CreationHook: func(*MockCommand) (*MockCommand, error) {
				startupAttempts.Add(1)
				return nil, errors.New("synthetic startup boundary")
			}}
			name := "canceled-create"
			requestContext, stopRequest := context.WithCancel(ctx)
			defer stopRequest()
			var hook *createPreparationHook
			var unblock func()
			if timing == "before request" {
				stopRequest()
			}
			if timing == "during preparation" {
				hook = &createPreparationHook{message: "Process Manager: prepare to create process " + name, entered: make(chan struct{}), release: make(chan struct{})}
				logger := logrus.StandardLogger()
				original := logger.ReplaceHooks(make(logrus.LevelHooks))
				logger.AddHook(hook)
				blocked := hook
				var release sync.Once
				unblock = func() { release.Do(func() { close(blocked.release) }) }
				t.Cleanup(func() { unblock(); logger.ReplaceHooks(original) })
			}
			completed := make(chan error, 1)
			go func() {
				_, err := pm.ProcessCreate(requestContext, &rpc.ProcessCreateRequest{Spec: &rpc.ProcessSpec{Name: name, Binary: "/synthetic/longhorn"}})
				completed <- err
			}()
			if hook != nil {
				select {
				case <-hook.entered:
				case <-ctx.Done():
					t.Fatal("creation never entered native preparation")
				}
				list, err := pm.ProcessList(ctx, &rpc.ProcessListRequest{})
				if err != nil || len(list.Processes) != 0 {
					t.Fatal("fixture was already registered")
				}
				if _, err := pm.ProcessDelete(ctx, &rpc.ProcessDeleteRequest{Name: name}); status.Code(err) != codes.NotFound {
					t.Fatalf("unexpected native pre-registration deletion: %v", err)
				}
				stopRequest()
				unblock()
			}
			select {
			case err := <-completed:
				if timing == "active control" {
					if err != nil || startupAttempts.Load() != 1 {
						t.Fatalf("active startup path was not exercised: %v", err)
					}
				} else if status.Code(err) != codes.Canceled || startupAttempts.Load() != 0 {
					t.Fatalf("canceled request reached startup: err=%v attempts=%d", err, startupAttempts.Load())
				}
			case <-ctx.Done():
				t.Fatal("creation did not complete")
			}
			if timing != "active control" {
				if p := pm.findProcess(name); p != nil {
					t.Fatal("canceled creation left a registered process")
				}
				if count, err := countOpenFileDescriptors(filepath.Join(pm.logsDir, name+".log")); err != nil || count != 0 {
					t.Fatalf("canceled preparation left a log descriptor: count=%d err=%v", count, err)
				}
			}
			if p := pm.findProcess(name); p != nil {
				_, _ = pm.ProcessDelete(ctx, &rpc.ProcessDeleteRequest{Name: name, Uuid: p.UUID})
			}
		})
	}
}
