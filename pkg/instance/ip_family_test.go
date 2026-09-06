package instance

import (
	"context"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/process"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

func startPortArgsProcessManager(t *testing.T) (*process.Manager, string, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	manager, err := process.NewManager(ctx, "30000-30020", t.TempDir())
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	manager.Executor = &process.MockExecutor{}
	manager.HealthChecker = &process.MockHealthChecker{}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	server := grpc.NewServer()
	rpc.RegisterProcessManagerServiceServer(server, manager)
	go func() { _ = server.Serve(listener) }()
	cleanup := func() {
		defer cancel()
		defer server.Stop()
		if _, err := manager.ProcessGet(ctx, &rpc.ProcessGetRequest{Name: "engine"}); status.Code(err) == codes.NotFound {
			return
		}
		if _, err := manager.ProcessDelete(ctx, &rpc.ProcessDeleteRequest{Name: "engine"}); err != nil {
			t.Error(err)
			return
		}
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := manager.ProcessGet(ctx, &rpc.ProcessGetRequest{Name: "engine"}); status.Code(err) == codes.NotFound {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Error("process did not finish cleanup")
	}
	return manager, listener.Addr().String(), cleanup
}

func TestV1InstanceCreatePreservesPortArgs(t *testing.T) {
	testV1PreservesPortArgs(t, false)
}

func TestV1InstanceReplacePreservesPortArgs(t *testing.T) {
	testV1PreservesPortArgs(t, true)
}

func testV1PreservesPortArgs(t *testing.T, replace bool) {
	t.Helper()
	tests := []struct {
		name      string
		portCount int32
		portArgs  []string
		wantArgs  func(int32) []string
	}{
		{
			name:      "caller IPv6 wildcard template",
			portCount: 1, portArgs: []string{"--listen,[::]:"},
			wantArgs: func(port int32) []string { return []string{"base", "--listen", fmt.Sprintf("[::]:%d", port)} },
		},
		{
			name:      "caller IPv4 wildcard template",
			portCount: 1, portArgs: []string{"--listen,:"},
			wantArgs: func(port int32) []string { return []string{"base", "--listen", fmt.Sprintf(":%d", port)} },
		},
		{
			name:      "caller host templates preserve multiple port positions",
			portCount: 2, portArgs: []string{"--metrics,:", "--listen,0.0.0.0:"},
			wantArgs: func(port int32) []string {
				return []string{"base", "--metrics", fmt.Sprintf(":%d", port), "--listen", fmt.Sprintf("0.0.0.0:%d", port+1)}
			},
		},
		{
			name:     "zero ports",
			wantArgs: func(int32) []string { return []string{"base"} },
		},
		{
			name:     "reserved ports without templates",
			wantArgs: func(int32) []string { return []string{"base"} },
		},
		{
			name:      "explicit host and nonstandard argument",
			portCount: 2, portArgs: []string{"--listen,192.0.2.10:", "--http-port,"},
			wantArgs: func(port int32) []string {
				return []string{"base", "--listen", fmt.Sprintf("192.0.2.10:%d", port), "--http-port", fmt.Sprint(port + 1)}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager, address, cleanup := startPortArgsProcessManager(t)
			defer cleanup()
			if replace {
				if _, err := manager.ProcessCreate(t.Context(), &rpc.ProcessCreateRequest{Spec: &rpc.ProcessSpec{
					Name: "engine", Binary: "old-engine",
				}}); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(5 * time.Second)
				for {
					old, err := manager.ProcessGet(t.Context(), &rpc.ProcessGetRequest{Name: "engine"})
					if err != nil {
						t.Fatal(err)
					}
					if old.Status.State == types.ProcessStateRunning {
						break
					}
					if time.Now().After(deadline) {
						t.Fatalf("initial process did not become running: %s", old.Status.State)
					}
					time.Sleep(10 * time.Millisecond)
				}
			}
			ops := V1DataEngineInstanceOps{processManagerServiceAddress: address}
			spec := &rpc.InstanceSpec{
				Name: "engine", Type: types.InstanceTypeEngine, DataEngine: rpc.DataEngine_DATA_ENGINE_V1,
				PortCount: test.portCount, PortArgs: test.portArgs,
				ProcessInstanceSpec: &rpc.ProcessInstanceSpec{Binary: "new-engine", Args: []string{"base"}},
			}
			original := slices.Clone(spec.PortArgs)
			var response *rpc.InstanceResponse
			var err error
			if replace {
				response, err = ops.InstanceReplace(&rpc.InstanceReplaceRequest{Spec: spec, TerminateSignal: "SIGHUP"})
			} else {
				response, err = ops.InstanceCreate(&rpc.InstanceCreateRequest{Spec: spec})
			}
			if err != nil {
				t.Fatal(err)
			}
			if got, want := response.Spec.ProcessInstanceSpec.Args, test.wantArgs(response.Status.PortStart); !slices.Equal(got, want) {
				t.Fatalf("process argv = %q, want %q", got, want)
			}
			if !slices.Equal(spec.PortArgs, original) {
				t.Fatalf("caller port templates mutated: got %q, want %q", spec.PortArgs, original)
			}
		})
	}
}
