package instance

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

type cancellationProcessManager struct {
	rpc.UnimplementedProcessManagerServiceServer
	entered, canceled chan struct{}
}

var (
	_ func(*rpc.InstanceCreateRequest) (*rpc.InstanceResponse, error) = V1DataEngineInstanceOps{}.InstanceCreate
	_ func(*rpc.InstanceCreateRequest) (*rpc.InstanceResponse, error) = V2DataEngineInstanceOps{}.InstanceCreate
)

type legacyOnlyCreate struct {
	InstanceOps
	called bool
}

func (s *legacyOnlyCreate) InstanceCreate(*rpc.InstanceCreateRequest) (*rpc.InstanceResponse, error) {
	s.called = true
	return &rpc.InstanceResponse{}, nil
}

func TestLegacyInstanceCreatorStillWorks(t *testing.T) {
	ops := &legacyOnlyCreate{}
	server := &Server{ops: map[rpc.DataEngine]InstanceOps{rpc.DataEngine_DATA_ENGINE_V2: ops}}
	response, err := server.InstanceCreate(context.Background(), &rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{DataEngine: rpc.DataEngine_DATA_ENGINE_V2}})
	if err != nil || response == nil || !ops.called {
		t.Fatalf("legacy creation interface was not preserved: %v", err)
	}
}

func (s *cancellationProcessManager) ProcessCreate(ctx context.Context, _ *rpc.ProcessCreateRequest) (*rpc.ProcessResponse, error) {
	close(s.entered)
	<-ctx.Done()
	close(s.canceled)
	return nil, status.FromContextError(ctx.Err()).Err()
}

func TestV1CreateForwardsCallerCancellationToProcessManager(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	backend := &cancellationProcessManager{entered: make(chan struct{}), canceled: make(chan struct{})}
	rpc.RegisterProcessManagerServiceServer(server, backend)
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		if err := <-serveErrors; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("serve: %v", err)
		}
	})
	instance := &Server{ops: map[rpc.DataEngine]InstanceOps{
		rpc.DataEngine_DATA_ENGINE_V1: V1DataEngineInstanceOps{processManagerServiceAddress: listener.Addr().String()},
	}}
	requestContext, stopRequest := context.WithCancel(ctx)
	defer stopRequest()
	completed := make(chan error, 1)
	go func() {
		_, err := instance.InstanceCreate(requestContext, &rpc.InstanceCreateRequest{Spec: &rpc.InstanceSpec{
			Name: "cancellation-control", Type: types.InstanceTypeReplica, DataEngine: rpc.DataEngine_DATA_ENGINE_V1,
			ProcessInstanceSpec: &rpc.ProcessInstanceSpec{Binary: "/synthetic/longhorn"},
		}})
		completed <- err
	}()
	select {
	case <-backend.entered:
	case <-ctx.Done():
		t.Fatal("native forwarding did not reach the process manager")
	}
	stopRequest()
	select {
	case err := <-completed:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("caller cancellation was lost: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("forwarded create outlived caller cancellation")
	}
	select {
	case <-backend.canceled:
	case <-ctx.Done():
		t.Fatal("downstream request was not cancelled")
	}
}
