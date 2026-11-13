package client

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

type cancellationCreateClient struct {
	rpc.ProcessManagerServiceClient
	observed context.Context
}

func (c *cancellationCreateClient) ProcessCreate(ctx context.Context, _ *rpc.ProcessCreateRequest, _ ...grpc.CallOption) (*rpc.ProcessResponse, error) {
	c.observed = ctx
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &rpc.ProcessResponse{}, nil
}

func TestProcessCreatePreservesClientContext(t *testing.T) {
	for _, state := range []string{"canceled", "expired", "active"} {
		t.Run(state, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			want := codes.OK
			if state == "canceled" {
				cancel()
				want = codes.Canceled
			}
			if state == "expired" {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				want = codes.DeadlineExceeded
			}
			defer cancel()
			service := &cancellationCreateClient{}
			client := &ProcessManagerClient{ProcessManagerServiceContext: ProcessManagerServiceContext{ctx: ctx, service: service}}
			_, err := client.ProcessCreate("context-probe", "/synthetic/longhorn", 0, nil, nil)
			if status.Code(err) != want {
				t.Fatalf("client context was discarded: got=%v want=%v", err, want)
			}
			if service.observed == nil {
				t.Fatal("native request path was not exercised")
			}
			if _, bounded := service.observed.Deadline(); !bounded {
				t.Fatal("service timeout was lost")
			}
		})
	}
}
