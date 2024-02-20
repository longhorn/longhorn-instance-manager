package instance

import (
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
)

func (s *Server) LogSetLevel(ctx context.Context, req *rpc.LogSetLevelRequest) (resp *emptypb.Empty, err error) {
	ops, ok := s.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.LogSetLevel(ctx, req)
}

func (ops V1DataEngineInstanceOps) LogSetLevel(ctx context.Context, req *rpc.LogSetLevelRequest) (resp *emptypb.Empty, err error) {
	/* TODO: Implement this */
	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineInstanceOps) LogSetLevel(ctx context.Context, req *rpc.LogSetLevelRequest) (resp *emptypb.Empty, err error) {
	c, err := spdkclient.NewSPDKClient(ops.spdkServiceAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}
	defer c.Close()

	err = c.LogSetLevel(req.Level)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to set log level").Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) LogSetFlags(ctx context.Context, req *rpc.LogSetFlagsRequest) (resp *emptypb.Empty, err error) {
	ops, ok := s.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.LogSetFlags(ctx, req)
}

func (ops V1DataEngineInstanceOps) LogSetFlags(ctx context.Context, req *rpc.LogSetFlagsRequest) (resp *emptypb.Empty, err error) {
	/* TODO: Implement this */
	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineInstanceOps) LogSetFlags(ctx context.Context, req *rpc.LogSetFlagsRequest) (resp *emptypb.Empty, err error) {
	c, err := spdkclient.NewSPDKClient(ops.spdkServiceAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}
	defer c.Close()

	err = c.LogSetFlags(req.Flags)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to set log flags").Error())
	}
	return &emptypb.Empty{}, nil
}

func (s *Server) LogGetLevel(ctx context.Context, req *rpc.LogGetLevelRequest) (resp *rpc.LogGetLevelResponse, err error) {
	ops, ok := s.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.LogGetLevel(ctx, req)
}

func (ops V1DataEngineInstanceOps) LogGetLevel(ctx context.Context, req *rpc.LogGetLevelRequest) (resp *rpc.LogGetLevelResponse, err error) {
	/* TODO: Implement this */
	return &rpc.LogGetLevelResponse{}, nil
}

func (ops V2DataEngineInstanceOps) LogGetLevel(ctx context.Context, req *rpc.LogGetLevelRequest) (resp *rpc.LogGetLevelResponse, err error) {
	c, err := spdkclient.NewSPDKClient(ops.spdkServiceAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}
	defer c.Close()

	level, err := c.LogGetLevel()
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to get log level").Error())
	}
	return &rpc.LogGetLevelResponse{
		Level: level,
	}, nil
}

func (s *Server) LogGetFlags(ctx context.Context, req *rpc.LogGetFlagsRequest) (resp *rpc.LogGetFlagsResponse, err error) {
	ops, ok := s.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.LogGetFlags(ctx, req)
}

func (ops V1DataEngineInstanceOps) LogGetFlags(ctx context.Context, req *rpc.LogGetFlagsRequest) (resp *rpc.LogGetFlagsResponse, err error) {
	/* TODO: Implement this */
	return &rpc.LogGetFlagsResponse{}, nil
}

func (ops V2DataEngineInstanceOps) LogGetFlags(ctx context.Context, req *rpc.LogGetFlagsRequest) (resp *rpc.LogGetFlagsResponse, err error) {
	c, err := spdkclient.NewSPDKClient(ops.spdkServiceAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}
	defer c.Close()

	flags, err := c.LogGetFlags()
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to get log flags").Error())
	}
	return &rpc.LogGetFlagsResponse{
		Flags: flags,
	}, nil
}
