package instance

import (
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

const (
	NonSPDKLogLevelTrace = "TRACE"
	SPDKLogLevelDebug    = "DEBUG"
)

func (s *Server) LogSetLevel(ctx context.Context, req *rpc.LogSetLevelRequest) (resp *emptypb.Empty, err error) {
	ops, ok := s.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.LogSetLevel(ctx, req)
}

func logSetLevel(level string) error {
	// Set instance-manager log level.  We expect a string such as "debug", "info", or "warn".
	newLevel, err := logrus.ParseLevel(level)
	if err != nil {
		return err
	}

	oldLevel := logrus.GetLevel()
	if oldLevel != newLevel {
		logrus.Warnf("Updating log level from %v to %v", oldLevel, newLevel)
		logrus.SetLevel(newLevel)
	}

	return nil
}

// This method is used to set instance-manager internal log level regardless of engine type.
func (ops V1DataEngineInstanceOps) LogSetLevel(ctx context.Context, req *rpc.LogSetLevelRequest) (resp *emptypb.Empty, err error) {
	if err := logSetLevel(req.Level); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// This method is used to set the log level for spdk_tgt, for v2 engine type.
func (ops V2DataEngineInstanceOps) LogSetLevel(ctx context.Context, req *rpc.LogSetLevelRequest) (resp *emptypb.Empty, err error) {
	spdkLevel := strings.ToUpper(req.Level)

	c, err := spdkclient.NewSPDKClient(ops.spdkServiceAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	err = c.LogSetLevel(spdkLevel)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to set v2 data engine log level").Error())
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
	// There is no V1 implementation.  Log flags are not a thing as they are for SPDK.
	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineInstanceOps) LogSetFlags(ctx context.Context, req *rpc.LogSetFlagsRequest) (resp *emptypb.Empty, err error) {
	c, err := spdkclient.NewSPDKClient(ops.spdkServiceAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

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
	return &rpc.LogGetLevelResponse{
		Level: logrus.GetLevel().String(),
	}, nil
}

func (ops V2DataEngineInstanceOps) LogGetLevel(ctx context.Context, req *rpc.LogGetLevelRequest) (resp *rpc.LogGetLevelResponse, err error) {
	return &rpc.LogGetLevelResponse{
		Level: logrus.GetLevel().String(),
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
	// No implementation necessary.
	return &rpc.LogGetFlagsResponse{}, nil
}

func (ops V2DataEngineInstanceOps) LogGetFlags(ctx context.Context, req *rpc.LogGetFlagsRequest) (resp *rpc.LogGetFlagsResponse, err error) {
	c, err := spdkclient.NewSPDKClient(ops.spdkServiceAddress)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	flags, err := c.LogGetFlags()
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to get log flags").Error())
	}
	return &rpc.LogGetFlagsResponse{
		Flags: flags,
	}, nil
}
