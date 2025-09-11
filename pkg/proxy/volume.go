package proxy

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/enginerpc"

	lhns "github.com/longhorn/go-common-libs/ns"
	lhtypes "github.com/longhorn/go-common-libs/types"
	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

func (p *Proxy) VolumeGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVolumeGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Getting volume")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.VolumeGet(ctx, req)
}

func (ops V1DataEngineProxyOps) VolumeGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVolumeGetProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.Address,
				"engineName": req.EngineName,
				"volumeName": req.VolumeName,
				"dataEngine": req.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	recv, err := c.VolumeGet()
	if err != nil {
		return nil, err
	}

	return &rpc.EngineVolumeGetProxyResponse{
		Volume: &enginerpc.Volume{
			Name:                      recv.Name,
			Size:                      recv.Size,
			ReplicaCount:              int32(recv.ReplicaCount),
			Endpoint:                  recv.Endpoint,
			Frontend:                  recv.Frontend,
			FrontendState:             recv.FrontendState,
			IsExpanding:               recv.IsExpanding,
			LastExpansionError:        recv.LastExpansionError,
			LastExpansionFailedAt:     recv.LastExpansionFailedAt,
			UnmapMarkSnapChainRemoved: recv.UnmapMarkSnapChainRemoved,
			SnapshotMaxCount:          int32(recv.SnapshotMaxCount),
			SnapshotMaxSize:           recv.SnapshotMaxSize,
		},
	}, nil
}

func (ops V2DataEngineProxyOps) VolumeGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVolumeGetProxyResponse, err error) {
	c, err := getSPDKClientFromAddress(req.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.Address,
				"engineName": req.EngineName,
				"volumeName": req.VolumeName,
				"dataEngine": req.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	recv, err := c.EngineGet(req.EngineName)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get engine %v: %v", req.EngineName, err)
	}

	return &rpc.EngineVolumeGetProxyResponse{
		Volume: &enginerpc.Volume{
			Name:                      recv.Name,
			Size:                      int64(recv.SpecSize),
			ReplicaCount:              int32(len(recv.ReplicaAddressMap)),
			Endpoint:                  recv.Endpoint,
			Frontend:                  recv.Frontend,
			FrontendState:             "",
			IsExpanding:               false,
			LastExpansionError:        "",
			LastExpansionFailedAt:     "",
			UnmapMarkSnapChainRemoved: false,
		},
	}, nil
}

func (p *Proxy) VolumeExpand(ctx context.Context, req *rpc.EngineVolumeExpandRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Expanding volume to size %v", req.Expand.Size)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.VolumeExpand(ctx, req)
}

func (ops V1DataEngineProxyOps) VolumeExpand(ctx context.Context, req *rpc.EngineVolumeExpandRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	err = c.VolumeExpand(req.Expand.Size)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) VolumeExpand(ctx context.Context, req *rpc.EngineVolumeExpandRequest) (resp *emptypb.Empty, err error) {
	c, err := getSPDKClientFromAddress(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, "failed to get SPDK client from engine address %v: %v", req.ProxyEngineRequest.Address, err)
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close SPDK client")
		}
	}()

	err = c.EngineExpand(ctx, req.ProxyEngineRequest.EngineName, uint64(req.Expand.Size))
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (p *Proxy) VolumeFrontendStart(ctx context.Context, req *rpc.EngineVolumeFrontendStartRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Starting volume frontend %v", req.FrontendStart.Frontend)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.VolumeFrontendStart(ctx, req)
}

func (ops V1DataEngineProxyOps) VolumeFrontendStart(ctx context.Context, req *rpc.EngineVolumeFrontendStartRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	err = c.VolumeFrontendStart(req.FrontendStart.Frontend)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) VolumeFrontendStart(ctx context.Context, req *rpc.EngineVolumeFrontendStartRequest) (resp *emptypb.Empty, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "VolumeFrontendStart is not yet implemented for V2 engine")
}

func (p *Proxy) VolumeFrontendShutdown(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Info("Shutting down volume frontend")

	ops, ok := p.ops[req.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.DataEngine)
	}
	return ops.VolumeFrontendShutdown(ctx, req)
}

func (ops V1DataEngineProxyOps) VolumeFrontendShutdown(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.Address,
				"engineName": req.EngineName,
				"volumeName": req.VolumeName,
				"dataEngine": req.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	err = c.VolumeFrontendShutdown()
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) VolumeFrontendShutdown(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *emptypb.Empty, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "VolumeFrontendShutdown is not yet implemented for V2 engine")
}

func (p *Proxy) VolumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Setting volume flag UnmapMarkSnapChainRemoved to %v", req.UnmapMarkSnap.Enabled)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.VolumeUnmapMarkSnapChainRemovedSet(ctx, req)
}

func (ops V1DataEngineProxyOps) VolumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	err = c.VolumeUnmapMarkSnapChainRemovedSet(req.UnmapMarkSnap.Enabled)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) VolumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (resp *emptypb.Empty, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "VolumeUnmapMarkSnapChainRemovedSet is not yet implemented for V2 engine")
}

func (p *Proxy) VolumeSnapshotMaxCountSet(ctx context.Context, req *rpc.EngineVolumeSnapshotMaxCountSetRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Setting volume flag SnapshotMaxCount to %v", req.Count.Count)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.VolumeSnapshotMaxCountSet(ctx, req)
}

func (ops V1DataEngineProxyOps) VolumeSnapshotMaxCountSet(ctx context.Context, req *rpc.EngineVolumeSnapshotMaxCountSetRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	err = c.VolumeSnapshotMaxCountSet(int(req.Count.Count))
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) VolumeSnapshotMaxCountSet(ctx context.Context, req *rpc.EngineVolumeSnapshotMaxCountSetRequest) (resp *emptypb.Empty, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "VolumeSnapshotMaxCountSet is not yet implemented for V2 engine")
}

func (p *Proxy) VolumeSnapshotMaxSizeSet(ctx context.Context, req *rpc.EngineVolumeSnapshotMaxSizeSetRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Setting volume flag SnapshotMaxSize to %v", req.Size.Size)

	ops, ok := p.ops[req.ProxyEngineRequest.DataEngine]
	if !ok {
		return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "unsupported data engine %v", req.ProxyEngineRequest.DataEngine)
	}
	return ops.VolumeSnapshotMaxSizeSet(ctx, req)
}

func (ops V1DataEngineProxyOps) VolumeSnapshotMaxSizeSet(ctx context.Context, req *rpc.EngineVolumeSnapshotMaxSizeSetRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := c.Close(); closeErr != nil {
			logrus.WithFields(logrus.Fields{
				"serviceURL": req.ProxyEngineRequest.Address,
				"engineName": req.ProxyEngineRequest.EngineName,
				"volumeName": req.ProxyEngineRequest.VolumeName,
				"dataEngine": req.ProxyEngineRequest.DataEngine,
			}).WithError(closeErr).Warn("Failed to close Controller client")
		}
	}()

	err = c.VolumeSnapshotMaxSizeSet(req.Size.Size)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (ops V2DataEngineProxyOps) VolumeSnapshotMaxSizeSet(ctx context.Context, req *rpc.EngineVolumeSnapshotMaxSizeSetRequest) (resp *emptypb.Empty, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "VolumeSnapshotMaxSizeSet is not yet implemented for V2 engine")
}

func (p *Proxy) RemountReadOnlyVolume(ctx context.Context, req *rpc.RemountVolumeRequest) (resp *emptypb.Empty, err error) {
	volumeName := req.VolumeName
	volumeNameSHA := sha256.Sum256([]byte(volumeName))
	volumeNameSHAStr := hex.EncodeToString(volumeNameSHA[:])

	volumeMountPointMap, err := util.GetVolumeMountPointMap()
	if err != nil {
		return &emptypb.Empty{}, err
	}

	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceNet}
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.ProcDirectory, namespaces)
	if err != nil {
		return &emptypb.Empty{}, err
	}

	if mp, exists := volumeMountPointMap[volumeNameSHAStr]; exists {
		opts := []string{
			"-o",
			"remount,rw",
			mp.Path,
		}
		if _, err := nsexec.Execute(nil, "mount", opts, lhtypes.ExecuteDefaultTimeout); err != nil {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "remount failed with error: %v", err)
		}
	}

	return &emptypb.Empty{}, nil
}
