package proxy

import (
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
)

func (p *Proxy) VolumeGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVolumeGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Trace("Getting volume")

	switch req.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.volumeGet(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkVolumeGet(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.DataEngine)
	}
}

func (p *Proxy) volumeGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVolumeGetProxyResponse, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	recv, err := c.VolumeGet()
	if err != nil {
		return nil, err
	}

	return &rpc.EngineVolumeGetProxyResponse{
		Volume: &eptypes.Volume{
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
		},
	}, nil
}

func (p *Proxy) spdkVolumeGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVolumeGetProxyResponse, err error) {
	c, err := spdkclient.NewSPDKClient(p.spdkServiceAddress)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	recv, err := c.EngineGet(req.EngineName)
	if err != nil {
		return nil, err
	}

	return &rpc.EngineVolumeGetProxyResponse{
		Volume: &eptypes.Volume{
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

	switch req.ProxyEngineRequest.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.volumeExpand(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkVolumeExpand(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.ProxyEngineRequest.DataEngine)
	}
}

func (p *Proxy) volumeExpand(ctx context.Context, req *rpc.EngineVolumeExpandRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.VolumeExpand(req.Expand.Size)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (p *Proxy) spdkVolumeExpand(ctx context.Context, req *rpc.EngineVolumeExpandRequest) (resp *emptypb.Empty, err error) {
	return nil, grpcstatus.Errorf(grpccodes.Unimplemented, "not implemented")
}

func (p *Proxy) VolumeFrontendStart(ctx context.Context, req *rpc.EngineVolumeFrontendStartRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Starting volume frontend %v", req.FrontendStart.Frontend)

	switch req.ProxyEngineRequest.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.volumeFrontendStart(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkVolumeFrontendStart(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.ProxyEngineRequest.DataEngine)
	}
}

func (p *Proxy) volumeFrontendStart(ctx context.Context, req *rpc.EngineVolumeFrontendStartRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.VolumeFrontendStart(req.FrontendStart.Frontend)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (p *Proxy) spdkVolumeFrontendStart(ctx context.Context, req *rpc.EngineVolumeFrontendStartRequest) (resp *emptypb.Empty, err error) {
	/* Not implemented */
	return &emptypb.Empty{}, nil
}

func (p *Proxy) VolumeFrontendShutdown(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.Address,
		"engineName": req.EngineName,
		"volumeName": req.VolumeName,
		"dataEngine": req.DataEngine,
	})
	log.Info("Shutting down volume frontend")

	switch req.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.volumeFrontendShutdown(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkVolumeFrontendShutdown(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.DataEngine)
	}
}

func (p *Proxy) volumeFrontendShutdown(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.Address, req.VolumeName, req.EngineName)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.VolumeFrontendShutdown()
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (p *Proxy) spdkVolumeFrontendShutdown(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *emptypb.Empty, err error) {
	/* Not implemented */
	return &emptypb.Empty{}, nil
}

func (p *Proxy) VolumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (resp *emptypb.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{
		"serviceURL": req.ProxyEngineRequest.Address,
		"engineName": req.ProxyEngineRequest.EngineName,
		"volumeName": req.ProxyEngineRequest.VolumeName,
		"dataEngine": req.ProxyEngineRequest.DataEngine,
	})
	log.Infof("Setting volume flag UnmapMarkSnapChainRemoved to %v", req.UnmapMarkSnap.Enabled)

	switch req.ProxyEngineRequest.DataEngine {
	case rpc.DataEngine_DATA_ENGINE_V1:
		return p.volumeUnmapMarkSnapChainRemovedSet(ctx, req)
	case rpc.DataEngine_DATA_ENGINE_V2:
		return p.spdkVolumeUnmapMarkSnapChainRemovedSet(ctx, req)
	default:
		return nil, grpcstatus.Errorf(grpccodes.InvalidArgument, "unknown data engine %v", req.ProxyEngineRequest.DataEngine)
	}

}

func (p *Proxy) volumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (resp *emptypb.Empty, err error) {
	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address, req.ProxyEngineRequest.VolumeName,
		req.ProxyEngineRequest.EngineName)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.VolumeUnmapMarkSnapChainRemovedSet(req.UnmapMarkSnap.Enabled)
	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func (p *Proxy) spdkVolumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (resp *emptypb.Empty, err error) {
	/* Not implemented */
	return &emptypb.Empty{}, nil
}
