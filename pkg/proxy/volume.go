package proxy

import (
	"crypto/sha256"
	"encoding/hex"
	"os/exec"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"

	eclient "github.com/longhorn/longhorn-engine/pkg/controller/client"
	eptypes "github.com/longhorn/longhorn-engine/proto/ptypes"
)

func (p *Proxy) VolumeGet(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *rpc.EngineVolumeGetProxyResponse, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Trace("Getting volume")

	c, err := eclient.NewControllerClient(req.Address)
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

func (p *Proxy) VolumeExpand(ctx context.Context, req *rpc.EngineVolumeExpandRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Infof("Expanding volume to size %v", req.Expand.Size)

	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.VolumeExpand(req.Expand.Size)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (p *Proxy) VolumeFrontendStart(ctx context.Context, req *rpc.EngineVolumeFrontendStartRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Infof("Starting volume frontend %v", req.FrontendStart.Frontend)

	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.VolumeFrontendStart(req.FrontendStart.Frontend)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (p *Proxy) VolumeFrontendShutdown(ctx context.Context, req *rpc.ProxyEngineRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.Address})
	log.Info("Shutting down volume frontend")

	c, err := eclient.NewControllerClient(req.Address)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.VolumeFrontendShutdown()
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (p *Proxy) VolumeUnmapMarkSnapChainRemovedSet(ctx context.Context, req *rpc.EngineVolumeUnmapMarkSnapChainRemovedSetRequest) (resp *empty.Empty, err error) {
	log := logrus.WithFields(logrus.Fields{"serviceURL": req.ProxyEngineRequest.Address})
	log.Infof("Setting volume flag UnmapMarkSnapChainRemoved to %v", req.UnmapMarkSnap.Enabled)

	c, err := eclient.NewControllerClient(req.ProxyEngineRequest.Address)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	err = c.VolumeUnmapMarkSnapChainRemovedSet(req.UnmapMarkSnap.Enabled)
	if err != nil {
		return nil, err
	}

	return &empty.Empty{}, nil
}

func (p *Proxy) RemountReadOnlyVolume(ctx context.Context, req *rpc.RemountVolumeRequest) (resp *emptypb.Empty, err error) {
	volumeName := req.VolumeName
	volumeNameSHA := sha256.Sum256([]byte(volumeName))
	volumeNameSHAStr := hex.EncodeToString(volumeNameSHA[:])

	volumeMountPointMap, err := util.GetVolumeMountPointMap()
	if err != nil {
		logrus.WithError(err).Warn("Failed to get all volume mount points")
	}

	if mp, exists := volumeMountPointMap[volumeNameSHAStr]; exists {
		cmd := exec.CommandContext(ctx, "mount", "-o", "remount,rw", mp.Path)
		if out, err := cmd.CombinedOutput(); err != nil {
			return nil, grpcstatus.Errorf(grpccodes.Internal, "remount failed with output: %v", out)
		}

	}

	return &emptypb.Empty{}, nil
}
