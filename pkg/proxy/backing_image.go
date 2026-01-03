package proxy

import (
	"context"
	"fmt"
	"time"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/emptypb"

	spdkapi "github.com/longhorn/longhorn-spdk-engine/pkg/api"
	spdkclient "github.com/longhorn/longhorn-spdk-engine/pkg/client"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

const (
	maxMonitorRetryCount         = 10
	monitorRetryPollInterval     = 1 * time.Second
	spdkTgtReadinessProbeTimeout = 60 * time.Second
)

func (p *Proxy) SPDKBackingImageCreate(ctx context.Context, req *rpc.SPDKBackingImageCreateRequest) (*rpc.SPDKBackingImageResponse, error) {
	log := logrus.WithFields(logrus.Fields{
		"name":             req.Name,
		"backingImageUUID": req.BackingImageUuid,
		"diskUuid":         req.DiskUuid,
		"size":             req.Size,
		"Checksum":         req.Checksum,
		"FromAddress":      req.FromAddress,
		"SrcLvsUuid":       req.SrcLvsUuid,
	})

	log.Info("Backing Image Server: Creating SPDk Backing Image")
	ret, err := p.spdkLocalClient.BackingImageCreate(req.Name, req.BackingImageUuid, req.DiskUuid, req.Size, req.Checksum, req.FromAddress, req.SrcLvsUuid)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	return spdkBackingImageToBackingImageResponse(ret), nil
}

func (p *Proxy) SPDKBackingImageDelete(ctx context.Context, req *rpc.SPDKBackingImageDeleteRequest) (*emptypb.Empty, error) {
	log := logrus.WithFields(logrus.Fields{
		"name":     req.Name,
		"diskUuid": req.DiskUuid,
	})
	log.Info("Backing Image Server: Deleting SPDk Backing Image")
	return &emptypb.Empty{}, p.spdkLocalClient.BackingImageDelete(req.Name, req.DiskUuid)
}

func (p *Proxy) SPDKBackingImageGet(ctx context.Context, req *rpc.SPDKBackingImageGetRequest) (*rpc.SPDKBackingImageResponse, error) {
	log := logrus.WithFields(logrus.Fields{
		"name":     req.Name,
		"diskUuid": req.DiskUuid,
	})
	log.Debug("Backing Image Server: Get SPDk Backing Image")
	ret, err := p.spdkLocalClient.BackingImageGet(req.Name, req.DiskUuid)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	return spdkBackingImageToBackingImageResponse(ret), nil
}

func (p *Proxy) SPDKBackingImageList(ctx context.Context, req *emptypb.Empty) (*rpc.SPDKBackingImageListResponse, error) {
	logrus.Debug("Backing Image Server: List SPDk Backing Image")

	backingImages := map[string]*rpc.SPDKBackingImageResponse{}

	ret, err := p.spdkLocalClient.BackingImageList()
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	for name, bi := range ret {
		// name is in the form of "bi-%s-disk-%s" because one instance-manager manages multiple disks
		backingImages[name] = spdkBackingImageToBackingImageResponse(bi)
	}
	return &rpc.SPDKBackingImageListResponse{BackingImages: backingImages}, nil
}

func (p *Proxy) SPDKBackingImageWatch(req *emptypb.Empty, srv rpc.ProxyEngineService_SPDKBackingImageWatchServer) error {
	logrus.Info("Start watching SPDK backing image")

	done := make(chan struct{})

	// Create a client for watching SPDK backing image
	spdkClient, err := spdkclient.NewSPDKClient(p.spdkServiceAddress)
	go func() {
		<-done
		logrus.Info("Stopped clients for watching SPDK backing image")
		if spdkClient != nil {
			if closeErr := spdkClient.Close(); closeErr != nil {
				logrus.WithError(closeErr).Warn("Failed to close SPDK client")
			}
		}
		close(done)
	}()
	if err != nil {
		done <- struct{}{}
		return grpcstatus.Error(grpccodes.Internal, errors.Wrapf(err, "failed to create SPDK client").Error())
	}

	notifyChan := make(chan struct{}, 1024)
	defer close(notifyChan)

	g, ctx := errgroup.WithContext(p.ctx)

	g.Go(func() error {
		defer func() {
			// Close the clients for closing streams and unblocking notifier Recv() with error.
			done <- struct{}{}
		}()
		err := p.handleNotify(ctx, notifyChan, srv)
		if err != nil {
			logrus.WithError(err).Error("Failed to handle notify")
		}
		return err
	})

	g.Go(func() error {
		return p.watchSPDKBackingImage(ctx, req, spdkClient, notifyChan)
	})

	if err := g.Wait(); err != nil {
		logrus.WithError(err).Error("Failed to watch backing images")
		return errors.Wrap(err, "failed to watch backing images")
	}

	return nil
}

func (p *Proxy) handleNotify(ctx context.Context, notifyChan chan struct{}, srv rpc.ProxyEngineService_SPDKBackingImageWatchServer) error {
	logrus.Info("Start handling notify")

	for {
		select {
		case <-ctx.Done():
			logrus.Info("Stopped handling notify due to the context done")
			return ctx.Err()
		case <-notifyChan:
			if err := srv.Send(&emptypb.Empty{}); err != nil {
				return errors.Wrap(err, "failed to send backing image response")
			}
		}
	}
}

func (p *Proxy) watchSPDKBackingImage(ctx context.Context, req *emptypb.Empty, client *spdkclient.SPDKClient, notifyChan chan struct{}) error {
	logrus.Info("Start watching SPDK replicas")

	notifier, err := client.BackingImageWatch(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to create SPDK replica watch notifier")
	}

	failureCount := 0
	for {
		if failureCount >= maxMonitorRetryCount {
			logrus.Errorf("Continuously receiving errors for %v times, stopping watching SPDK backing images", maxMonitorRetryCount)
			return fmt.Errorf("continuously receiving errors for %v times, stopping watching SPDK backing images", maxMonitorRetryCount)
		}

		select {
		case <-ctx.Done():
			logrus.Info("Stopped watching SPDK backing images")
			return ctx.Err()
		default:
			_, err := notifier.Recv()
			if err != nil {
				status, ok := grpcstatus.FromError(err)
				if ok && status.Code() == grpccodes.Canceled {
					logrus.WithError(err).Warn("SPDK backing image watch is canceled")
					return err
				}
				logrus.WithError(err).Error("Failed to receive next item in SPDK backing image watch")
				time.Sleep(monitorRetryPollInterval)
				failureCount++
			} else {
				notifyChan <- struct{}{}
			}
		}
	}
}

func spdkBackingImageToBackingImageResponse(bi *spdkapi.BackingImage) *rpc.SPDKBackingImageResponse {
	return &rpc.SPDKBackingImageResponse{
		Spec: &rpc.SPDKBackingImageSpec{
			Name:             bi.Name,
			BackingImageUuid: bi.BackingImageUUID,
			DiskUuid:         bi.LvsUUID,
			Size:             bi.Size,
			Checksum:         bi.ExpectedChecksum,
		},
		Status: &rpc.SPDKBackingImageStatus{
			Progress: bi.Progress,
			State:    bi.State,
			Checksum: bi.CurrentChecksum,
			ErrorMsg: bi.ErrorMsg,
		},
	}
}
