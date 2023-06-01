package client

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

const (
	GRPCServiceTimeout = 3 * time.Minute
)

type SPDKServiceContext struct {
	cc      *grpc.ClientConn
	service spdkrpc.SPDKServiceClient
}

func (c SPDKServiceContext) Close() error {
	if c.cc == nil {
		return nil
	}
	return c.cc.Close()
}

func (c *SPDKClient) getSPDKServiceClient() spdkrpc.SPDKServiceClient {
	return c.service
}

type SPDKClient struct {
	serviceURL string
	SPDKServiceContext
}

func NewSPDKClient(serviceUrl string) (*SPDKClient, error) {
	getSPDKServiceContext := func(serviceUrl string) (SPDKServiceContext, error) {
		connection, err := grpc.Dial(serviceUrl, grpc.WithInsecure())
		if err != nil {
			return SPDKServiceContext{}, errors.Wrapf(err, "cannot connect to SPDKService %v", serviceUrl)
		}

		return SPDKServiceContext{
			cc:      connection,
			service: spdkrpc.NewSPDKServiceClient(connection),
		}, nil
	}

	serviceContext, err := getSPDKServiceContext(serviceUrl)
	if err != nil {
		return nil, err
	}

	return &SPDKClient{
		serviceURL:         serviceUrl,
		SPDKServiceContext: serviceContext,
	}, nil
}

func (c *SPDKClient) ReplicaCreate(name, lvsName, lvsUUID string, specSize uint64, exposeRequired bool) (*api.Replica, error) {
	if name == "" || lvsName == "" || lvsUUID == "" {
		return nil, fmt.Errorf("failed to start SPDK replica: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaCreate(ctx, &spdkrpc.ReplicaCreateRequest{
		Name:           name,
		LvsName:        lvsName,
		LvsUuid:        lvsUUID,
		SpecSize:       specSize,
		ExposeRequired: exposeRequired,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start SPDK replica")
	}

	return api.ProtoReplicaToReplica(resp), nil
}

func (c *SPDKClient) ReplicaDelete(name string, cleanupRequired bool) error {
	if name == "" {
		return fmt.Errorf("failed to delete SPDK replica: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.ReplicaDelete(ctx, &spdkrpc.ReplicaDeleteRequest{
		Name:            name,
		CleanupRequired: cleanupRequired,
	})
	return errors.Wrapf(err, "failed to delete SPDK replica %v", name)
}

func (c *SPDKClient) ReplicaGet(name string) (*api.Replica, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get SPDK replica: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaGet(ctx, &spdkrpc.ReplicaGetRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK replica %v", name)
	}
	return api.ProtoReplicaToReplica(resp), nil
}

func (c *SPDKClient) ReplicaList() (map[string]*api.Replica, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.ReplicaList(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SPDK replicas")
	}

	res := map[string]*api.Replica{}
	for replicaName, r := range resp.Replicas {
		res[replicaName] = api.ProtoReplicaToReplica(r)
	}
	return res, nil
}

func (c *SPDKClient) ReplicaWatch(ctx context.Context) (*api.ReplicaStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.ReplicaWatch(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open replica watch stream")
	}

	return api.NewReplicaStream(stream), nil
}

func (c *SPDKClient) EngineCreate(name, volumeName, frontend string, specSize uint64, replicaAddressMap map[string]string) (*api.Engine, error) {
	if name == "" || volumeName == "" || frontend == "" || len(replicaAddressMap) == 0 {
		return nil, fmt.Errorf("failed to start SPDK engine: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineCreate(ctx, &spdkrpc.EngineCreateRequest{
		Name:              name,
		VolumeName:        volumeName,
		SpecSize:          specSize,
		ReplicaAddressMap: replicaAddressMap,
		Frontend:          frontend,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to start SPDK engine")
	}

	return api.ProtoEngineToEngine(resp), nil
}

func (c *SPDKClient) EngineDelete(name string) error {
	if name == "" {
		return fmt.Errorf("failed to delete SPDK engine: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineDelete(ctx, &spdkrpc.EngineDeleteRequest{
		Name: name,
	})
	return errors.Wrapf(err, "failed to delete SPDK engine %v", name)
}

func (c *SPDKClient) EngineGet(name string) (*api.Engine, error) {
	if name == "" {
		return nil, fmt.Errorf("failed to get SPDK engine: missing required parameter name")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineGet(ctx, &spdkrpc.EngineGetRequest{
		Name: name,
	})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get SPDK engine %v", name)
	}
	return api.ProtoEngineToEngine(resp), nil
}

func (c *SPDKClient) EngineList() (map[string]*api.Engine, error) {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	resp, err := client.EngineList(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list SPDK engines")
	}

	res := map[string]*api.Engine{}
	for engineName, e := range resp.Engines {
		res[engineName] = api.ProtoEngineToEngine(e)
	}
	return res, nil
}

func (c *SPDKClient) EngineWatch(ctx context.Context) (*api.EngineStream, error) {
	client := c.getSPDKServiceClient()
	stream, err := client.EngineWatch(ctx, &empty.Empty{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open engine watch stream")
	}

	return api.NewEngineStream(stream), nil
}

func (c *SPDKClient) EngineReplicaDelete(engineName, replicaName, replicaAddress string) error {
	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.EngineReplicaDelete(ctx, &spdkrpc.EngineReplicaDeleteRequest{
		EngineName:     engineName,
		ReplicaName:    replicaName,
		ReplicaAddress: replicaAddress,
	})
	if err != nil {
		return errors.Wrap(err, "failed to delete replica from engine")
	}

	return nil
}

func (c *SPDKClient) DiskCreate(diskName, diskPath string, blockSize int64) (*spdkrpc.Disk, error) {
	if diskName == "" || diskPath == "" {
		return nil, fmt.Errorf("failed to create disk: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.DiskCreate(ctx, &spdkrpc.DiskCreateRequest{
		DiskName:  diskName,
		DiskPath:  diskPath,
		BlockSize: blockSize,
	})
}

func (c *SPDKClient) DiskGet(diskName string) (*spdkrpc.Disk, error) {
	if diskName == "" {
		return nil, fmt.Errorf("failed to get disk info: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	return client.DiskGet(ctx, &spdkrpc.DiskGetRequest{
		DiskName: diskName,
	})
}

func (c *SPDKClient) DiskDelete(diskName, diskUUID string) error {
	if diskName == "" || diskUUID == "" {
		return fmt.Errorf("failed to delete disk: missing required parameter")
	}

	client := c.getSPDKServiceClient()
	ctx, cancel := context.WithTimeout(context.Background(), GRPCServiceTimeout)
	defer cancel()

	_, err := client.DiskDelete(ctx, &spdkrpc.DiskDeleteRequest{
		DiskName: diskName,
		DiskUuid: diskUUID,
	})
	return err
}
