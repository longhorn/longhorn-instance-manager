package spdk

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

type SPDKClient struct {
	sync.Mutex
	client *spdkclient.Client
}

func NewSPDKClient() (*SPDKClient, error) {
	client, err := spdkclient.NewClient()
	if err != nil {
		return nil, err
	}
	return &SPDKClient{
		client: client,
	}, nil
}

func (c *SPDKClient) Reconnect() error {
	c.Lock()
	defer c.Unlock()

	oldClient := c.client

	client, err := spdkclient.NewClient()
	if err != nil {
		return errors.Wrap(err, "failed to create new SPDK client")
	}
	c.client = client

	// Try the best effort to close the old client after a new client is created
	err = oldClient.Close()
	if err != nil {
		logrus.WithError(err).Warn("Failed to close old SPDK client")
	}
	return nil
}

func (c *SPDKClient) Close() error {
	c.Lock()
	defer c.Unlock()
	return c.client.Close()
}

func (c *SPDKClient) BdevAioCreate(filePath, name string, blockSize uint64) (bdevName string, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevAioCreate(filePath, name, blockSize)
}

func (c *SPDKClient) BdevAioDelete(name string) (deleted bool, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevAioDelete(name)
}

func (c *SPDKClient) BdevAioGet(name string, timeout uint64) (bdevAioInfoList []spdktypes.BdevInfo, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevAioGet(name, timeout)
}

func (c *SPDKClient) BdevLvolCreate(lvstoreName, lvstoreUUID, lvolName string, sizeInMib uint64, clearMethod spdktypes.BdevLvolClearMethod, thinProvision bool) (uuid string, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevLvolCreate(lvstoreName, lvstoreUUID, lvolName, sizeInMib, clearMethod, thinProvision)
}

func (c *SPDKClient) BdevLvolGet(name string, timeout uint64) (bdevLvolInfoList []spdktypes.BdevInfo, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevLvolGet(name, timeout)
}

func (c *SPDKClient) BdevLvolDelete(name string) (deleted bool, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevLvolDelete(name)
}

func (c *SPDKClient) BdevLvolCreateLvstore(bdevName, lvsName string, clusterSize uint32) (uuid string, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevLvolCreateLvstore(bdevName, lvsName, clusterSize)
}

func (c *SPDKClient) BdevLvolGetLvstore(lvsName, uuid string) (lvstoreInfoList []spdktypes.LvstoreInfo, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevLvolGetLvstore(lvsName, uuid)
}

func (c *SPDKClient) BdevLvolRenameLvstore(oldName, newName string) (renamed bool, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevLvolRenameLvstore(oldName, newName)
}

func (c *SPDKClient) BdevLvolSnapshot(name, snapshotName string) (uuid string, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevLvolSnapshot(name, snapshotName)
}

func (c *SPDKClient) BdevLvolShallowCopy(srcLvolName, dstBdevName string) (copied bool, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevLvolShallowCopy(srcLvolName, dstBdevName)
}

func (c *SPDKClient) BdevGetBdevs(name string, timeout uint64) (bdevInfoList []spdktypes.BdevInfo, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevGetBdevs(name, timeout)
}

func (c *SPDKClient) NvmfGetSubsystems(name, tgtName string) (subsystemList []spdktypes.NvmfSubsystem, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.NvmfGetSubsystems(name, tgtName)
}

func (c *SPDKClient) BdevRaidCreate(name string, raidLevel spdktypes.BdevRaidLevel, stripSizeKb uint32, baseBdevs []string) (created bool, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevRaidCreate(name, raidLevel, stripSizeKb, baseBdevs)
}

func (c *SPDKClient) BdevRaidDelete(name string) (deleted bool, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevRaidDelete(name)
}

func (c *SPDKClient) BdevRaidRemoveBaseBdev(name string) (removed bool, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevRaidRemoveBaseBdev(name)
}

func (c *SPDKClient) BdevRaidGet(name string, timeout uint64) (bdevRaidInfoList []spdktypes.BdevInfo, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevRaidGet(name, timeout)
}

func (c *SPDKClient) StartExposeBdev(nqn, bdevName, ip, port string) error {
	c.Lock()
	defer c.Unlock()
	return c.client.StartExposeBdev(nqn, bdevName, ip, port)
}

func (c *SPDKClient) StopExposeBdev(nqn string) error {
	c.Lock()
	defer c.Unlock()
	return c.client.StopExposeBdev(nqn)
}

func (c *SPDKClient) BdevNvmeAttachController(name, subnqn, traddr, trsvcid string, trtype spdktypes.NvmeTransportType, adrfam spdktypes.NvmeAddressFamily, ctrlrLossTimeoutSec, reconnectDelaySec, fastIOFailTimeoutSec int32) (bdevNameList []string, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevNvmeAttachController(name, subnqn, traddr, trsvcid, trtype, adrfam, ctrlrLossTimeoutSec, reconnectDelaySec, fastIOFailTimeoutSec)
}

func (c *SPDKClient) BdevNvmeDetachController(name string) (detached bool, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevNvmeDetachController(name)
}

func (c *SPDKClient) BdevNvmeGetControllers(name string) (controllerInfoList []spdktypes.BdevNvmeControllerInfo, err error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevNvmeGetControllers(name)
}

func (c *SPDKClient) BdevNvmeSetOptions(ctrlrLossTimeoutSec, reconnectDelaySec, fastIOFailTimeoutSec, transportAckTimeout int32) (bool, error) {
	c.Lock()
	defer c.Unlock()
	return c.client.BdevNvmeSetOptions(ctrlrLossTimeoutSec, reconnectDelaySec, fastIOFailTimeoutSec, transportAckTimeout)
}
