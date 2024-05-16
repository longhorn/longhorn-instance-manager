package spdk

import (
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/nvme"

	commonNs "github.com/longhorn/go-common-libs/ns"
	commonUtils "github.com/longhorn/go-common-libs/utils"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
)

func exposeSnapshotLvolBdev(spdkClient *spdkclient.Client, lvsName, lvolName, ip string, port int32, executor *commonNs.Executor) (subsystemNQN, controllerName string, err error) {
	bdevLvolList, err := spdkClient.BdevLvolGet(spdktypes.GetLvolAlias(lvsName, lvolName), 0)
	if err != nil {
		return "", "", err
	}
	if len(bdevLvolList) == 0 {
		return "", "", errors.Errorf("cannot find lvol bdev %v for backup", lvolName)
	}

	portStr := strconv.Itoa(int(port))
	nguid := commonUtils.RandomID(nvmeNguidLength)
	err = spdkClient.StartExposeBdev(helpertypes.GetNQN(lvolName), bdevLvolList[0].UUID, nguid, ip, portStr)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to expose snapshot lvol bdev %v", lvolName)
	}

	for r := 0; r < nvme.RetryCounts; r++ {
		subsystemNQN, err = nvme.DiscoverTarget(ip, portStr, executor)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to discover target for snapshot lvol bdev %v", lvolName)
			time.Sleep(nvme.RetryInterval)
			continue
		}

		controllerName, err = nvme.ConnectTarget(ip, portStr, subsystemNQN, executor)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to connect target for snapshot lvol bdev %v", lvolName)
			time.Sleep(nvme.RetryInterval)
			continue
		}
	}
	return subsystemNQN, controllerName, nil
}
