package spdk

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/go-spdk-helper/pkg/nvme"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	commonns "github.com/longhorn/go-common-libs/ns"
	commonutils "github.com/longhorn/go-common-libs/utils"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
)

func exposeSnapshotLvolBdev(spdkClient *spdkclient.Client, lvsName, lvolName, ip string, port int32, executor *commonns.Executor) (subsystemNQN, controllerName string, err error) {
	bdevLvolList, err := spdkClient.BdevLvolGet(spdktypes.GetLvolAlias(lvsName, lvolName), 0)
	if err != nil {
		return "", "", err
	}
	if len(bdevLvolList) == 0 {
		return "", "", errors.Errorf("cannot find lvol bdev %v for backup", lvolName)
	}

	portStr := strconv.Itoa(int(port))
	nguid := commonutils.RandomID(nvmeNguidLength)
	err = spdkClient.StartExposeBdev(helpertypes.GetNQN(lvolName), bdevLvolList[0].UUID, nguid, ip, portStr)
	if err != nil {
		return "", "", errors.Wrapf(err, "failed to expose snapshot lvol bdev %v", lvolName)
	}

	for r := 0; r < maxNumRetries; r++ {
		subsystemNQN, err = nvme.DiscoverTarget(ip, portStr, executor)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to discover target for snapshot lvol bdev %v", lvolName)
			time.Sleep(retryInterval)
			continue
		}

		controllerName, err = nvme.ConnectTarget(ip, portStr, subsystemNQN, executor)
		if err != nil {
			logrus.WithError(err).Errorf("Failed to connect target for snapshot lvol bdev %v", lvolName)
			time.Sleep(retryInterval)
			continue
		}
	}
	return subsystemNQN, controllerName, nil
}

func splitHostPort(address string) (string, int32, error) {
	if strings.Contains(address, ":") {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return "", 0, errors.Wrapf(err, "failed to split host and port from address %v", address)
		}

		portAsInt := 0
		if port != "" {
			portAsInt, err = strconv.Atoi(port)
			if err != nil {
				return "", 0, errors.Wrapf(err, "failed to parse port %v", port)
			}
		}
		return host, int32(portAsInt), nil
	}

	return address, 0, nil
}

// connectNVMfBdev connects to the NVMe-oF target, which is exposed by a remote lvol bdev.
// controllerName is typically the lvol name, and address is the IP:port of the NVMe-oF target.
func connectNVMfBdev(spdkClient *spdkclient.Client, controllerName, address string, ctrlrLossTimeout, fastIOFailTimeoutSec int) (bdevName string, err error) {
	if controllerName == "" || address == "" {
		return "", fmt.Errorf("controllerName or address is empty")
	}

	ip, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}

	nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(controllerName, helpertypes.GetNQN(controllerName),
		ip, port, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4,
		int32(ctrlrLossTimeout), helpertypes.DefaultReplicaReconnectDelaySec, int32(fastIOFailTimeoutSec),
		helpertypes.DefaultMultipath)
	if err != nil {
		return "", err
	}

	if len(nvmeBdevNameList) != 1 {
		return "", fmt.Errorf("got zero or multiple results when attaching lvol %s with address %s as a NVMe bdev: %+v", controllerName, address, nvmeBdevNameList)
	}

	return nvmeBdevNameList[0], nil
}

func disconnectNVMfBdev(spdkClient *spdkclient.Client, bdevName string) error {
	if bdevName == "" {
		return nil
	}
	if _, err := spdkClient.BdevNvmeDetachController(helperutil.GetNvmeControllerNameFromNamespaceName(bdevName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}
	return nil
}

// CleanupLvolTree retrieves the lvol tree with BFS. Then try its best effort to do cleanup bottom up.
func CleanupLvolTree(spdkClient *spdkclient.Client, rootLvolName string, bdevLvolMap map[string]*spdktypes.BdevInfo, log logrus.FieldLogger) {
	var queue []*spdktypes.BdevInfo
	if bdevLvolMap[rootLvolName] != nil {
		queue = []*spdktypes.BdevInfo{bdevLvolMap[rootLvolName]}
	}
	for idx := 0; idx < len(queue); idx++ {
		for _, childLvolName := range queue[idx].DriverSpecific.Lvol.Clones {
			if bdevLvolMap[childLvolName] != nil {
				queue = append(queue, bdevLvolMap[childLvolName])
			}
		}
	}
	for idx := len(queue) - 1; idx >= 0; idx-- {
		// This may fail since there may be a rebuilding failed replicas on the same host that leaves an orphan rebuilding lvol as a child of a snapshot lvol.
		// Then this snapshot lvol would have multiple children then cannot be deleted.
		if _, err := spdkClient.BdevLvolDelete(queue[idx].UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			log.WithError(err).Errorf("Failed to delete lvol %v(%v) from the lvol tree with root %v(%s), this lvol may accidentally have some leftover orphans children %+v, will continue", queue[idx].Aliases[0], queue[idx].UUID, bdevLvolMap[rootLvolName].Aliases[0], bdevLvolMap[rootLvolName].UUID, queue[idx].DriverSpecific.Lvol.Clones)
		}
	}
}
