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
func connectNVMfBdev(spdkClient *spdkclient.Client, controllerName, address string) (bdevName string, err error) {
	if controllerName == "" || address == "" {
		return "", fmt.Errorf("controllerName or address is empty")
	}

	ip, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}

	nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(controllerName, helpertypes.GetNQN(controllerName),
		ip, port, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4,
		helpertypes.DefaultCtrlrLossTimeoutSec, helpertypes.DefaultReconnectDelaySec, helpertypes.DefaultFastIOFailTimeoutSec,
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

// CleanupLvolTree retrieves the lvol tree with BFS. Then do cleanup bottom up
func CleanupLvolTree(spdkClient *spdkclient.Client, rootLvolName string, bdevLvolMap map[string]*spdktypes.BdevInfo) (err error) {
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
		if _, err := spdkClient.BdevLvolDelete(queue[idx].UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
	}
	return nil
}
