package util

import (
	"fmt"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	commonTypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

func GetDevNameFromBDF(bdf string) (string, error) {
	ne, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrap(err, "failed to create executor")
	}

	cmdArgs := []string{"-n", "--nodeps", "--output", "NAME"}
	output, err := ne.Execute(nil, "lsblk", cmdArgs, types.ExecuteTimeout)
	if err != nil {
		return "", errors.Wrap(err, "failed to list block devices")
	}

	devices := strings.Fields(string(output))
	for _, dev := range devices {
		link, err := os.Readlink("/sys/block/" + dev)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to read link for %s", dev)
			continue
		}

		if strings.Contains(link, bdf) {
			return dev, nil
		}
	}

	return "", fmt.Errorf("failed to find device for BDF %s", bdf)
}

func GetBlockDiskSubsystems(devPath string) ([]string, error) {
	ne, err := helperutil.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create executor")
	}

	cmdArgs := []string{"-n", "-d", "-o", "subsystems", devPath}
	output, err := ne.Execute(nil, "lsblk", cmdArgs, types.ExecuteTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get disk subsystems")
	}

	return strings.Split(strings.TrimSpace(string(output)), ":"), nil
}
