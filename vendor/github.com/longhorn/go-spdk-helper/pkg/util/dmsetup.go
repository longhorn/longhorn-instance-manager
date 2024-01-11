package util

import (
	"regexp"

	commonNs "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/go-spdk-helper/pkg/types"
)

const (
	dmsetupBinary = "dmsetup"
)

// DmsetupCreate creates a device mapper device with the given name and table
func DmsetupCreate(dmDeviceName, table string, executor *commonNs.Executor) error {
	opts := []string{
		"create", dmDeviceName, "--table", table,
	}
	_, err := executor.Execute(dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupSuspend suspends the device mapper device with the given name
func DmsetupSuspend(dmDeviceName string, noflush, nolockfs bool, executor *commonNs.Executor) error {
	opts := []string{
		"suspend", dmDeviceName,
	}

	if noflush {
		opts = append(opts, "--noflush")
	}

	if nolockfs {
		opts = append(opts, "--nolockfs")
	}

	_, err := executor.Execute(dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupResume removes the device mapper device with the given name
func DmsetupResume(dmDeviceName string, executor *commonNs.Executor) error {
	opts := []string{
		"resume", dmDeviceName,
	}
	_, err := executor.Execute(dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupReload reloads the table of the device mapper device with the given name and table
func DmsetupReload(dmDeviceName, table string, executor *commonNs.Executor) error {
	opts := []string{
		"reload", dmDeviceName, "--table", table,
	}
	_, err := executor.Execute(dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupRemove removes the device mapper device with the given name
func DmsetupRemove(dmDeviceName string, force, deferred bool, executor *commonNs.Executor) error {
	opts := []string{
		"remove", dmDeviceName,
	}
	if force {
		opts = append(opts, "--force")
	}
	if deferred {
		opts = append(opts, "--deferred")
	}
	_, err := executor.Execute(dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupDeps returns the dependent devices of the device mapper device with the given name
func DmsetupDeps(dmDeviceName string, executor *commonNs.Executor) ([]string, error) {
	opts := []string{
		"deps", dmDeviceName, "-o", "devname",
	}

	outputStr, err := executor.Execute(dmsetupBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return nil, err
	}

	return parseDependentDevicesFromString(outputStr), nil
}

func parseDependentDevicesFromString(str string) []string {
	re := regexp.MustCompile(`\(([\w-]+)\)`)
	matches := re.FindAllStringSubmatch(str, -1)

	devices := make([]string, 0, len(matches))

	for _, match := range matches {
		devices = append(devices, match[1])
	}

	return devices
}
