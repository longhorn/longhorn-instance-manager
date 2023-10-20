package util

import (
	"regexp"
)

const (
	dmsetupBinary = "dmsetup"
)

func DmsetupCreate(dmDeviceName, table string, executor Executor) error {
	opts := []string{
		"create", dmDeviceName, "--table", table,
	}
	_, err := executor.Execute(dmsetupBinary, opts)
	return err
}

func DmsetupSuspend(dmDeviceName string, executor Executor) error {
	opts := []string{
		"suspend", dmDeviceName,
	}
	_, err := executor.Execute(dmsetupBinary, opts)
	return err
}

func DmsetupResume(dmDeviceName string, executor Executor) error {
	opts := []string{
		"resume", dmDeviceName,
	}
	_, err := executor.Execute(dmsetupBinary, opts)
	return err
}

func DmsetupReload(dmDeviceName, table string, executor Executor) error {
	opts := []string{
		"reload", dmDeviceName, "--table", table,
	}
	_, err := executor.Execute(dmsetupBinary, opts)
	return err
}

func DmsetupRemove(dmDeviceName string, executor Executor) error {
	opts := []string{
		"remove", dmDeviceName,
	}
	_, err := executor.Execute(dmsetupBinary, opts)
	return err
}

func DmsetupDeps(dmDeviceName string, executor Executor) ([]string, error) {
	opts := []string{
		"deps", dmDeviceName, "-o", "devname",
	}

	outputStr, err := executor.Execute(dmsetupBinary, opts)
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
