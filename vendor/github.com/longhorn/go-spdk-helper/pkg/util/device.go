package util

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	lsblkBinary    = "lsblk"
	blockdevBinary = "blockdev"
)

type BlockDevice struct {
	Name   string `json:"name"`
	Major  int    `json:"maj"`
	Minor  int    `json:"min"`
	MajMin string `json:"maj:min"`
}

type BlockDevices struct {
	Devices []BlockDevice `json:"blockdevices"`
}

type KernelDevice struct {
	Nvme   BlockDevice
	Export BlockDevice
}

func RemoveDevice(dev string) error {
	if _, err := os.Stat(dev); err == nil {
		if err := remove(dev); err != nil {
			return errors.Wrapf(err, "failed to removing device %s", dev)
		}
	}
	return nil
}

func GetKnownDevices(executor Executor) (map[string]*KernelDevice, error) {
	knownDevices := make(map[string]*KernelDevice)

	/* Example command output
	   $ lsblk -l -n -o NAME,MAJ:MIN
	   sda           8:0
	   sdb           8:16
	   sdc           8:32
	   nvme0n1     259:0
	   nvme0n1p1   259:1
	   nvme0n1p128 259:2
	   nvme1n1     259:3
	*/

	opts := []string{
		"-l", "-n", "-o", "NAME,MAJ:MIN",
	}

	output, err := executor.Execute(lsblkBinary, opts)
	if err != nil {
		return knownDevices, err
	}

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		f := strings.Fields(line)
		if len(f) == 2 {
			dev := &KernelDevice{
				Nvme: BlockDevice{
					Name: f[0],
				},
			}
			if _, err := fmt.Sscanf(f[1], "%d:%d", &dev.Nvme.Major, &dev.Nvme.Minor); err != nil {
				return nil, fmt.Errorf("invalid major:minor %s for NVMe device %s", dev.Nvme.Name, f[1])
			}
			knownDevices[dev.Nvme.Name] = dev
		}
	}

	return knownDevices, nil
}

func DetectDevice(path string, executor Executor) (*KernelDevice, error) {
	/* Example command output
	   $ lsblk -l -n <Device Path> -o NAME,MAJ:MIN
	   nvme1n1     259:3
	*/

	opts := []string{
		"-l", "-n", path, "-o", "NAME,MAJ:MIN",
	}

	output, err := executor.Execute(lsblkBinary, opts)
	if err != nil {
		return nil, err
	}

	var dev *KernelDevice
	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		f := strings.Fields(line)
		if len(f) == 2 {
			dev = &KernelDevice{
				Nvme: BlockDevice{
					Name: f[0],
				},
			}
			if _, err := fmt.Sscanf(f[1], "%d:%d", &dev.Nvme.Major, &dev.Nvme.Minor); err != nil {
				return nil, fmt.Errorf("invalid major:minor %s for device %s with path %s", dev.Nvme.Name, f[1], path)
			}
		}
		break
	}
	if dev == nil {
		return nil, fmt.Errorf("failed to get device with path %s", path)
	}

	return dev, nil
}

func parseMajorMinorFromJSON(jsonStr string) (int, int, error) {
	var data BlockDevices
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to parse JSON")
	}

	if len(data.Devices) != 1 {
		return 0, 0, fmt.Errorf("number of devices is not 1")
	}

	majMinParts := splitMajMin(data.Devices[0].MajMin)

	if len(majMinParts) != 2 {
		return 0, 0, fmt.Errorf("invalid maj:min format: %s", data.Devices[0].MajMin)
	}

	major, err := parseNumber(majMinParts[0])
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to parse major number")
	}

	minor, err := parseNumber(majMinParts[1])
	if err != nil {
		return 0, 0, errors.Wrap(err, "failed to parse minor number")
	}

	return major, minor, nil
}

func splitMajMin(majMin string) []string {
	return splitIgnoreEmpty(majMin, ":")
}

func splitIgnoreEmpty(str string, sep string) []string {
	parts := []string{}
	for _, part := range strings.Split(str, sep) {
		if part != "" {
			parts = append(parts, part)
		}
	}
	return parts
}

func parseNumber(str string) (int, error) {
	return strconv.Atoi(strings.TrimSpace(str))
}

func SuspendLinearDmDevice(name string, executor Executor) error {
	logrus.Infof("Suspending linear dm device %s", name)

	return DmsetupSuspend(name, executor)
}

func ResumeLinearDmDevice(name string, executor Executor) error {
	logrus.Infof("Resuming linear dm device %s", name)

	return DmsetupResume(name, executor)
}

func ReloadLinearDmDevice(name string, dev *KernelDevice, executor Executor) error {
	devPath := fmt.Sprintf("/dev/%s", dev.Nvme.Name)

	// Get the size of the device
	opts := []string{
		"--getsize", devPath,
	}
	output, err := executor.Execute(blockdevBinary, opts)
	if err != nil {
		return err
	}
	sectors, err := strconv.ParseInt(strings.TrimSpace(output), 10, 64)
	if err != nil {
		return err
	}

	table := fmt.Sprintf("0 %v linear %v 0", sectors, devPath)

	logrus.Infof("Reloading linear dm device %s with table %s", name, table)

	return DmsetupReload(name, table, executor)
}

func getDeviceSectorSize(devPath string, executor Executor) (int64, error) {
	opts := []string{
		"--getsize", devPath,
	}

	output, err := executor.Execute(blockdevBinary, opts)
	if err != nil {
		return -1, err
	}

	return strconv.ParseInt(strings.TrimSpace(output), 10, 64)
}

func getDeviceNumbers(devPath string, executor Executor) (int, int, error) {
	opts := []string{
		"-l", "-J", "-n", "-o", "MAJ:MIN", devPath,
	}
	output, err := executor.Execute(lsblkBinary, opts)
	if err != nil {
		return -1, -1, err
	}

	return parseMajorMinorFromJSON(output)
}

func CreateLinearDmDevice(name string, dev *KernelDevice, executor Executor) error {
	if dev == nil {
		return fmt.Errorf("found nil device for linear dm device creation")
	}

	nvmeDevPath := fmt.Sprintf("/dev/%s", dev.Nvme.Name)
	sectors, err := getDeviceSectorSize(nvmeDevPath, executor)
	if err != nil {
		return err
	}

	// Create a device mapper device with the same size as the original device
	table := fmt.Sprintf("0 %v linear %v 0", sectors, nvmeDevPath)
	logrus.Infof("Creating linear dm device %s with table %s", name, table)
	err = DmsetupCreate(name, table, executor)
	if err != nil {
		return err
	}

	dmDevPath := fmt.Sprintf("/dev/mapper/%s", name)
	major, minor, err := getDeviceNumbers(dmDevPath, executor)
	if err != nil {
		return err
	}

	dev.Export.Name = name
	dev.Export.Major = major
	dev.Export.Minor = minor

	return nil
}

func RemoveLinearDmDevice(name string, executor Executor) error {
	devPath := fmt.Sprintf("/dev/mapper/%s", name)
	if _, err := os.Stat(devPath); err != nil {
		logrus.WithError(err).Warnf("Failed to stat linear dm device %s", devPath)
		return nil
	}

	logrus.Infof("Removing linear dm device %s", name)
	return DmsetupRemove(name, executor)
}

func DuplicateDevice(dev *KernelDevice, dest string) error {
	if dev == nil {
		return fmt.Errorf("found nil device for device duplication")
	}
	if dest == "" {
		return fmt.Errorf("found empty destination for device duplication")
	}
	dir := filepath.Dir(dest)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			logrus.WithError(err).Fatalf("device %v: Failed to create directory for %v", dev.Nvme.Name, dest)
		}
	}
	if err := mknod(dest, dev.Export.Major, dev.Export.Minor); err != nil {
		return errors.Wrapf(err, "cannot create device node %s for device %s", dest, dev.Nvme.Name)
	}
	if err := os.Chmod(dest, 0660); err != nil {
		return errors.Wrapf(err, "cannot change permission of the device %s", dest)
	}
	return nil
}

func mknod(device string, major, minor int) error {
	var fileMode os.FileMode = 0660
	fileMode |= unix.S_IFBLK
	dev := int(unix.Mkdev(uint32(major), uint32(minor)))

	logrus.Infof("Creating device %s %d:%d", device, major, minor)
	return unix.Mknod(device, uint32(fileMode), dev)
}

func removeAsync(path string, done chan<- error) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logrus.WithError(err).Errorf("Failed to remove %v", path)
		done <- err
	}
	done <- nil
}

func remove(path string) error {
	done := make(chan error)
	go removeAsync(path, done)
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		return fmt.Errorf("timeout trying to delete %s", path)
	}
}
