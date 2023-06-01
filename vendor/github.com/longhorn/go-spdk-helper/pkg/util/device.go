package util

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	LSBLKBinary = "lsblk"
)

type KernelDevice struct {
	Name  string
	Major int
	Minor int
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

	output, err := executor.Execute(LSBLKBinary, opts)
	if err != nil {
		return knownDevices, err
	}

	scanner := bufio.NewScanner(strings.NewReader(output))
	for scanner.Scan() {
		line := scanner.Text()
		f := strings.Fields(line)
		if len(f) == 2 {
			dev := &KernelDevice{
				Name: f[0],
			}
			if _, err := fmt.Sscanf(f[1], "%d:%d", &dev.Major, &dev.Minor); err != nil {
				return nil, fmt.Errorf("invalid major:minor %s for device %s", dev.Name, f[1])
			}
			knownDevices[dev.Name] = dev
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

	output, err := executor.Execute(LSBLKBinary, opts)
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
				Name: f[0],
			}
			if _, err := fmt.Sscanf(f[1], "%d:%d", &dev.Major, &dev.Minor); err != nil {
				return nil, fmt.Errorf("invalid major:minor %s for device %s with path %s", dev.Name, f[1], path)
			}
		}
		break
	}
	if dev == nil {
		return nil, fmt.Errorf("failed to get device with path %s", path)
	}

	return dev, nil
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
			logrus.Fatalf("device %v: Cannot create directory for %v", dev.Name, dest)
		}
	}
	if err := mknod(dest, dev.Major, dev.Minor); err != nil {
		return errors.Wrapf(err, "cannot create device node %s for device %s", dest, dev.Name)
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
		logrus.Errorf("Unable to remove: %v", path)
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
