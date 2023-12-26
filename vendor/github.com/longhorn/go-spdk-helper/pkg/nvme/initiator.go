package nvme

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	commonNs "github.com/longhorn/go-common-libs/ns"
	commonTypes "github.com/longhorn/go-common-libs/types"
	"github.com/longhorn/nsfilelock"

	"github.com/longhorn/go-spdk-helper/pkg/types"
	"github.com/longhorn/go-spdk-helper/pkg/util"
)

const (
	LockFile    = "/var/run/longhorn-spdk.lock"
	LockTimeout = 120 * time.Second

	RetryCounts   = 5
	RetryInterval = 3 * time.Second

	waitDeviceTimeout = 60 * time.Second

	HostProc = "/host/proc"

	validateDiskCreationTimeout = 30 // seconds
)

type Initiator struct {
	Name               string
	SubsystemNQN       string
	UUID               string
	TransportAddress   string
	TransportServiceID string

	Endpoint       string
	ControllerName string
	NamespaceName  string
	dev            *util.KernelDevice
	isUp           bool

	hostProc string
	executor *commonNs.Executor

	logger logrus.FieldLogger
}

func NewInitiator(name, subsystemNQN, hostProc string) (*Initiator, error) {
	if name == "" || subsystemNQN == "" {
		return nil, fmt.Errorf("empty name or subsystem for initiator creation")
	}

	// If transportAddress or transportServiceID is empty, the initiator is still valid for stopping
	executor, err := util.NewExecutor(commonTypes.ProcDirectory)
	if err != nil {
		return nil, err
	}

	return &Initiator{
		Name:         name,
		SubsystemNQN: subsystemNQN,

		Endpoint: util.GetLonghornDevicePath(name),

		hostProc: hostProc,
		executor: executor,

		logger: logrus.WithFields(logrus.Fields{
			"name":         name,
			"subsystemNQN": subsystemNQN,
		}),
	}, nil
}

// Suspend suspends the device mapper device for the NVMe initiator
func (i *Initiator) Suspend() error {
	if i.hostProc != "" {
		lock := nsfilelock.NewLockWithTimeout(util.GetHostNamespacePath(i.hostProc), LockFile, LockTimeout)
		if err := lock.Lock(); err != nil {
			return errors.Wrapf(err, "failed to get file lock for initiator %s", i.Name)
		}
		defer lock.Unlock()
	}

	if err := i.suspendLinearDmDevice(); err != nil {
		return errors.Wrapf(err, "failed to suspend device mapper device for NVMe initiator %s", i.Name)
	}

	return nil
}

// Start starts the NVMe initiator with the given transportAddress and transportServiceID
func (i *Initiator) Start(transportAddress, transportServiceID string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to start NVMe initiator %s", i.Name)
		}
	}()

	if transportAddress == "" || transportServiceID == "" {
		return fmt.Errorf("invalid TransportAddress %s and TransportServiceID %s for initiator %s start", transportAddress, transportServiceID, i.Name)
	}

	if i.hostProc != "" {
		lock := nsfilelock.NewLockWithTimeout(util.GetHostNamespacePath(i.hostProc), LockFile, LockTimeout)
		if err := lock.Lock(); err != nil {
			return errors.Wrapf(err, "failed to get file lock for initiator %s", i.Name)
		}
		defer lock.Unlock()
	}

	// Check if the initiator/NVMe device is already launched and matches the params
	if err := i.loadNVMeDeviceInfoWithoutLock(); err == nil {
		if i.TransportAddress == transportAddress && i.TransportServiceID == transportServiceID {
			i.logger.WithFields(logrus.Fields{
				"transportAddress":   transportAddress,
				"transportServiceID": transportServiceID,
			})
			if err = i.LoadEndpoint(); err == nil {
				i.logger.Info("NVMe initiator is already launched with correct params")
				return nil
			}
			i.logger.WithError(err).Warnf("NVMe initiator is launched with failed to load the endpoint")
		} else {
			i.logger.Warnf("NVMe initiator is launched but with incorrect address, the required one is %s:%s, will try to stop then relaunch it",
				transportAddress, transportServiceID)
		}
	}

	i.logger.Infof("Stopping NVMe initiator blindly before starting")
	if err := i.stopWithoutLock(); err != nil {
		return errors.Wrapf(err, "failed to stop the mismatching NVMe initiator %s before starting", i.Name)
	}

	i.logger.WithFields(logrus.Fields{
		"transportAddress":   transportAddress,
		"transportServiceID": transportServiceID,
	})
	i.logger.Infof("Launching NVMe initiator")

	// Setup initiator
	for counter := 0; counter < RetryCounts; counter++ {
		// Rerun this API for a discovered target should be fine
		if i.SubsystemNQN, err = DiscoverTarget(transportAddress, transportServiceID, i.executor); err != nil {
			i.logger.WithError(err).Warnf("Failed to discover")
			time.Sleep(RetryInterval)
			continue
		}

		if i.ControllerName, err = ConnectTarget(transportAddress, transportServiceID, i.SubsystemNQN, i.executor); err != nil {
			i.logger.WithError(err).Warnf("Failed to connect target")
			time.Sleep(RetryInterval)
			continue
		}
		break
	}

	if i.ControllerName == "" {
		return fmt.Errorf("failed to start NVMe initiator %s within %d * %v sec retries", i.Name, RetryCounts, RetryInterval.Seconds())
	}

	for t := 0; t < int(waitDeviceTimeout.Seconds()); t++ {
		if err = i.loadNVMeDeviceInfoWithoutLock(); err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return errors.Wrapf(err, "failed to load device info after starting NVMe initiator %s", i.Name)
	}

	i.logger.Infof("Creating linear dm device for NVMe initiator %s", i.Name)
	if err := i.createLinearDmDevice(); err != nil {
		return errors.Wrapf(err, "failed to create linear dm device for NVMe initiator %s", i.Name)
	}

	i.logger.Infof("Creating endpoint %v", i.Endpoint)
	if err := i.makeEndpoint(); err != nil {
		return err
	}

	i.logger.Infof("Launched NVMe initiator: %+v", i)

	return nil
}

func (i *Initiator) Stop() error {
	if i.hostProc != "" {
		lock := nsfilelock.NewLockWithTimeout(util.GetHostNamespacePath(i.hostProc), LockFile, LockTimeout)
		if err := lock.Lock(); err != nil {
			return errors.Wrapf(err, "failed to get file lock for NVMe initiator %s", i.Name)
		}
		defer lock.Unlock()
	}

	return i.stopWithoutLock()
}

func (i *Initiator) stopWithoutLock() error {
	if err := i.removeEndpoint(); err != nil {
		return err
	}

	if err := i.removeLinearDmDevice(true, true); err != nil {
		return err
	}

	if err := DisconnectTarget(i.SubsystemNQN, i.executor); err != nil {
		return errors.Wrapf(err, "failed to logout target")
	}

	i.ControllerName = ""
	i.NamespaceName = ""
	i.TransportAddress = ""
	i.TransportServiceID = ""

	return nil
}

func (i *Initiator) GetControllerName() string {
	return i.ControllerName
}

func (i *Initiator) GetNamespaceName() string {
	return i.NamespaceName
}

func (i *Initiator) GetTransportAddress() string {
	return i.TransportAddress
}

func (i *Initiator) GetTransportServiceID() string {
	return i.TransportServiceID
}

func (i *Initiator) GetEndpoint() string {
	if i.isUp {
		return i.Endpoint
	}
	return ""
}

func (i *Initiator) LoadNVMeDeviceInfo() (err error) {
	if i.hostProc != "" {
		lock := nsfilelock.NewLockWithTimeout(util.GetHostNamespacePath(i.hostProc), LockFile, LockTimeout)
		if err := lock.Lock(); err != nil {
			return errors.Wrapf(err, "failed to get file lock for NVMe initiator %s", i.Name)
		}
		defer lock.Unlock()
	}

	return i.loadNVMeDeviceInfoWithoutLock()
}

func (i *Initiator) loadNVMeDeviceInfoWithoutLock() error {
	nvmeDevices, err := GetDevices(i.TransportAddress, i.TransportServiceID, i.SubsystemNQN, i.executor)
	if err != nil {
		return err
	}
	if len(nvmeDevices) != 1 {
		return fmt.Errorf("found zero or multiple devices NVMe initiator %s", i.Name)
	}
	if len(nvmeDevices[0].Namespaces) != 1 {
		return fmt.Errorf("found zero or multiple devices for NVMe initiator %s", i.Name)
	}
	if i.ControllerName != "" && i.ControllerName != nvmeDevices[0].Controllers[0].Controller {
		return fmt.Errorf("found mismatching between the detected controller name %s and the recorded value %s for NVMe initiator %s", nvmeDevices[0].Controllers[0].Controller, i.ControllerName, i.Name)
	}
	i.ControllerName = nvmeDevices[0].Controllers[0].Controller
	i.NamespaceName = nvmeDevices[0].Namespaces[0].NameSpace
	i.TransportAddress, i.TransportServiceID = GetIPAndPortFromControllerAddress(nvmeDevices[0].Controllers[0].Address)
	i.logger.WithFields(logrus.Fields{
		"controllerName":     i.ControllerName,
		"namespaceName":      i.NamespaceName,
		"transportAddress":   i.TransportAddress,
		"transportServiceID": i.TransportServiceID,
	})

	devicePath := fmt.Sprintf("/dev/%s", i.NamespaceName)
	dev, err := util.DetectDevice(devicePath, i.executor)
	if err != nil {
		return errors.Wrapf(err, "cannot find the device for NVMe initiator %s with namespace name %s", i.Name, i.NamespaceName)
	}

	i.dev = dev
	return nil
}

func (i *Initiator) isNamespaceExist(devices []string) bool {
	for _, device := range devices {
		if device == i.NamespaceName {
			return true
		}
	}
	return false
}

func (i *Initiator) findDependentDevices(devName string) ([]string, error) {
	depDevices, err := util.DmsetupDeps(devName, i.executor)
	if err != nil {
		return nil, err
	}
	return depDevices, nil
}

func (i *Initiator) LoadEndpoint() error {
	dev, err := util.DetectDevice(i.Endpoint, i.executor)
	if err != nil {
		return err
	}

	depDevices, err := i.findDependentDevices(dev.Nvme.Name)
	if err != nil {
		return err
	}

	if i.NamespaceName != "" && !i.isNamespaceExist(depDevices) {
		return fmt.Errorf("detected device %s name mismatching from endpoint %v for NVMe initiator %s", dev.Nvme.Name, i.Endpoint, i.Name)
	}
	i.dev = dev
	i.isUp = true

	return nil
}

func (i *Initiator) makeEndpoint() error {
	if err := util.DuplicateDevice(i.dev, i.Endpoint); err != nil {
		return errors.Wrap(err, "failed to duplicate device")
	}
	i.isUp = true
	return nil
}

func (i *Initiator) removeEndpoint() error {
	if err := util.RemoveDevice(i.Endpoint); err != nil {
		return err
	}
	i.dev = nil
	i.isUp = false

	return nil
}

func (i *Initiator) removeLinearDmDevice(force, deferred bool) error {
	devPath := getDmDevicePath(i.Name)
	if _, err := os.Stat(devPath); err != nil {
		if os.IsNotExist(err) {
			logrus.Infof("Linear dm device %s doesn't exist", devPath)
			return nil
		}
		return errors.Wrapf(err, "failed to stat linear dm device %s", devPath)
	}

	logrus.Infof("Removing linear dm device %s", i.Name)
	return util.DmsetupRemove(i.Name, force, deferred, i.executor)
}

func (i *Initiator) createLinearDmDevice() error {
	if i.dev == nil {
		return fmt.Errorf("found nil device for linear dm device creation")
	}

	nvmeDevPath := fmt.Sprintf("/dev/%s", i.dev.Nvme.Name)
	sectors, err := util.GetDeviceSectorSize(nvmeDevPath, i.executor)
	if err != nil {
		return err
	}

	// Create a device mapper device with the same size as the original device
	table := fmt.Sprintf("0 %v linear %v 0", sectors, nvmeDevPath)
	logrus.Infof("Creating linear dm device %s with table %s", i.Name, table)
	if err := util.DmsetupCreate(i.Name, table, i.executor); err != nil {
		return err
	}

	dmDevPath := getDmDevicePath(i.Name)
	if err := validateDiskCreation(dmDevPath, validateDiskCreationTimeout); err != nil {
		return err
	}

	major, minor, err := util.GetDeviceNumbers(dmDevPath, i.executor)
	if err != nil {
		return err
	}

	i.dev.Export.Name = i.Name
	i.dev.Export.Major = major
	i.dev.Export.Minor = minor

	return nil
}

func validateDiskCreation(path string, timeout int) error {
	for i := 0; i < timeout; i++ {
		isBlockDev, _ := util.IsBlockDevice(path)
		if isBlockDev {
			return nil
		}
		time.Sleep(time.Second * 1)
	}

	return fmt.Errorf("failed to validate device %s creation", path)
}

func (i *Initiator) suspendLinearDmDevice() error {
	logrus.Infof("Suspending linear dm device %s", i.Name)

	return util.DmsetupSuspend(i.Name, i.executor)
}

func (i *Initiator) resumeLinearDmDevice() error {
	logrus.Infof("Resuming linear dm device %s", i.Name)

	return util.DmsetupResume(i.Name, i.executor)
}

func (i *Initiator) reloadLinearDmDevice() error {
	devPath := fmt.Sprintf("/dev/%s", i.dev.Nvme.Name)

	// Get the size of the device
	opts := []string{
		"--getsize", devPath,
	}
	output, err := i.executor.Execute(util.BlockdevBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return err
	}
	sectors, err := strconv.ParseInt(strings.TrimSpace(output), 10, 64)
	if err != nil {
		return err
	}

	table := fmt.Sprintf("0 %v linear %v 0", sectors, devPath)

	logrus.Infof("Reloading linear dm device %s with table %s", i.Name, table)

	return util.DmsetupReload(i.Name, table, i.executor)
}

func getDmDevicePath(name string) string {
	return fmt.Sprintf("/dev/mapper/%s", name)
}
