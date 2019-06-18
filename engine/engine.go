package engine

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-iscsi-helper/iscsiblk"
	"github.com/longhorn/longhorn-engine/controller/client"
	eutil "github.com/longhorn/longhorn-engine/util"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
	"github.com/longhorn/longhorn-engine-launcher/util"
)

const (
	FrontendTGTBlockDev = "tgt-blockdev"
	FrontendTGTISCSI    = "tgt-iscsi"

	SocketDirectory = "/var/run"
	DevPath         = "/dev/longhorn/"

	WaitInterval = time.Second
	WaitCount    = 60

	SwitchWaitInterval = time.Second
	SwitchWaitCount    = 15
)

type Engine struct {
	lock *sync.RWMutex

	Name             string
	VolumeName       string
	Binary           string
	ListenAddr       string
	Listen           string
	Size             int64
	Frontend         string
	Backends         []string
	Replicas         []string
	BackupListenPort int

	backupListen string
	backupBinary string

	scsiDevice *iscsiblk.ScsiDevice

	Endpoint string
}

func NewEngine(spec *rpc.EngineSpec) *Engine {
	e := &Engine{
		Name:             spec.Name,
		VolumeName:       spec.VolumeName,
		Binary:           spec.Binary,
		Size:             spec.Size,
		Listen:           spec.Listen,
		ListenAddr:       spec.ListenAddr,
		Frontend:         spec.Frontend,
		Backends:         spec.Backends,
		Replicas:         spec.Replicas,
		BackupListenPort: 0,

		lock: &sync.RWMutex{},
	}
	return e
}

func (e *Engine) RPCResponse() *rpc.EngineResponse {
	e.lock.RLock()
	defer e.lock.RUnlock()

	return &rpc.EngineResponse{
		Spec: &rpc.EngineSpec{
			Name:       e.Name,
			VolumeName: e.VolumeName,
			Binary:     e.Binary,
			Listen:     e.Listen,
			ListenAddr: e.ListenAddr,
			Size:       e.Size,
			Frontend:   e.Frontend,
			Backends:   e.Backends,
			Replicas:   e.Replicas,
		},
		Status: &rpc.EngineStatus{
			Endpoint: e.Endpoint,
		},
	}
}

func (e *Engine) startFrontend(frontend string) error {
	controllerCli := client.NewControllerClient(e.Listen)

	if err := controllerCli.VolumeFrontendStart(frontend); err != nil {
		return err
	}

	return nil
}

func (e *Engine) shutdownFrontend() error {
	controllerCli := client.NewControllerClient(e.Listen)

	if err := controllerCli.VolumeFrontendShutdown(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) finishStartFrontend(tID int) error {
	// not going to use it
	stopCh := make(chan struct{})
	if err := <-e.WaitForSocket(stopCh); err != nil {
		return err
	}
	if e.scsiDevice == nil {
		bsOpts := fmt.Sprintf("size=%v", e.Size)
		scsiDev, err := iscsiblk.NewScsiDevice(e.VolumeName, e.GetSocketPath(), "longhorn", bsOpts, tID)
		if err != nil {
			return err
		}
		e.scsiDevice = scsiDev

		switch e.Frontend {
		case FrontendTGTBlockDev:
			if err := iscsiblk.StartScsi(e.scsiDevice); err != nil {
				return err
			}
			if err := e.createDev(); err != nil {
				return err
			}
			logrus.Infof("launcher: SCSI device %s created", e.scsiDevice.Device)
			break
		case FrontendTGTISCSI:
			if err := iscsiblk.SetupTarget(e.scsiDevice); err != nil {
				return err
			}
			logrus.Infof("launcher: iSCSI target %s created", e.scsiDevice.Target)
			break
		default:
			return fmt.Errorf("unknown frontend %v", e.Frontend)
		}
	}

	return nil
}

func (e *Engine) finishShutdownFrontend() (int, error) {
	if e.scsiDevice == nil {
		return 0, nil
	}
	switch e.Frontend {
	case FrontendTGTBlockDev:
		dev := e.getDev()
		if err := eutil.RemoveDevice(dev); err != nil {
			return 0, fmt.Errorf("Fail to remove device %s: %v", dev, err)
		}
		if err := iscsiblk.StopScsi(e.VolumeName, e.scsiDevice.TargetID); err != nil {
			return 0, fmt.Errorf("Fail to stop SCSI device: %v", err)
		}
		logrus.Infof("launcher: SCSI device %v shutdown", dev)
		break
	case FrontendTGTISCSI:
		if err := iscsiblk.DeleteTarget(e.scsiDevice.Target, e.scsiDevice.TargetID); err != nil {
			return 0, fmt.Errorf("Fail to delete target %v", e.scsiDevice.Target)
		}
		logrus.Infof("launcher: SCSI target %v ", e.scsiDevice.Target)
		break
	case "":
		logrus.Infof("launcher: skip shutdown frontend since it's not enabeld")
		break
	default:
		return 0, fmt.Errorf("unknown frontend %v", e.Frontend)
	}

	tID := e.scsiDevice.TargetID
	e.scsiDevice = nil

	return tID, nil
}

func (e *Engine) PrepareUpgrade() error {
	logrus.Infof("launcher: prepare for upgrade")
	if err := e.BackupBinary(); err != nil {
		return errors.Wrap(err, "failed to backup old controller binary")
	}
	if err := e.SwitchPortToBackup(); err != nil {
		return errors.Wrapf(err, "failed to ask old controller to switch listening port %v", e.BackupListenPort)
	}
	logrus.Infof("launcher: preparation completed")
	return nil
}

func (e *Engine) RollbackUpgrade() error {
	logrus.Infof("launcher: rolling back upgrade")
	if err := e.SwitchPortToOriginal(); err != nil {
		return errors.Wrap(err, "failed to restore original port")
	}
	if err := e.RestoreBackupBinary(); err != nil {
		return errors.Wrap(err, "failed to restore old controller binary")
	}
	logrus.Infof("launcher: rollback completed")
	return nil
}

func (e *Engine) BackupBinary() error {
	if e.backupBinary != "" {
		logrus.Warnf("launcher: backup binary %v already exists", e.backupBinary)
		return nil
	}
	backupBinary := e.Binary + ".bak"
	if err := util.CopyFile(e.Binary, backupBinary); err != nil {
		return errors.Wrapf(err, "cannot make backup of %v", e.Binary)
	}
	e.backupBinary = backupBinary
	logrus.Infof("launcher: backup binary %v to %v", e.Binary, e.backupBinary)
	return nil
}

func (e *Engine) RemoveBackupBinary() error {
	if e.backupBinary == "" {
		logrus.Warnf("launcher: backup binary %v already removed", e.backupBinary)
		return nil
	}
	if err := util.RemoveFile(e.backupBinary); err != nil {
		return errors.Wrapf(err, "cannot remove backup binary %v", e.backupBinary)
	}
	logrus.Infof("launcher: removed backup binary %v", e.backupBinary)
	e.backupBinary = ""
	return nil
}

func (e *Engine) RestoreBackupBinary() error {
	if e.backupBinary == "" {
		return fmt.Errorf("cannot restore, backup binary doesn't exist")
	}
	if err := util.RemoveFile(e.Binary); err != nil {
		return errors.Wrapf(err, "cannot remove original binary %v", e.Binary)
	}
	if err := util.CopyFile(e.backupBinary, e.Binary); err != nil {
		return errors.Wrapf(err, "cannot restore backup of %v from %v", e.Binary, e.backupBinary)
	}
	logrus.Infof("launcher: backup binary %v restored to %v", e.backupBinary, e.Binary)
	if err := e.RemoveBackupBinary(); err != nil {
		return errors.Wrapf(err, "failed to clean up backup binary %v", e.backupBinary)
	}
	return nil
}

func (e *Engine) SwitchPortToBackup() (err error) {
	controllerCli := client.NewControllerClient(e.Listen)
	if err := controllerCli.PortUpdate(e.BackupListenPort); err != nil {
		return err
	}

	addrs := strings.Split(e.Listen, ":")
	addr := addrs[0]
	e.backupListen = addr + ":" + strconv.Itoa(e.BackupListenPort)

	controllerCli = client.NewControllerClient(e.backupListen)
	for i := 0; i < SwitchWaitCount; i++ {
		if err := controllerCli.Check(); err == nil {
			break
		}
		logrus.Infof("launcher: wait for controller to switch to %v", e.backupListen)
		time.Sleep(SwitchWaitInterval)
	}
	if err := controllerCli.Check(); err != nil {
		return errors.Wrapf(err, "test connection to %v failed", e.backupListen)
	}
	logrus.Infof("launcher: original controller updated listen to %v", e.backupListen)
	return nil
}

func (e *Engine) SwitchPortToOriginal() (err error) {
	if e.backupListen == "" {
		return fmt.Errorf("backup listen wasn't set")
	}
	addrs := strings.Split(e.Listen, ":")
	port, err := strconv.Atoi(addrs[len(addrs)-1])
	if err != nil {
		return fmt.Errorf("unable to parse listen port %v", e.Listen)
	}

	controllerCli := client.NewControllerClient(e.backupListen)
	if err := controllerCli.PortUpdate(port); err != nil {
		return err
	}

	controllerCli = client.NewControllerClient(e.Listen)
	for i := 0; i < SwitchWaitCount; i++ {
		if err := controllerCli.Check(); err == nil {
			break
		}
		logrus.Infof("launcher: wait for controller to switch to %v", e.Listen)
		time.Sleep(SwitchWaitInterval)
	}
	if err := controllerCli.Check(); err != nil {
		return errors.Wrapf(err, "test connection to %v failed", e.Listen)
	}
	e.backupListen = ""
	logrus.Infof("launcher: controller updated listen to %v", e.Listen)
	return nil
}

func (e *Engine) GetSocketPath() string {
	if e.VolumeName == "" {
		panic("Invalid volume name")
	}
	return filepath.Join(SocketDirectory, "longhorn-"+e.VolumeName+".sock")
}

func (e *Engine) WaitForSocket(stopCh chan struct{}) chan error {
	errCh := make(chan error)
	go func(errCh chan error, stopCh chan struct{}) {
		socket := e.GetSocketPath()
		timeout := time.After(time.Duration(WaitCount) * WaitInterval)
		tick := time.Tick(WaitInterval)
		for {
			select {
			case <-timeout:
				errCh <- fmt.Errorf("launcher: wait for socket %v timed out", socket)
			case <-tick:
				if _, err := os.Stat(socket); err == nil {
					errCh <- nil
					return
				}
				logrus.Infof("launcher: wait for socket %v to show up", socket)
			case <-stopCh:
				logrus.Infof("launcher: stop wait for socket routine")
				return
			}
		}
	}(errCh, stopCh)

	return errCh
}

func (e *Engine) ReloadSocketConnection() error {
	cmd := exec.Command("sg_raw", e.getDev(), "a6", "00", "00", "00", "00", "00")
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to reload socket connection")
	}
	return nil
}

func (e *Engine) getDev() string {
	return filepath.Join(DevPath, e.VolumeName)
}

func (e *Engine) createDev() error {
	if _, err := os.Stat(DevPath); os.IsNotExist(err) {
		if err := os.MkdirAll(DevPath, 0755); err != nil {
			logrus.Fatalln("launcher: Cannot create directory ", DevPath)
		}
	}

	dev := e.getDev()
	if _, err := os.Stat(dev); err == nil {
		logrus.Warnf("Device %s already exists, clean it up", dev)
		if err := eutil.RemoveDevice(dev); err != nil {
			return errors.Wrapf(err, "cannot cleanup block device file %v", dev)
		}
	}

	if err := eutil.DuplicateDevice(e.scsiDevice.Device, dev); err != nil {
		return err
	}
	logrus.Infof("launcher: Device %s is ready", dev)
	return nil
}
