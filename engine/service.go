package engine

import (
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-engine-launcher/rpc"
	"github.com/longhorn/longhorn-engine-launcher/util"
)

type Service struct {
	lock     *sync.RWMutex
	launcher rpc.LonghornProcessLauncherServiceServer
	listen   string

	engines       map[string]*Engine
	availableTIDs *util.Bitmap
}

const (
	MaxTgtTargetNumber = 4096
)

func NewService(l rpc.LonghornProcessLauncherServiceServer, listen string) (*Service, error) {
	return &Service{
		lock:     &sync.RWMutex{},
		launcher: l,
		listen:   listen,

		engines:       map[string]*Engine{},
		availableTIDs: util.NewBitmap(1, MaxTgtTargetNumber),
	}, nil
}

func (s *Service) EngineCreate(ctx context.Context, req *rpc.EngineCreateRequest) (ret *rpc.EngineResponse, err error) {
	e := NewEngine(req.Spec)
	if err := s.registerEngine(e); err != nil {
		return nil, errors.Wrapf(err, "failed to register engine %v", e.Name)
	}
	if err := s.startEngine(e); err != nil {
		s.unregisterEngine(e)
		return nil, errors.Wrapf(err, "failed to start engine %v", e.Name)
	}
	// Field Listen maybe updated. Filed backupListen need to be set
	s.updateEngine(e)

	logrus.Infof("Engine %v has been created", e.Name)

	return e.RPCResponse(), nil
}

func (s *Service) registerEngine(e *Engine) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, exists := s.engines[e.Name]
	if exists {
		return fmt.Errorf("engine %v already exists", e.Name)
	}

	s.engines[e.Name] = e
	return nil
}

func (s *Service) updateEngine(e *Engine) {
	s.lock.Lock()
	defer s.lock.Unlock()

	s.engines[e.Name] = e
	return
}

func (s *Service) unregisterEngine(e *Engine) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	_, exists := s.engines[e.Name]
	if !exists {
		return nil
	}

	delete(s.engines, e.Name)

	return nil
}

// During running this function, FrontendStartCallback() will be called automatically.
// Hence need to be careful about deadlock
func (s *Service) startEngine(e *Engine) error {
	portArgs := []string{}
	portCount := 1

	args := []string{
		"controller", e.VolumeName,
		"--launcher", s.listen,
		"--launcher-id", e.Name,
	}
	if e.Listen != "" {
		args = append(args, "--listen", e.Listen)
	} else {
		if e.ListenAddr == "" {
			return fmt.Errorf("neither arg listen nor arg listenAddr is provided for engine %v", e.Name)
		}
		portArgs = append(portArgs, "--listen,"+e.ListenAddr)
		portCount = portCount + 1
	}
	if e.Frontend != "" {
		args = append(args, "--frontend", "socket")
	}
	for _, b := range e.Backends {
		args = append(args, "--enable-backend", b)
	}
	for _, r := range e.Replicas {
		args = append(args, "--replica", r)
	}
	req := &rpc.ProcessCreateRequest{
		Spec: &rpc.ProcessSpec{
			Name:      e.Name,
			Binary:    e.Binary,
			Args:      args,
			PortArgs:  portArgs,
			PortCount: int32(portCount),
		},
	}
	ret, err := s.launcher.ProcessCreate(nil, req)
	if err != nil {
		return err
	}

	if e.Listen == "" {
		e.Listen = e.ListenAddr + strconv.Itoa(int(ret.Status.PortStart))
	}
	e.BackupListenPort = int(ret.Status.PortEnd)

	return nil
}

func (s *Service) EngineDelete(ctx context.Context, req *rpc.EngineRequest) (*empty.Empty, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	e, exists := s.engines[req.Name]
	if !exists {
		return &empty.Empty{}, nil
	}

	// TODO: check if this is graceful delete
	if err := s.deleteEngine(e); err != nil {
		return nil, err
	}

	logrus.Infof("Engine %v has been deleted", req.Name)

	return &empty.Empty{}, nil
}

func (s *Service) deleteEngine(e *Engine) error {
	if err := s.cleanupEngineFrontend(e); err != nil {
		return err
	}

	if _, err := s.launcher.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
		Name: e.Name,
	}); err != nil {
		return err
	}

	delete(s.engines, e.Name)

	return nil
}

func (s *Service) cleanupEngineFrontend(e *Engine) error {
	tID, err := e.finishShutdownFrontend()
	if err != nil {
		return err
	}

	if err = s.availableTIDs.ReleaseRange(int32(tID), int32(tID)); err != nil {
		return err
	}

	s.engines[e.Name] = e

	return nil
}

func (s *Service) EngineGet(ctx context.Context, req *rpc.EngineRequest) (ret *rpc.EngineResponse, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	e := s.engines[req.Name]
	if e == nil {
		return nil, fmt.Errorf("cannot find engine %v", req.Name)
	}
	return e.RPCResponse(), nil
}

func (s *Service) EngineUpgrade(ctx context.Context, req *rpc.EngineUpgradeRequest) (ret *empty.Empty, err error) {
	s.lock.Lock()

	e := s.engines[req.Spec.Name]
	if e == nil {
		s.lock.Unlock()
		return nil, errors.Wrapf(err, "failed to find engine process %v", req.Spec.Name)
	}

	if e.Binary == req.Spec.Binary {
		s.lock.Unlock()
		return nil, fmt.Errorf("cannot upgrade with the same binary")
	}

	if _, err := os.Stat(req.Spec.Binary); os.IsNotExist(err) {
		return nil, errors.Wrap(err, "cannot find the binary to be upgraded")
	}

	binary := e.Binary
	if err := s.updateEngineBinary(binary, req.Spec.Binary); err != nil {
		s.lock.Unlock()
		return nil, errors.Wrap(err, "failed to update engine binary")
	}

	// TODO: live upgrade
	if err := s.deleteEngine(e); err != nil {
		s.lock.Unlock()
		return nil, errors.Wrap(err, "failed to delete old engine")
	}
	s.lock.Unlock()

	e.Binary = req.Spec.Binary
	e.Replicas = req.Spec.Replicas
	if err := s.registerEngine(e); err != nil {
		return nil, err
	}

	if err = s.startEngine(e); err != nil {
		return nil, errors.Wrapf(err, "failed to start new engine %v in EngineUpgrade", e.Name)
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	stopCh := make(chan struct{})
	socketError := e.WaitForSocket(stopCh)
	select {
	case err = <-socketError:
		if err != nil {
			logrus.Errorf("error waiting for the socket %v", err)
			err = errors.Wrapf(err, "error waiting for the socket")
		}
		break
	}
	close(stopCh)
	close(socketError)

	if err = e.ReloadSocketConnection(); err != nil {
		return nil, errors.Wrapf(err, "failed to reload socket connection for new engine %v in EngineUpgrade", e.Name)
	}

	e.RemoveBackupBinary()
	s.engines[e.Name] = e

	logrus.Infof("Engine %v has been upgraded to binary %v", e.Name, e.Binary)

	return &empty.Empty{}, nil
}

func (s *Service) updateEngineBinary(binary, newBinary string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to update engine binary %v to %v", binary, newBinary)
	}()

	if err := util.RemoveFile(binary); err != nil {
		return err
	}

	if err := util.CopyFile(newBinary, binary); err != nil {
		return err
	}

	return nil
}

func (s *Service) FrontendStart(ctx context.Context, req *rpc.FrontendStartRequest) (ret *empty.Empty, err error) {
	s.lock.Lock()
	e := s.engines[req.Name]

	if e.Frontend == req.Frontend {
		logrus.Debugf("Engine frontend %v is already up", e.Frontend)
		s.lock.Unlock()
		return &empty.Empty{}, nil
	}
	if e.Frontend != "" {
		s.lock.Unlock()
		return nil, fmt.Errorf("cannot set frontend if it's already set")
	}

	if req.Frontend != FrontendTGTBlockDev && req.Frontend != FrontendTGTISCSI {
		s.lock.Unlock()
		return nil, fmt.Errorf("Invalid frontend %v", req.Frontend)
	}

	e.Frontend = req.Frontend
	s.engines[req.Name] = e
	s.lock.Unlock()

	// the controller will call back to launcher. be careful about deadlock
	if err := e.startFrontend("socket"); err != nil {
		return nil, err
	}

	logrus.Infof("Engine frontend %v has been started", req.Frontend)

	return &empty.Empty{}, nil
}

func (s *Service) FrontendShutdown(ctx context.Context, req *rpc.FrontendShutdownRequest) (ret *empty.Empty, err error) {
	s.lock.Lock()
	e := s.engines[req.Name]

	if e.Frontend == "" || e.scsiDevice == nil {
		logrus.Debugf("Engine frontend is already down")
		s.lock.Unlock()
		return &empty.Empty{}, nil
	}
	s.lock.Unlock()

	// the controller will call back to launcher. be careful about deadlock
	if err := e.shutdownFrontend(); err != nil {
		return nil, err
	}

	s.lock.Lock()
	e.Frontend = ""
	s.lock.Unlock()

	logrus.Infof("Engine %v frontend has been shut down", e.Name)

	return &empty.Empty{}, nil
}

func (s *Service) FrontendStartCallback(ctx context.Context, req *rpc.EngineRequest) (ret *empty.Empty, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	name := req.Name
	e, exists := s.engines[name]
	if !exists {
		return nil, fmt.Errorf("engine %v not found", name)
	}

	tID, _, err := s.availableTIDs.AllocateRange(1)
	if err != nil || tID == 0 {
		return nil, fmt.Errorf("cannot get available tid for frontend starting")
	}

	if err := e.finishStartFrontend(int(tID)); err != nil {
		return nil, errors.Wrapf(err, "failed to finish FrontendStart for %v", name)
	}

	s.engines[name] = e

	logrus.Infof("Engine %v frontend start callback complete", e.Name)

	return &empty.Empty{}, nil
}

func (s *Service) FrontendShutdownCallback(ctx context.Context, req *rpc.EngineRequest) (ret *empty.Empty, err error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	name := req.Name
	e, exists := s.engines[name]
	if !exists {
		return nil, fmt.Errorf("engine %v not found", name)
	}

	if err := s.cleanupEngineFrontend(e); err != nil {
		return nil, errors.Wrapf(err, "failed to cleanup Engine frontend for %v", name)
	}

	s.engines[e.Name] = e

	logrus.Infof("Engine %v frontend shutdown callback complete", e.Name)

	return &empty.Empty{}, nil
}
