package cmd

import (
	"crypto/tls"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-instance-manager/pkg/health"
	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/process"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

func StartCmd() cli.Command {
	return cli.Command{
		Name: "daemon",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "localhost:8500",
			},
			cli.StringFlag{
				Name:  "logs-dir",
				Value: "/var/log/instances",
			},
			cli.StringFlag{
				Name:  "port-range",
				Value: "10000-30000",
			},
		},
		Action: func(c *cli.Context) {
			if err := start(c); err != nil {
				logrus.Fatalf("Error running start command: %v.", err)
			}
		},
	}
}

func cleanup(pm *process.Manager) {
	logrus.Infof("Try to gracefully shut down Instance Manager")
	pmResp, err := pm.ProcessList(nil, &rpc.ProcessListRequest{})
	if err != nil {
		logrus.Errorf("Failed to list processes before shutdown")
		return
	}
	for _, p := range pmResp.Processes {
		pm.ProcessDelete(nil, &rpc.ProcessDeleteRequest{
			Name: p.Spec.Name,
		})
	}

	for i := 0; i < types.WaitCount; i++ {
		pmResp, err := pm.ProcessList(nil, &rpc.ProcessListRequest{})
		if err != nil {
			logrus.Errorf("Failed to list instance processes when shutting down")
			break
		}
		if len(pmResp.Processes) == 0 {
			logrus.Infof("Instance Manager has shutdown all processes.")
			break
		}
		time.Sleep(types.WaitInterval)
	}

	logrus.Errorf("Failed to cleanup all processes for Instance Manager graceful shutdown")
}

func start(c *cli.Context) error {
	listen := c.String("listen")
	logsDir := c.String("logs-dir")
	portRange := c.String("port-range")

	if err := util.SetUpLogger(logsDir); err != nil {
		return err
	}

	shutdownCh := make(chan error)
	pm, err := process.NewManager(portRange, logsDir, shutdownCh)
	if err != nil {
		return err
	}
	hc := health.NewHealthCheckServer(pm)

	// setup tls config
	var tlsConfig *tls.Config
	tlsDir := c.GlobalString("tls-dir")
	if tlsDir != "" {
		tlsConfig, err = util.LoadServerTLS(
			filepath.Join(tlsDir, "ca.crt"),
			filepath.Join(tlsDir, "tls.crt"),
			filepath.Join(tlsDir, "tls.key"),
			"longhorn-backend.longhorn-system")
		if err != nil {
			return errors.Wrap(err, "failed to load tls key pair from file")
		}
	}

	if tlsConfig != nil {
		logrus.Info("creating grpc server with mtls auth")
	} else {
		logrus.Info("creating grpc server with no auth")
	}

	rpcService, listenAt, err := util.NewServer("tcp://"+listen, tlsConfig)
	if err != nil {
		return errors.Wrap(err, "failed to setup grpc server")
	}

	rpc.RegisterProcessManagerServiceServer(rpcService, pm)
	healthpb.RegisterHealthServer(rpcService, hc)
	reflection.Register(rpcService)

	go func() {
		if err := rpcService.Serve(listenAt); err != nil {
			logrus.Errorf("Stopping due to %v:", err)
		}
		// graceful shutdown before exit
		cleanup(pm)
		close(shutdownCh)
	}()
	logrus.Infof("Instance Manager listening to %v", listen)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Instance Manager received %v to exit", sig)
		rpcService.Stop()
	}()

	return <-shutdownCh
}
