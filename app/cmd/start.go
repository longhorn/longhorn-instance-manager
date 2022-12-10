package cmd

import (
	"crypto/tls"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/longhorn/longhorn-instance-manager/pkg/health"
	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/process"
	"github.com/longhorn/longhorn-instance-manager/pkg/proxy"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

func StartCmd() cli.Command {
	return cli.Command{
		Name: "daemon",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "listen",
				Value: "tcp://localhost:8500",
				Usage: "specifies the server endpoint to listen on supported protocols are 'tcp' and 'unix'. The proxy server will be listening on the next port.",
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
				logrus.Fatalf("Failed to run start command: %v.", err)
			}
		},
	}
}

func cleanup(pm *process.Manager) {
	logrus.Info("Trying to gracefully shut down Instance Manager")
	pmResp, err := pm.ProcessList(nil, &rpc.ProcessListRequest{})
	if err != nil {
		logrus.WithError(err).Error("Failed to list processes before shutdown")
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
			logrus.WithError(err).Error("Failed to list instance processes when shutting down")
			break
		}
		if len(pmResp.Processes) == 0 {
			logrus.Info("Shutdown all instance processes successfully")
			break
		}
		time.Sleep(types.WaitInterval)
	}

	logrus.Error("Failed to clean up all processes for Instance Manager graceful shutdown")
}

func start(c *cli.Context) (err error) {
	listen := c.String("listen")
	logsDir := c.String("logs-dir")
	portRange := c.String("port-range")

	if err := util.SetUpLogger(logsDir); err != nil {
		return err
	}

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
			logrus.WithError(err).Warnf("Failed to add TLS key pair from %v", tlsDir)
		}
	}

	if tlsConfig != nil {
		logrus.Info("Creating gRPC server with mtls auth")
	} else {
		logrus.Info("Creating gRPC server with no auth")
	}

	shutdownCh := make(chan error)

	// Start proxy server
	proxyAddress, err := getProxyAddress(listen)
	if err != nil {
		return err
	}

	// TODO: skip proxy for replica instance manager pod
	proxy, err := proxy.NewProxy(logsDir, shutdownCh)
	if err != nil {
		return err
	}
	hcProxy := health.NewProxyHealthCheckServer(proxy)

	rpcProxyService, proxyServer, err := util.NewServer(proxyAddress, tlsConfig,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup proxy gRPCserver")
	}

	rpc.RegisterProxyEngineServiceServer(rpcProxyService, proxy)
	healthpb.RegisterHealthServer(rpcProxyService, hcProxy)
	reflection.Register(rpcProxyService)

	go func() {
		if err := rpcProxyService.Serve(proxyServer); err != nil {
			logrus.Errorf("Stopping proxy gRPCserver due to %v", err)
		}
		// graceful shutdown before exit
		close(shutdownCh)
	}()
	logrus.Infof("Instance Manager proxy gRPC server listening to %v", proxyAddress)

	// Start process server
	pm, err := process.NewManager(portRange, logsDir, shutdownCh)
	if err != nil {
		return err
	}
	hc := health.NewHealthCheckServer(pm)

	rpcService, listenAt, err := util.NewServer(listen, tlsConfig,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return errors.Wrap(err, "failed to setup process gRPC server")
	}

	rpc.RegisterProcessManagerServiceServer(rpcService, pm)
	healthpb.RegisterHealthServer(rpcService, hc)
	reflection.Register(rpcService)

	go func() {
		if err := rpcService.Serve(listenAt); err != nil {
			logrus.Errorf("Stopping process gRPC server due to %v", err)
		}
		// graceful shutdown before exit
		cleanup(pm)
		close(shutdownCh)
	}()
	logrus.Infof("Instance Manager process gRPC server listening to %v", listen)

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Instance Manager received %v to exit", sig)
		rpcService.Stop()
	}()

	return <-shutdownCh
}

func getProxyAddress(listen string) (string, error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", err
	}

	intPort, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}

	return net.JoinHostPort(host, strconv.Itoa(intPort+1)), nil
}
