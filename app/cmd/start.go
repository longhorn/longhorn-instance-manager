package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof" // for runtime profiling
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

	spdk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"
	spdkrpc "github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/disk"
	"github.com/longhorn/longhorn-instance-manager/pkg/health"
	rpc "github.com/longhorn/longhorn-instance-manager/pkg/imrpc"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance"
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
				Value: "localhost:8500",
				Usage: "specifies the server endpoint to listen on supported protocols are 'tcp' and 'unix'. The proxy server will be listening on the next port.",
			},
			cli.StringFlag{
				Name:  "logs-dir",
				Value: "/var/log/instances",
			},
			cli.StringFlag{
				Name:  "port-range",
				Value: "10000-20000",
			},
			cli.StringFlag{
				Name:  "spdk-port-range",
				Value: "20001-30000",
			},
			cli.BoolFlag{
				Name:  "spdk-enabled",
				Usage: "enable SPDK support",
			},
		},
		Action: func(c *cli.Context) {
			if err := start(c); err != nil {
				logrus.WithError(err).Fatal("Failed to run start command")
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
			return
		}
		time.Sleep(types.WaitInterval)
	}

	logrus.Error("Failed to clean up all processes for Instance Manager graceful shutdown")
}

func start(c *cli.Context) (err error) {
	listen := c.String("listen")
	logsDir := c.String("logs-dir")
	processPortRange := c.String("port-range")
	spdkPortRange := c.String("spdk-port-range")
	spdkEnabled := c.Bool("spdk-enabled")

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

	go func() {
		debugAddress := ":6060"
		debugHandler := http.DefaultServeMux
		logrus.Infof("Debug pprof server listening on %s", debugAddress)
		if err := http.ListenAndServe(debugAddress, debugHandler); err != nil && err != http.ErrServerClosed {
			logrus.Errorf(fmt.Sprintf("ListenAndServe: %s", err))
		}
	}()

	processManagerServiceAddress, proxyServiceAddress, diskServiceAddress, instanceServiceAddress, spdkServiceAddress, err := getServiceAddresses(listen)
	if err != nil {
		return err
	}

	// Start disk server
	diskGRPCServer, diskGRPCListener, err := setupDiskGRPCServer(diskServiceAddress, spdkServiceAddress, spdkEnabled, shutdownCh)
	if err != nil {
		return err
	}
	go func() {
		if err := diskGRPCServer.Serve(diskGRPCListener); err != nil {
			logrus.WithError(err).Error("Stopping disk gRPC server")
		}
		// graceful shutdown before exit
		close(shutdownCh)
	}()
	logrus.Infof("Instance Manager disk gRPC server listening to %v", diskServiceAddress)

	// Start instance server
	instanceGRPCServer, instanceRPCListener, err := setupInstanceGRPCServer(logsDir,
		instanceServiceAddress, processManagerServiceAddress, diskServiceAddress, spdkServiceAddress, tlsConfig, spdkEnabled, shutdownCh)
	if err != nil {
		return err
	}
	go func() {
		if err := instanceGRPCServer.Serve(instanceRPCListener); err != nil {
			logrus.WithError(err).Error("Stopping instance gRPC server")
		}
		// graceful shutdown before exit
		close(shutdownCh)
	}()
	logrus.Infof("Instance Manager instance gRPC server listening to %v", instanceServiceAddress)

	// Start proxy server
	proxyGRPCServer, proxyGRPCListener, err := setupProxyGRPCServer(logsDir, proxyServiceAddress, diskServiceAddress, spdkServiceAddress, tlsConfig, shutdownCh)
	if err != nil {
		return err
	}
	go func() {
		if err := proxyGRPCServer.Serve(proxyGRPCListener); err != nil {
			logrus.WithError(err).Error("Stopping proxy gRPC server")
		}
		// graceful shutdown before exit
		close(shutdownCh)
	}()
	logrus.Infof("Instance Manager proxy gRPC server listening to %v", proxyServiceAddress)

	// Start process manager server
	pm, pmGRPCServer, pmGRPCListener, err := setupProcessManagerGRPCServer(processPortRange, logsDir, processManagerServiceAddress, shutdownCh)
	if err != nil {
		return err
	}
	go func() {
		if err := pmGRPCServer.Serve(pmGRPCListener); err != nil {
			logrus.WithError(err).Error("Stopping process manager gRPC server")
		}
		// graceful shutdown before exit
		cleanup(pm)
		close(shutdownCh)
	}()
	logrus.Infof("Instance Manager process manager gRPC server listening to %v", listen)

	// Start SPDK server
	if spdkEnabled {
		spdkGRPCServer, spdkGRPCListener, err := setupSPDKGRPCServer(spdkPortRange, spdkServiceAddress, shutdownCh)
		if err != nil {
			return err
		}
		go func() {
			if err := spdkGRPCServer.Serve(spdkGRPCListener); err != nil {
				logrus.WithError(err).Error("Stopping SPDK gRPC server")
			}
			// graceful shutdown before exit
			close(shutdownCh)
		}()
		logrus.Infof("Instance Manager SPDK gRPC server listening to %v", spdkServiceAddress)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Instance Manager received %v to exit", sig)
		pmGRPCServer.Stop()
	}()

	return <-shutdownCh
}

func getServiceAddresses(listen string) (processManagerServiceAddress, proxyServiceAddress, diskServiceAddress, instanceServiceAddress, spdkerviceAddress string, err error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "", "", "", "", "", err
	}

	intPort, err := strconv.Atoi(port)
	if err != nil {
		return "", "", "", "", "", err
	}

	return net.JoinHostPort(host, strconv.Itoa(intPort)),
		net.JoinHostPort(host, strconv.Itoa(intPort+1)),
		net.JoinHostPort(host, strconv.Itoa(intPort+2)),
		net.JoinHostPort(host, strconv.Itoa(intPort+3)),
		net.JoinHostPort(host, strconv.Itoa(intPort+4)),
		nil
}

func setupDiskGRPCServer(listen, spdkServiceAddress string, spdkEnabled bool, shutdownCh chan error) (*grpc.Server, net.Listener, error) {
	srv, err := disk.NewServer(spdkEnabled, spdkServiceAddress, shutdownCh)
	if err != nil {
		return nil, nil, err
	}
	hc := health.NewDiskHealthCheckServer(srv)

	grpcServer, rpcListener, err := util.NewServer(listen, nil,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to setup disk gRPC server")
	}

	rpc.RegisterDiskServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, hc)
	reflection.Register(grpcServer)

	return grpcServer, rpcListener, nil
}

func setupSPDKGRPCServer(portRange, listen string, shutdownCh chan error) (*grpc.Server, net.Listener, error) {
	portStart, portEnd, err := util.ParsePortRange(portRange)

	srv, err := spdk.NewServer(context.Background(), portStart, portEnd)
	if err != nil {
		return nil, nil, err
	}
	hc := health.NewSPDKHealthCheckServer(srv)

	grpcServer, grpcListener, err := util.NewServer(listen, nil,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to setup SPDK gRPC server")
	}

	spdkrpc.RegisterSPDKServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, hc)
	reflection.Register(grpcServer)

	return grpcServer, grpcListener, nil
}

func setupProxyGRPCServer(logsDir, listen, diskServiceAddress, spdkServiceAddress string, tlsConfig *tls.Config, shutdownCh chan error) (*grpc.Server, net.Listener, error) {
	// TODO: skip proxy for replica instance manager pod
	srv, err := proxy.NewProxy(logsDir, diskServiceAddress, spdkServiceAddress, shutdownCh)
	if err != nil {
		return nil, nil, err
	}
	hc := health.NewProxyHealthCheckServer(srv)

	grpcProxyServer, grpcProxyListener, err := util.NewServer(listen, tlsConfig,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to setup proxy gRPC server")
	}

	rpc.RegisterProxyEngineServiceServer(grpcProxyServer, srv)
	healthpb.RegisterHealthServer(grpcProxyServer, hc)
	reflection.Register(grpcProxyServer)

	return grpcProxyServer, grpcProxyListener, nil
}

func setupProcessManagerGRPCServer(portRange, logsDir, listen string, shutdownCh chan error) (*process.Manager, *grpc.Server, net.Listener, error) {
	srv, err := process.NewManager(portRange, logsDir, shutdownCh)
	if err != nil {
		return nil, nil, nil, err
	}
	hc := health.NewHealthCheckServer(srv)

	grpcServer, grpcListener, err := util.NewServer(listen, nil,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, nil, errors.Wrap(err, "failed to setup process manager gRPC server")
	}

	rpc.RegisterProcessManagerServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, hc)
	reflection.Register(grpcServer)

	return srv, grpcServer, grpcListener, nil
}

func setupInstanceGRPCServer(logsDir, listen, processManagerServiceAddress, diskServiceAddress, spdkServiceAddress string, tlsConfig *tls.Config, spdkEnabled bool, shutdownCh chan error) (*grpc.Server, net.Listener, error) {
	srv, err := instance.NewServer(logsDir, processManagerServiceAddress, diskServiceAddress, spdkServiceAddress, spdkEnabled, shutdownCh)
	if err != nil {
		return nil, nil, err
	}
	hc := health.NewInstanceHealthCheckServer(srv)

	grpcServer, grpcListener, err := util.NewServer(listen, tlsConfig,
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to setup instance gRPC server")
	}

	rpc.RegisterInstanceServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, hc)
	reflection.Register(grpcServer)

	return grpcServer, grpcListener, nil
}
