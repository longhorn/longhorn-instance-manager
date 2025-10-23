package cmd

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	_ "net/http/pprof" // for runtime profiling
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"k8s.io/mount-utils"

	engineutil "github.com/longhorn/longhorn-engine/pkg/util"
	spdk "github.com/longhorn/longhorn-spdk-engine/pkg/spdk"
	spdkutil "github.com/longhorn/longhorn-spdk-engine/pkg/util"
	rpc "github.com/longhorn/types/pkg/generated/imrpc"
	spdkrpc "github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-instance-manager/pkg/disk"
	"github.com/longhorn/longhorn-instance-manager/pkg/health"
	"github.com/longhorn/longhorn-instance-manager/pkg/instance"
	"github.com/longhorn/longhorn-instance-manager/pkg/process"
	"github.com/longhorn/longhorn-instance-manager/pkg/proxy"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

const (
	spdkTgtStopTimeout = 120 * time.Second
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
	logrus.Infof("Trying to gracefully shut down %v", types.ProcessManagerGrpcService)

	pmResp, err := pm.ProcessList(context.TODO(), &rpc.ProcessListRequest{})
	if err != nil {
		logrus.WithError(err).Errorf("Failed to list processes before shutting down %v", types.ProcessManagerGrpcService)
		return
	}
	for _, p := range pmResp.Processes {
		if _, err := pm.ProcessDelete(context.TODO(), &rpc.ProcessDeleteRequest{
			Name: p.Spec.Name,
		}); err != nil {
			logrus.WithError(err).Errorf("Failed to delete process %s", p.Spec.Name)
		}
	}

	for i := 0; i < types.WaitCount; i++ {
		pmResp, err := pm.ProcessList(context.TODO(), &rpc.ProcessListRequest{})
		if err != nil {
			logrus.WithError(err).Errorf("Failed to list processes when shutting down %v", types.ProcessManagerGrpcService)
			break
		}
		if len(pmResp.Processes) == 0 {
			logrus.Info("Shut down all processes successfully")
			return
		}
		time.Sleep(types.WaitInterval)
	}

	logrus.Errorf("Failed to clean up all processes for %s graceful shutdown", types.ProcessManagerGrpcService)
}

func unfreezeFilesystems() error {
	// We do not need to switch to the host mount namespace to get mount points here. Usually, longhorn-engine runs in a
	// container that has / bind mounted to /host with at least HostToContainer (rslave) propagation.
	// - If it does not, we likely can't do a namespace swap anyway, since we don't have access to /host/proc.
	// - If it does, we just need to know where in the container we can access the mount points to unfreeze the file
	//   system.
	mounter := mount.New("")
	mountPoints, err := mounter.List()
	if err != nil {
		return errors.Wrap(err, "failed to list mount points while starting up")
	}

	for _, mountPoint := range mountPoints {
		if strings.Contains(mountPoint.Device, engineutil.DevicePathPrefix) {
			// We do not actually expect any filesystems to be frozen. This is a best effort attempt to unfreeze them
			// if somehow instance manager crashed at the wrong moment during a snapshot.
			unfroze, err := engineutil.UnfreezeFilesystem(mountPoint.Path, nil)
			if err != nil {
				logrus.WithError(err).Warnf("Failed to unfreeze filesystem mounted at %v", mountPoint)
			}
			if unfroze {
				logrus.Warnf("Unfroze filesystem mounted at %v", mountPoint)
			}
		}
	}
	return nil
}

func start(c *cli.Context) (err error) {
	listen := c.String("listen")
	logsDir := c.String("logs-dir")
	processPortRange := c.String("port-range")
	spdkPortRange := c.String("spdk-port-range")
	spdkEnabled := c.Bool("spdk-enabled")

	defer func() {
		if spdkEnabled {
			logrus.Infof("Stopping spdk_tgt daemon")
			if err := spdkutil.StopSPDKTgtDaemon(spdkTgtStopTimeout); err != nil {
				logrus.WithError(err).Error("Failed to stop spdk_tgt daemon")
			}
		}
	}()

	if err := util.SetUpLogger(logsDir); err != nil {
		return err
	}

	if !spdkEnabled {
		if err := unfreezeFilesystems(); err != nil {
			return err
		}
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

	go func() {
		debugAddress := ":6060"
		debugHandler := http.DefaultServeMux
		logrus.Infof("Debug pprof server listening on %s", debugAddress)
		if err := http.ListenAndServe(debugAddress, debugHandler); err != nil && err != http.ErrServerClosed {
			logrus.Errorf("ListenAndServe: %s", err)
		}
	}()

	addresses, err := getServiceAddresses(listen)
	if err != nil {
		logrus.WithError(err).Error("Failed to get service addresses")
		return err
	}

	// Create gRPC servers
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	servers := map[string]*grpc.Server{}
	listeners := map[string]net.Listener{}

	// Start disk server
	diskGRPCServer, diskGRPCListener, err := setupDiskGRPCServer(ctx, addresses[types.DiskGrpcService], addresses[types.SpdkGrpcService], spdkEnabled)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to setup %s", types.DiskGrpcService)
		return err
	}
	servers[types.DiskGrpcService] = diskGRPCServer
	listeners[types.DiskGrpcService] = diskGRPCListener

	// Start instance server
	instanceGRPCServer, instanceRPCListener, err := setupInstanceGRPCServer(ctx, logsDir,
		addresses[types.InstanceGrpcService], addresses[types.ProcessManagerGrpcService],
		addresses[types.SpdkGrpcService], tlsConfig, spdkEnabled)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to set up %s", types.InstanceGrpcService)
		return err
	}
	servers[types.InstanceGrpcService] = instanceGRPCServer
	listeners[types.InstanceGrpcService] = instanceRPCListener

	// Start proxy server
	proxyGRPCServer, proxyGRPCListener, err := setupProxyGRPCServer(ctx, logsDir,
		addresses[types.ProxyGRPCService], addresses[types.DiskGrpcService], addresses[types.SpdkGrpcService], tlsConfig)
	if err != nil {
		logrus.WithError(err).Errorf("Failed to set up %s", types.ProxyGRPCService)
		return err
	}
	servers[types.ProxyGRPCService] = proxyGRPCServer
	listeners[types.ProxyGRPCService] = proxyGRPCListener

	// Start process-manager server
	pm, pmGRPCServer, pmGRPCListener, err := setupProcessManagerGRPCServer(ctx, processPortRange, logsDir, addresses[types.ProcessManagerGrpcService])
	if err != nil {
		logrus.WithError(err).Errorf("Failed to set up %s", types.ProcessManagerGrpcService)
		return err
	}
	servers[types.ProcessManagerGrpcService] = pmGRPCServer
	listeners[types.ProcessManagerGrpcService] = pmGRPCListener

	// Start spdk server
	if spdkEnabled {
		spdkGRPCServer, spdkGRPCListener, err := setupSPDKGRPCServer(ctx, spdkPortRange, addresses[types.SpdkGrpcService])
		if err != nil {
			logrus.WithError(err).Errorf("Failed to set up %s", types.SpdkGrpcService)
			return err
		}
		servers[types.SpdkGrpcService] = spdkGRPCServer
		listeners[types.SpdkGrpcService] = spdkGRPCListener
	}

	g, _ := errgroup.WithContext(ctx)

	// Register signal handler
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	g.Go(func() error {
		sig := <-sigs
		logrus.Infof("Instance Manager received %v to exit", sig)

		for _, server := range servers {
			server.Stop()
		}
		return nil
	})

	// Start gRPC servers
	for name, server := range servers {
		name, server := name, server
		g.Go(func() error {
			defer func() {
				// Send SIGTERM to stop other grpc servers
				select {
				case sigs <- syscall.SIGTERM:
					logrus.Infof("Instance Manager sent %v to exit", syscall.SIGTERM)
				default:
					logrus.Infof("Instance Manager already sent %v to exit", syscall.SIGTERM)
				}
			}()

			listener := listeners[name]
			address := addresses[name]

			logrus.Infof("%s listening to %v", name, address)
			err := server.Serve(listener)
			if err != nil {
				logrus.WithError(err).Errorf("%s failed to serve", name)
			}

			if name == types.ProcessManagerGrpcService {
				cleanup(pm)
			}

			logrus.Infof("Stopped %s", name)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		logrus.WithError(err).Error("Instance Manager exited with error")
	}

	return nil
}

func getServiceAddresses(listen string) (addresses map[string]string, err error) {
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return nil, err
	}

	intPort, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		types.ProcessManagerGrpcService: net.JoinHostPort(host, strconv.Itoa(intPort)),
		types.ProxyGRPCService:          net.JoinHostPort(host, strconv.Itoa(intPort+1)),
		types.DiskGrpcService:           net.JoinHostPort(host, strconv.Itoa(intPort+2)),
		types.InstanceGrpcService:       net.JoinHostPort(host, strconv.Itoa(intPort+3)),
		types.SpdkGrpcService:           net.JoinHostPort(host, strconv.Itoa(intPort+4)),
	}, nil
}

func setupDiskGRPCServer(ctx context.Context, listen, spdkServiceAddress string, spdkEnabled bool) (*grpc.Server, net.Listener, error) {
	srv, err := disk.NewServer(ctx, spdkEnabled, spdkServiceAddress)
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
		return nil, nil, errors.Wrapf(err, "failed to setup %s", types.DiskGrpcService)
	}

	rpc.RegisterDiskServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, hc)
	reflection.Register(grpcServer)

	return grpcServer, rpcListener, nil
}

func setupSPDKGRPCServer(ctx context.Context, portRange, listen string) (*grpc.Server, net.Listener, error) {
	portStart, portEnd, err := util.ParsePortRange(portRange)
	if err != nil {
		return nil, nil, err
	}

	srv, err := spdk.NewServer(ctx, portStart, portEnd)
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
		return nil, nil, errors.Wrapf(err, "failed to setup %s", types.SpdkGrpcService)
	}

	spdkrpc.RegisterSPDKServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, hc)
	reflection.Register(grpcServer)

	return grpcServer, grpcListener, nil
}

func setupProxyGRPCServer(ctx context.Context, logsDir, listen, diskServiceAddress, spdkServiceAddress string, tlsConfig *tls.Config) (*grpc.Server, net.Listener, error) {
	// TODO: skip proxy for replica instance manager pod
	srv, err := proxy.NewProxy(ctx, logsDir, diskServiceAddress, spdkServiceAddress)
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
		return nil, nil, errors.Wrapf(err, "failed to setup %s", types.ProxyGRPCService)
	}

	rpc.RegisterProxyEngineServiceServer(grpcProxyServer, srv)
	healthpb.RegisterHealthServer(grpcProxyServer, hc)
	reflection.Register(grpcProxyServer)

	return grpcProxyServer, grpcProxyListener, nil
}

func setupProcessManagerGRPCServer(ctx context.Context, portRange, logsDir, listen string) (*process.Manager, *grpc.Server, net.Listener, error) {
	srv, err := process.NewManager(ctx, portRange, logsDir)
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
		return nil, nil, nil, errors.Wrapf(err, "failed to setup %s", types.ProcessManagerGrpcService)
	}

	rpc.RegisterProcessManagerServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, hc)
	reflection.Register(grpcServer)

	return srv, grpcServer, grpcListener, nil
}

func setupInstanceGRPCServer(ctx context.Context, logsDir, listen, processManagerServiceAddress, spdkServiceAddress string, tlsConfig *tls.Config, spdkEnabled bool) (*grpc.Server, net.Listener, error) {
	srv, err := instance.NewServer(ctx, logsDir, processManagerServiceAddress, spdkServiceAddress, spdkEnabled)
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
		return nil, nil, errors.Wrapf(err, "failed to setup %s", types.InstanceGrpcService)
	}

	rpc.RegisterInstanceServiceServer(grpcServer, srv)
	healthpb.RegisterHealthServer(grpcServer, hc)
	reflection.Register(grpcServer)

	return grpcServer, grpcListener, nil
}
