package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-instance-manager/app/cmd"
	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
)

// following variables will be filled by `-ldflags "-X ..."`
var (
	Version   string
	GitCommit string
	BuildDate string
)

func main() {
	a := &cli.Command{}

	a.Version = Version
	meta.Version = Version
	meta.GitCommit = GitCommit
	meta.BuildDate = BuildDate

	logrus.SetReportCaller(true)
	logrus.SetFormatter(&logrus.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (function string, file string) {
			fileName := fmt.Sprintf("%s:%d", path.Base(f.File), f.Line)
			funcName := path.Base(f.Function)
			return funcName, fileName
		},
		TimestampFormat: time.RFC3339Nano,
		FullTimestamp:   true,
	})

	a.Before = func(ctx context.Context, c *cli.Command) (context.Context, error) {
		if c.Bool("debug") {
			logrus.SetLevel(logrus.DebugLevel)
		}
		return ctx, nil
	}
	a.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:  "url",
			Local: true,
			Value: "tcp://localhost:8500",
			Usage: "specifies the server endpoint to connect to supported protocols are 'tcp' and 'unix'",
		},
		&cli.BoolFlag{
			Name:  "debug",
			Local: true,
		},
		&cli.StringFlag{
			Name:     "tls-dir",
			Usage:    "when present will look for `tls.crt` and `tls.key` and `ca.crt` file in the specified directory",
			Sources:  cli.EnvVars("TLS_DIR"),
			Local:    true,
			Required: false,
		},
	}
	a.Commands = []*cli.Command{
		cmd.StartCmd(),
		cmd.ProcessCmd(),
		cmd.VersionCmd(),
	}
	if err := a.Run(context.Background(), os.Args); err != nil {
		logrus.WithError(err).Fatal("Error when executing command")
	}
}
