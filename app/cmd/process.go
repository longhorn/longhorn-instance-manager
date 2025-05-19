package cmd

import (
	"context"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-instance-manager/pkg/client"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

func ProcessCmd() cli.Command {
	return cli.Command{
		Name: "process",
		Subcommands: []cli.Command{
			ProcessCreateCmd(),
			ProcessDeleteCmd(),
			ProcessGetCmd(),
			ProcessListCmd(),
			ProcessReplaceCmd(),
		},
	}
}

func ProcessCreateCmd() cli.Command {
	return cli.Command{
		Name: "create",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "binary",
			},
			cli.IntFlag{
				Name: "port-count",
			},
			cli.StringSliceFlag{
				Name:  "port-args",
				Usage: "Automatically add additional arguments when starting the process. In case of space, use `,` instead.",
			},
		},
		Action: func(c *cli.Context) {
			if err := createProcess(c); err != nil {
				logrus.WithError(err).Fatal("Error running process create command")
			}
		},
	}
}

func createProcess(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := getProcessManagerClient(c, ctx, cancel)
	if err != nil {
		return errors.Wrap(err, "failed to initialize ProcessManager client")
	}
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close ProcessManager client")
		}
	}()

	process, err := cli.ProcessCreate(c.String("name"), c.String("binary"),
		c.Int("port-count"), c.Args(), c.StringSlice("port-args"))
	if err != nil {
		return errors.Wrap(err, "failed to create process")
	}
	return util.PrintJSON(process)
}

func ProcessDeleteCmd() cli.Command {
	return cli.Command{
		Name: "delete",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name:     "uuid",
				Required: false,
				Usage:    "Validate the process UUID. If provided, the process will be deleted only when both name and UUID are matched.",
			},
		},
		Action: func(c *cli.Context) {
			if err := deleteProcess(c); err != nil {
				logrus.WithError(err).Fatal("Error running process delete command")
			}
		},
	}
}

func deleteProcess(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := getProcessManagerClient(c, ctx, cancel)
	if err != nil {
		return errors.Wrap(err, "failed to initialize ProcessManager client")
	}
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close ProcessManager client")
		}
	}()

	process, err := cli.ProcessDelete(c.String("name"), c.String("uuid"))
	if err != nil {
		return errors.Wrap(err, "failed to delete process")
	}
	return util.PrintJSON(process)
}

func ProcessGetCmd() cli.Command {
	return cli.Command{
		Name: "get",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
		},
		Action: func(c *cli.Context) {
			if err := getProcess(c); err != nil {
				logrus.WithError(err).Fatal("Error running process get command")
			}
		},
	}
}

func getProcess(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli, err := getProcessManagerClient(c, ctx, cancel)
	if err != nil {
		return errors.Wrap(err, "failed to initialize ProcessManager client")
	}
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close ProcessManager client")
		}
	}()

	process, err := cli.ProcessGet(c.String("name"))
	if err != nil {
		return errors.Wrap(err, "failed to delete process")
	}
	return util.PrintJSON(process)
}

func ProcessListCmd() cli.Command {
	return cli.Command{
		Name:      "list",
		ShortName: "ls",
		Action: func(c *cli.Context) {
			if err := listProcess(c); err != nil {
				logrus.WithError(err).Fatal("Error running engine stop command")
			}
		},
	}
}

func listProcess(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli, err := getProcessManagerClient(c, ctx, cancel)
	if err != nil {
		return errors.Wrap(err, "failed to initialize ProcessManager client")
	}
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close ProcessManager client")
		}
	}()

	processes, err := cli.ProcessList()
	if err != nil {
		return errors.Wrap(err, "failed to list processes")
	}
	return util.PrintJSON(processes)
}

func ProcessReplaceCmd() cli.Command {
	return cli.Command{
		Name: "replace",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "binary",
			},
			cli.IntFlag{
				Name: "port-count",
			},
			cli.StringSliceFlag{
				Name:  "port-args",
				Usage: "Automatically add additional arguments when starting the process. In case of space, use `,` instead.",
			},
			cli.StringFlag{
				Name:  "terminate-signal",
				Usage: "The signal used to terminate the old process",
				Value: "SIGHUP",
			},
		},
		Action: func(c *cli.Context) {
			if err := replaceProcess(c); err != nil {
				logrus.WithError(err).Fatal("Error running engine replace command")
			}
		},
	}
}

func replaceProcess(c *cli.Context) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cli, err := getProcessManagerClient(c, ctx, cancel)
	if err != nil {
		return errors.Wrap(err, "failed to initialize ProcessManager client")
	}
	defer func() {
		if closeErr := cli.Close(); closeErr != nil {
			logrus.WithError(closeErr).Warn("Failed to close ProcessManager client")
		}
	}()

	process, err := cli.ProcessReplace(c.String("name"), c.String("binary"),
		c.Int("port-count"), c.Args(), c.StringSlice("port-args"), c.String("terminate-signal"))
	if err != nil {
		return errors.Wrap(err, "failed to replace processes")
	}
	return util.PrintJSON(process)
}

func getProcessManagerClient(c *cli.Context, ctx context.Context, ctxCancel context.CancelFunc) (*client.ProcessManagerClient, error) {
	url := c.GlobalString("url")
	tlsDir := c.GlobalString("tls-dir")

	if tlsDir != "" {
		imClient, err := client.NewProcessManagerClientWithTLS(ctx, ctxCancel, url,
			filepath.Join(tlsDir, "ca.crt"),
			filepath.Join(tlsDir, "tls.crt"),
			filepath.Join(tlsDir, "tls.key"),
			"longhorn-backend.longhorn-system")
		if err == nil {
			return imClient, err
		}
		logrus.WithError(err).Info("Falling back to non tls ProcessManager client")
	}

	return client.NewProcessManagerClient(ctx, ctxCancel, url, nil)
}
