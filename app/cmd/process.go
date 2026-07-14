package cmd

import (
	"context"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-instance-manager/pkg/client"
	"github.com/longhorn/longhorn-instance-manager/pkg/types"
	"github.com/longhorn/longhorn-instance-manager/pkg/util"
)

func ProcessCmd() *cli.Command {
	return &cli.Command{
		Name: "process",
		Commands: []*cli.Command{
			ProcessCreateCmd(),
			ProcessDeleteCmd(),
			ProcessGetCmd(),
			ProcessListCmd(),
			ProcessReplaceCmd(),
		},
	}
}

func ProcessCreateCmd() *cli.Command {
	return &cli.Command{
		Name: "create",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "name",
			},
			&cli.StringFlag{
				Name: "binary",
			},
			&cli.IntFlag{
				Name: "port-count",
			},
			&cli.StringSliceFlag{
				Name:  "port-args",
				Usage: "Automatically add additional arguments when starting the process. In case of space, use `,` instead.",
			},
		},
		Action: func(_ context.Context, c *cli.Command) error {
			err := createProcess(c)
			if err != nil {
				logrus.WithError(err).Fatal("Error running process create command")
			}
			return err
		},
	}
}

func createProcess(c *cli.Command) error {
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
		c.Int("port-count"), c.Args().Slice(), c.StringSlice("port-args"))
	if err != nil {
		return errors.Wrap(err, "failed to create process")
	}
	return util.PrintJSON(process)
}

func ProcessDeleteCmd() *cli.Command {
	return &cli.Command{
		Name: "delete",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "name",
			},
			&cli.StringFlag{
				Name:     "uuid",
				Required: false,
				Usage:    "Validate the process UUID. If provided, the process will be deleted only when both name and UUID are matched.",
			},
		},
		Action: func(_ context.Context, c *cli.Command) error {
			err := deleteProcess(c)
			if err != nil {
				logrus.WithError(err).Fatal("Error running process delete command")
			}
			return err
		},
	}
}

func deleteProcess(c *cli.Command) error {
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

func ProcessGetCmd() *cli.Command {
	return &cli.Command{
		Name: "get",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "name",
			},
		},
		Action: func(_ context.Context, c *cli.Command) error {
			err := getProcess(c)
			if err != nil {
				logrus.WithError(err).Fatal("Error running process get command")
			}
			return err
		},
	}
}

func getProcess(c *cli.Command) error {
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

func ProcessListCmd() *cli.Command {
	return &cli.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Action: func(_ context.Context, c *cli.Command) error {
			err := listProcess(c)
			if err != nil {
				logrus.WithError(err).Fatal("Error running engine stop command")
			}
			return err
		},
	}
}

func listProcess(c *cli.Command) error {
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

func ProcessReplaceCmd() *cli.Command {
	return &cli.Command{
		Name: "replace",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name: "name",
			},
			&cli.StringFlag{
				Name: "binary",
			},
			&cli.IntFlag{
				Name: "port-count",
			},
			&cli.StringSliceFlag{
				Name:  "port-args",
				Usage: "Automatically add additional arguments when starting the process. In case of space, use `,` instead.",
			},
			&cli.StringFlag{
				Name:  "terminate-signal",
				Usage: "The signal used to terminate the old process",
				Value: "SIGHUP",
			},
		},
		Action: func(_ context.Context, c *cli.Command) error {
			err := replaceProcess(c)
			if err != nil {
				logrus.WithError(err).Fatal("Error running engine replace command")
			}
			return err
		},
	}
}

func replaceProcess(c *cli.Command) error {
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
		c.Int("port-count"), c.Args().Slice(), c.StringSlice("port-args"), c.String("terminate-signal"))
	if err != nil {
		return errors.Wrap(err, "failed to replace processes")
	}
	return util.PrintJSON(process)
}

func getProcessManagerClient(c *cli.Command, ctx context.Context, ctxCancel context.CancelFunc) (*client.ProcessManagerClient, error) {
	url := c.String("url")
	tlsDir := c.String("tls-dir")

	if tlsDir != "" {
		imClient, err := client.NewProcessManagerClientWithTLS(ctx, ctxCancel, url,
			filepath.Join(tlsDir, types.TLSCAFile),
			filepath.Join(tlsDir, types.TLSCertFile),
			filepath.Join(tlsDir, types.TLSKeyFile),
			types.TLSPeerName)
		if err == nil {
			return imClient, err
		}
		logrus.WithError(err).Info("Falling back to non tls ProcessManager client")
	}

	return client.NewProcessManagerClient(ctx, ctxCancel, url, nil)
}
