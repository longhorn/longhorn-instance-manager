package cmd

import (
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
				logrus.Fatalf("Error running process create command: %v.", err)
			}
		},
	}
}

func createProcess(c *cli.Context) error {
	cli, err := getProcessManagerClient(c)
	if err != nil {
		return errors.Wrap(err, "failed to initialize client")
	}
	defer cli.Close()

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
		},
		Action: func(c *cli.Context) {
			if err := deleteProcess(c); err != nil {
				logrus.Fatalf("Error running process delete command: %v.", err)
			}
		},
	}
}

func deleteProcess(c *cli.Context) error {
	cli, err := getProcessManagerClient(c)
	if err != nil {
		return errors.Wrap(err, "failed to initialize client")
	}
	defer cli.Close()

	process, err := cli.ProcessDelete(c.String("name"))
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
				logrus.Fatalf("Error running process get command: %v.", err)
			}
		},
	}
}

func getProcess(c *cli.Context) error {
	cli, err := getProcessManagerClient(c)
	if err != nil {
		return errors.Wrap(err, "failed to initialize client")
	}
	defer cli.Close()

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
				logrus.Fatalf("Error running engine stop command: %v.", err)
			}
		},
	}
}

func listProcess(c *cli.Context) error {
	cli, err := getProcessManagerClient(c)
	if err != nil {
		return errors.Wrap(err, "failed to initialize client")
	}
	defer cli.Close()

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
				logrus.Fatalf("Error running engine replace command: %v.", err)
			}
		},
	}
}

func replaceProcess(c *cli.Context) error {
	cli, err := getProcessManagerClient(c)
	if err != nil {
		return errors.Wrap(err, "failed to initialize client")
	}
	defer cli.Close()

	process, err := cli.ProcessReplace(c.String("name"), c.String("binary"),
		c.Int("port-count"), c.Args(), c.StringSlice("port-args"), c.String("terminate-signal"))
	if err != nil {
		return errors.Wrap(err, "failed to replace processes")
	}
	return util.PrintJSON(process)
}

func getProcessManagerClient(c *cli.Context) (*client.ProcessManagerClient, error) {
	url := c.GlobalString("url")
	tlsDir := c.GlobalString("tls-dir")

	if tlsDir != "" {
		imClient, err := client.NewProcessManagerClientWithTLS(url,
			filepath.Join(tlsDir, "ca.crt"),
			filepath.Join(tlsDir, "tls.crt"),
			filepath.Join(tlsDir, "tls.key"),
			"longhorn-backend.longhorn-system")
		if err == nil {
			return imClient, err
		}
		logrus.WithError(err).Info("Falling back to non tls client")
	}

	return client.NewProcessManagerClient(url, nil)
}
