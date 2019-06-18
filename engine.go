package main

import (
	"fmt"

	"github.com/docker/go-units"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-engine-launcher/client"
	"github.com/longhorn/longhorn-engine-launcher/util"
)

func EngineCmd() cli.Command {
	return cli.Command{
		Name: "engine",
		Subcommands: []cli.Command{
			EngineCreateCmd(),
			EngineGetCmd(),
			EngineUpgradeCmd(),
			EngineDeleteCmd(),
			FrontendStartCmd(),
			FrontendShutdownCmd(),
			FrontendStartCallbackCmd(),
			FrontendShutdownCallbackCmd(),
		},
	}
}

func EngineCreateCmd() cli.Command {
	return cli.Command{
		Name: "create",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "volume-name",
			},
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "binary",
			},
			cli.StringFlag{
				Name: "size",
			},
			cli.StringFlag{
				Name: "listen",
			},
			cli.StringFlag{
				Name: "listen-addr",
			},
			cli.StringFlag{
				Name:  "frontend",
				Usage: "Supports tgt-blockdev or tgt-iscsi, or leave it empty to disable frontend",
			},
			cli.StringSliceFlag{
				Name:  "enable-backend",
				Value: (*cli.StringSlice)(&[]string{"tcp"}),
			},
			cli.StringSliceFlag{
				Name: "replica",
			},
		},
		Action: func(c *cli.Context) {
			if err := createEngine(c); err != nil {
				logrus.Fatalf("Error running engine create command: %v.", err)
			}
		},
	}
}

func createEngine(c *cli.Context) error {
	name := c.String("name")
	volumeName := c.String("volume-name")
	binary := c.String("binary")
	backends := c.StringSlice("enable-backend")
	replicas := c.StringSlice("replica")
	frontend := c.String("frontend")
	listen := c.String("listen")
	listenAddr := c.String("listen-addr")

	sizeString := c.String("size")
	if sizeString == "" {
		return fmt.Errorf("Invalid empty size")
	}
	size, err := units.RAMInBytes(sizeString)
	if err != nil {
		return err
	}

	url := c.GlobalString("url")
	cli := client.NewEngineManagerClient(url)
	engine, err := cli.EngineCreate(int64(size), name, volumeName, binary, listen, listenAddr, frontend, backends, replicas)
	if err != nil {
		return err
	}
	return util.PrintJSON(engine)
}

func EngineGetCmd() cli.Command {
	return cli.Command{
		Name: "get",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := getEngine(c); err != nil {
				logrus.Fatalf("Error running engine get command: %v.", err)
			}
		},
	}
}

func getEngine(c *cli.Context) error {
	id := c.String("id")

	url := c.GlobalString("url")
	cli := client.NewEngineManagerClient(url)
	engine, err := cli.EngineGet(id)
	if err != nil {
		return err
	}
	return util.PrintJSON(engine)
}

func EngineUpgradeCmd() cli.Command {
	return cli.Command{
		Name: "upgrade",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "name",
			},
			cli.StringFlag{
				Name: "binary",
			},
			cli.StringFlag{
				Name: "size",
			},
			cli.StringSliceFlag{
				Name: "replica",
			},
		},
		Action: func(c *cli.Context) {
			if err := upgradeEngine(c); err != nil {
				logrus.Fatalf("Error running engine upgrade command: %v.", err)
			}
		},
	}
}

func upgradeEngine(c *cli.Context) error {
	name := c.String("name")
	binary := c.String("binary")
	replicas := c.StringSlice("replica")

	sizeString := c.String("size")
	if sizeString == "" {
		return fmt.Errorf("Invalid empty size")
	}
	size, err := units.RAMInBytes(sizeString)
	if err != nil {
		return err
	}

	url := c.GlobalString("url")
	cli := client.NewEngineManagerClient(url)
	if err := cli.EngineUpgrade(size, name, binary, replicas); err != nil {
		return err
	}

	return nil
}

func EngineDeleteCmd() cli.Command {
	return cli.Command{
		Name: "delete",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := deleteEngine(c); err != nil {
				logrus.Fatalf("Error running engine delete command: %v.", err)
			}
		},
	}
}

func deleteEngine(c *cli.Context) error {
	id := c.String("id")

	url := c.GlobalString("url")
	cli := client.NewEngineManagerClient(url)
	if err := cli.EngineDelete(id); err != nil {
		return err
	}
	return nil
}

func FrontendStartCmd() cli.Command {
	name := "frontend-start"
	return cli.Command{
		Name: name,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
			cli.StringFlag{
				Name: "frontend",
			},
		},
		Action: func(c *cli.Context) {
			if err := startFrontend(c); err != nil {
				logrus.Fatalf("Error running %v command: %v.", name, err)
			}
		},
	}
}

func startFrontend(c *cli.Context) error {
	id := c.String("id")
	frontend := c.String("frontend")

	url := c.GlobalString("url")
	cli := client.NewEngineManagerClient(url)
	if err := cli.FrontendStart(id, frontend); err != nil {
		return err
	}
	return nil
}

func FrontendShutdownCmd() cli.Command {
	name := "frontend-shutdown"
	return cli.Command{
		Name: name,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := shutdownFrontend(c); err != nil {
				logrus.Fatalf("Error running %v command: %v.", name, err)
			}
		},
	}
}

func shutdownFrontend(c *cli.Context) error {
	id := c.String("id")

	url := c.GlobalString("url")
	cli := client.NewEngineManagerClient(url)
	if err := cli.FrontendShutdown(id); err != nil {
		return err
	}
	return nil
}

func FrontendStartCallbackCmd() cli.Command {
	name := "frontend-start-callback"
	return cli.Command{
		Name: name,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := startFrontendCallback(c); err != nil {
				logrus.Fatalf("Error running %v command: %v.", name, err)
			}
		},
	}
}

func startFrontendCallback(c *cli.Context) error {
	id := c.String("id")

	url := c.GlobalString("url")
	cli := client.NewEngineManagerClient(url)
	if err := cli.FrontendStartCallback(id); err != nil {
		return err
	}
	return nil
}

func FrontendShutdownCallbackCmd() cli.Command {
	name := "frontend-shutdown-callback"
	return cli.Command{
		Name: name,
		Flags: []cli.Flag{
			cli.StringFlag{
				Name: "id",
			},
		},
		Action: func(c *cli.Context) {
			if err := shutdownFrontendCallback(c); err != nil {
				logrus.Fatalf("Error running %v command: %v.", name, err)
			}
		},
	}
}

func shutdownFrontendCallback(c *cli.Context) error {
	id := c.String("id")

	url := c.GlobalString("url")
	cli := client.NewEngineManagerClient(url)
	if err := cli.FrontendShutdownCallback(id); err != nil {
		return err
	}
	return nil
}
