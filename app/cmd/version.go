package cmd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"github.com/longhorn/longhorn-instance-manager/pkg/meta"
)

func VersionCmd() cli.Command {
	return cli.Command{
		Name: "version",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name: "client-only",
			},
		},
		Action: func(c *cli.Context) {
			if err := version(c); err != nil {
				logrus.WithError(err).Fatal("Error running info command")
			}
		},
	}
}

type VersionOutput struct {
	ClientVersion *meta.VersionOutput `json:"clientVersion"`
	ServerVersion *meta.VersionOutput `json:"serverVersion"`
}

func version(c *cli.Context) error {
	clientVersion := meta.GetVersion()
	v := VersionOutput{ClientVersion: &clientVersion}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if !c.Bool("client-only") {
		cli, err := getProcessManagerClient(c, ctx, cancel)
		if err != nil {
			return errors.Wrap(err, "failed to initialize client")
		}
		defer cli.Close()

		version, err := cli.VersionGet()
		if err != nil {
			return err
		}
		v.ServerVersion = version
	}
	output, err := json.MarshalIndent(v, "", "\t")
	if err != nil {
		return err
	}

	fmt.Println(string(output))
	return nil
}
