package cmd

import (
	"encoding/json"
	"fmt"

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

	if !c.Bool("client-only") {
		cli, err := getProcessManagerClient(c)
		if err != nil {
			return fmt.Errorf("failed to initialize client: %v", err)
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
