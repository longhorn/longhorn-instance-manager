package client

import (
	"net"

	"github.com/pkg/errors"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/go-spdk-helper/pkg/types"
)

type Client struct {
	conn net.Conn

	jsonCli *jsonrpc.Client
}

func NewClient() (*Client, error) {
	conn, err := net.Dial(types.DefaultJSONServerNetwork, types.DefaultUnixDomainSocketPath)
	if err != nil {
		return nil, errors.Wrap(err, "error opening socket for spdk client")
	}

	return &Client{
		conn:    conn,
		jsonCli: jsonrpc.NewClient(conn),
	}, nil
}

func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
