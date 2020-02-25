package process

import (
	"fmt"

	"github.com/golang/protobuf/ptypes/empty"
	"golang.org/x/net/context"

	"github.com/longhorn/longhorn-instance-manager/pkg/rpc"
)

func (pm *Manager) VersionGet(ctx context.Context, empty *empty.Empty) (*rpc.VersionResponse, error) {
	return nil, fmt.Errorf("Unimplemented")
}
