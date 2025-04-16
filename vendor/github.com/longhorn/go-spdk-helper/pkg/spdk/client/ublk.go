package client

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

func (c *Client) UblkCreateTarget(cpumask string, disableUserCopy bool) (err error) {
	req := spdktypes.UblkCreateTargetRequest{
		Cpumask:         cpumask,
		DisableUserCopy: disableUserCopy,
	}
	cmdOutput, err := c.jsonCli.SendCommand("ublk_create_target", req)
	if err != nil {
		return errors.Wrapf(err, "failed to UblkCreateTarget: %v", string(cmdOutput))
	}
	return nil
}

func (c *Client) UblkDestroyTarget() (err error) {
	cmdOutput, err := c.jsonCli.SendCommand("ublk_destroy_target", struct{}{})
	if err != nil {
		return errors.Wrapf(err, "failed to UblkDestroyTarget: %v", string(cmdOutput))
	}
	return nil
}

// UblkGetDisks displays full or specified ublk device list
func (c *Client) UblkGetDisks(ublkID int32) (ublkDeviceList []spdktypes.UblkDevice, err error) {
	req := spdktypes.UblkGetDisksRequest{
		UblkId: ublkID,
	}
	cmdOutput, err := c.jsonCli.SendCommand("ublk_get_disks", req)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create UblkGetDisks: %v", string(cmdOutput))
	}
	return ublkDeviceList, json.Unmarshal(cmdOutput, &ublkDeviceList)
}

func (c *Client) UblkStartDisk(bdevName string, ublkId, queueDepth, numQueues int32) (err error) {
	req := spdktypes.UblkStartDiskRequest{
		BdevName:   bdevName,
		UblkId:     ublkId,
		QueueDepth: queueDepth,
		NumQueues:  numQueues,
	}
	cmdOutput, err := c.jsonCli.SendCommand("ublk_start_disk", req)
	if err != nil {
		return errors.Wrapf(err, "failed to UblkStartDisk: %v", string(cmdOutput))
	}
	return nil
}

func (c *Client) UblkRecoverDisk(bdevName string, ublkId int32) (err error) {
	req := spdktypes.UblkRecoverDiskRequest{
		BdevName: bdevName,
		UblkId:   ublkId,
	}
	cmdOutput, err := c.jsonCli.SendCommand("ublk_recover_disk", req)
	if err != nil {
		return errors.Wrapf(err, "failed to UblkRecoverDisk: %v", string(cmdOutput))
	}
	return nil
}

func (c *Client) UblkStopDisk(ublkId int32) (err error) {
	req := spdktypes.UblkStopDiskRequest{
		UblkId: ublkId,
	}
	cmdOutput, err := c.jsonCli.SendCommand("ublk_stop_disk", req)
	if err != nil {
		return errors.Wrapf(err, "failed to UblkStopDisk: %v", string(cmdOutput))
	}
	return nil
}

func (c *Client) FindUblkDevicePath(ublkID int32) (string, error) {
	ublkDeviceList, err := c.UblkGetDisks(ublkID)
	if err != nil {
		return "", err
	}
	devicePath := ""
	for _, ublkDevice := range ublkDeviceList {
		if ublkDevice.ID == ublkID {
			if devicePath != "" {
				return "", fmt.Errorf("found multiple ublk device with the id %v", ublkID)
			}
			devicePath = ublkDevice.UblkDevice
		}
	}
	return devicePath, nil
}
