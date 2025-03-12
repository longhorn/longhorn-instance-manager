package types

type UblkCreateTargetRequest struct {
	Cpumask         string `json:"cpumask,omitempty"`
	DisableUserCopy bool   `json:"disable_user_copy"`
}

type UblkGetDisksRequest struct {
	UblkId int `json:"ublk_id"`
}

type UblkDevice struct {
	BdevName   string `json:"bdev_name"`
	ID         int    `json:"id"`
	NumQueues  int    `json:"num_queues"`
	QueueDepth int    `json:"queue_depth"`
	UblkDevice string `json:"ublk_device"`
}

type UblkStartDiskRequest struct {
	BdevName   string `json:"bdev_name"`
	UblkId     int    `json:"ublk_id"`
	QueueDepth int    `json:"queue_depth"`
	NumQueues  int    `json:"num_queues"`
}

type UblkRecoverDiskRequest struct {
	BdevName string `json:"bdev_name"`
	UblkId   int    `json:"ublk_id"`
}

type UblkStopDiskRequest struct {
	UblkId int `json:"ublk_id"`
}
