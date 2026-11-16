package spdk

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

// NVMe-oF connection timeouts for the ShardGroup process's connections to
// shard endpoints. Tuned longer than the replica equivalents so that brief
// network blips do not flap an EC slot to FAILED before reconnect can land.
const (
	ecShardCtrlrLossTimeoutSec  = 120
	ecShardFastIOFailTimeoutSec = 15
)

// salvageConnectMaxRetries shortens the per-shard connect retry budget
// during salvage. Salvage already accepts a missing slot as "", so retrying
// a dead endpoint for 30 seconds buys nothing - and during engine-node
// failover where shard pods may also be rescheduling, six dead slots can
// burn the entire 3-minute GRPC timeout before bdev_ec_create even runs.
// 3 attempts still absorbs a transient reconnect.
const salvageConnectMaxRetries = 3

// attachShardBaseBdev returns the base bdev name for the slot:
//   - Remote shard: connects to it over NVMe-oF and returns the NVMe
//     initiator bdev name.
//   - Local shard: stops exposing it over NVMe-oF, so that bdev_ec can open
//     its lvol for writing, and returns the lvol alias.
//
// Lock order: the caller already holds the ShardGroup lock, and for a local
// shard this function also takes the shard lock. Always take the ShardGroup
// lock before a shard lock, never the other way around, to avoid a deadlock.
func (sg *ShardGroup) attachShardBaseBdev(spdkClient *spdkclient.Client, endpoint *ShardEndpoint, connectAttempts int) (string, error) {
	if endpoint.LocalShard != nil {
		bdevName, err := endpoint.LocalShard.StopExposeForLocalAttach(spdkClient)
		if err != nil {
			return "", errors.Wrapf(err, "failed to attach slot %d to local shard %s", endpoint.SlotIndex, endpoint.LocalShard.Name)
		}
		sg.log.Infof("Attaching slot %d directly to local lvol %s", endpoint.SlotIndex, bdevName)
		return bdevName, nil
	}
	controllerName := GetShardLvolName(sg.VolumeName, endpoint.SlotIndex)
	bdevName, err := connectNVMfBdev(spdkClient, controllerName, endpoint.Address,
		ecShardCtrlrLossTimeoutSec, ecShardFastIOFailTimeoutSec, connectAttempts, retryInterval)
	if err != nil {
		return "", errors.Wrapf(err, "failed to connect shard %s at %s", controllerName, endpoint.Address)
	}
	sg.log.Infof("Attached slot %d to remote shard at %s over NVMe-oF as bdev %s", endpoint.SlotIndex, endpoint.Address, bdevName)
	return bdevName, nil
}

// detachShardBaseBdev releases the base bdev of a slot. It is the reverse of
// attachShardBaseBdev:
//   - Remote shard: detaches its NVMe-oF controller. A controller that is
//     already gone is not an error.
//   - Local shard: exposes the shard over NVMe-oF again. Call it only when
//     bdev_ec is not using the shard.
func (sg *ShardGroup) detachShardBaseBdev(spdkClient *spdkclient.Client, endpoint *ShardEndpoint) error {
	if endpoint.LocalShard != nil {
		return endpoint.LocalShard.StartExposeAfterLocalDetach(spdkClient)
	}
	controllerName := GetShardLvolName(sg.VolumeName, endpoint.SlotIndex)
	if err := shardGroupDetachNvmeController(spdkClient, controllerName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to detach controller %s", controllerName)
	}
	return nil
}

// ShardGroup is the SPDK-side process state for an EC volume's lvstore + head
// lvol layer. Each ShardGroup process owns:
//
//   - k+m NVMe-oF connections to shard endpoints (the base bdevs).
//   - A bdev_ec on top of those shard bdevs.
//   - A per-volume lvstore on top of bdev_ec.
//   - A head lvol in the lvstore, exposed via NVMe-oF for the engine to consume.
//
// The lvstore and head lvol survive volume detach (cleanupRequired=false on
// Delete). They are torn down only on volume delete (cleanupRequired=true).
// This cleanupRequired discipline is what prevents the EC-volume detach
// data-loss bug - detach must never call BdevLvolDeleteLvstore on the
// bdev_ec-backed lvstore.
type ShardGroup struct {
	sync.RWMutex

	ctx context.Context

	Name       string // typically equals VolumeName
	VolumeName string

	// EC parameters, immutable after Create.
	DataChunks   uint32
	ParityChunks uint32
	StripSizeKb  uint32
	SpecSize     uint64

	// SalvageRequested selects the recovery path on Create: tolerate
	// missing shard connections (passing "" for unreachable slots) and
	// skip lvstore + head lvol creation - SPDK's bdev_examine
	// auto-imports the existing lvstore from the encoded shard blocks.
	// Set by the controller for ShardGroup process re-provisioning on a
	// new node (engine-node failover) or after process crash.
	SalvageRequested bool

	// Shard endpoints keyed by Shard CR external name
	// (<volumeName>-<slotIndex>). Populated at Create time from
	// ShardGroupSpec.shards.
	Shards map[string]*ShardEndpoint

	// SPDK names for the layered bdev stack.
	EcBdevName  string // <volumeName>-ec
	LvsName     string // <volumeName>-lvs
	LvsUUID     string // populated after lvstore creation
	ClusterSize uint64 // lvstore cluster size in bytes. Set to
	// spdktypes.EcLvstoreClusterSize at create, queried back from SPDK.
	// Used to convert NumAllocatedClusters to bytes.
	HeadLvolName string // == VolumeName
	HeadLvolUUID string // populated after head lvol creation
	HeadAlias    string // <LvsName>/<HeadLvolName>
	Nqn          string // NVMe-oF subsystem NQN for the exposed head lvol

	// Cached snapshot lineage for ShardGroupGet. Refreshed by
	// refreshECSnapshotMapNoLock() after create/expand/snapshot operations.
	Head        *api.Lvol
	SnapshotMap map[string]*api.Lvol
	ActualSize  uint64

	IP   string
	Port int32

	State    types.InstanceState
	ErrorMsg string

	IsExposed bool

	// UpdateCh should not be protected by the ShardGroup lock.
	UpdateCh chan interface{}

	log *safelog.SafeLogger
}

// ShardEndpoint is the shard address and slot index that the
// ShardGroup process consumes as a base bdev for bdev_ec.
type ShardEndpoint struct {
	Address   string // ip:port
	SlotIndex uint32
	// LocalShard is the shard when it runs in this SPDK process. The
	// ShardGroup attaches its lvol directly instead of over NVMe-oF, and
	// the shard is not exposed over NVMe-oF for as long as bdev_ec uses the
	// lvol. Nil means the shard runs on another node and is attached over
	// NVMe-oF to Address.
	//
	// The Server resolves this field at create and replace time. It is
	// never persisted; a ShardGroup recreated on another node resolves it
	// again there.
	LocalShard *Shard
}

// GetShardGroupEcBdevName returns the SPDK bdev name for the per-volume bdev_ec.
func GetShardGroupEcBdevName(volumeName string) string {
	return fmt.Sprintf("%s-ec", volumeName)
}

// GetShardGroupLvsName returns the SPDK lvstore name on the bdev_ec.
func GetShardGroupLvsName(volumeName string) string {
	return fmt.Sprintf("%s-lvs", volumeName)
}

// NewShardGroup constructs a ShardGroup in InstanceStatePending. A subsequent
// Create call materializes the SPDK stack on disk.
//
// salvageRequested=true selects the recovery path on Create - tolerate missing
// shard connections and skip lvstore + head lvol creation, letting bdev_examine
// re-discover the existing lvstore from the encoded blocks. salvageRequested is
// also forwarded to bdev_ec_create so the SPDK layer refuses to fresh-zero a
// torn in-band unmap bitmap on a recreate.
func NewShardGroup(ctx context.Context, name, volumeName string, specSize uint64,
	dataChunks, parityChunks, stripSizeKb uint32, shards map[string]*ShardEndpoint,
	salvageRequested bool, updateCh chan interface{}) *ShardGroup {

	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"shardGroupName": name,
		"volumeName":     volumeName,
		"dataChunks":     dataChunks,
		"parityChunks":   parityChunks,
		"stripSizeKb":    stripSizeKb,
	})

	roundedSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSize != specSize {
		log.Infof("Rounded up size from %v to %v since the size should be a multiple of MiB", specSize, roundedSize)
	}
	log = log.WithField("specSize", roundedSize)

	lvsName := GetShardGroupLvsName(volumeName)
	headLvolName := volumeName

	return &ShardGroup{
		ctx: ctx,

		Name:       name,
		VolumeName: volumeName,

		DataChunks:       dataChunks,
		ParityChunks:     parityChunks,
		StripSizeKb:      stripSizeKb,
		SpecSize:         roundedSize,
		SalvageRequested: salvageRequested,

		Shards: shards,

		EcBdevName:   GetShardGroupEcBdevName(volumeName),
		LvsName:      lvsName,
		HeadLvolName: headLvolName,
		HeadAlias:    spdktypes.GetLvolAlias(lvsName, headLvolName),
		Nqn:          helpertypes.GetNQN(headLvolName),

		SnapshotMap: map[string]*api.Lvol{},

		State: types.InstanceStatePending,

		UpdateCh: updateCh,

		log: safelog.NewSafeLogger(log),
	}
}

// ServiceShardGroupToProtoShardGroup converts in-memory ShardGroup state to the
// gRPC response message. EcStatus is left nil here; it is populated by
// ShardGroup.Get from live SPDK state, only when the ShardGroup is Running
// (the EC bdev exists only after a successful Create).
//
// Head and snapshot lvols route through serviceShardGroupLvolToProtoLvol, which
// renames the head to types.VolumeHead; without it the engine's ancestor
// selection rejects the EC backend and leaves e.Head, e.SnapshotMap, and
// e.ActualSize unwritten.
func ServiceShardGroupToProtoShardGroup(sg *ShardGroup) *spdkrpc.ShardGroup {
	snapshots := make(map[string]*spdkrpc.Lvol, len(sg.SnapshotMap))
	for snapshotName, snapshotLvol := range sg.SnapshotMap {
		snapshots[snapshotName] = serviceShardGroupLvolToProtoLvol(sg.HeadLvolName, snapshotLvol)
	}

	return &spdkrpc.ShardGroup{
		Name:         sg.Name,
		VolumeName:   sg.VolumeName,
		SpecSize:     sg.SpecSize,
		DataChunks:   sg.DataChunks,
		ParityChunks: sg.ParityChunks,
		StripSizeKb:  sg.StripSizeKb,
		EcBdevName:   sg.EcBdevName,
		LvsName:      sg.LvsName,
		LvsUuid:      sg.LvsUUID,
		HeadLvolName: sg.HeadLvolName,
		HeadLvolUuid: sg.HeadLvolUUID,
		NvmfNqn:      sg.Nqn,
		Ip:           sg.IP,
		Port:         sg.Port,
		ProcessState: instanceStateToProcessState(sg.State),
		ErrorMsg:     sg.ErrorMsg,
		ActualSize:   sg.ActualSize,
		Head:         serviceShardGroupLvolToProtoLvol(sg.HeadLvolName, sg.Head),
		Snapshots:    snapshots,
	}
}

// serviceShardGroupLvolToProtoLvol is the EC analog of ServiceLvolToProtoLvol.
// It carries the same engine-facing contract - the head lvol's Name is reported
// as types.VolumeHead, and any Parent / Children reference to the head bdev is
// rewritten to the same constant - so engine code that compares against
// types.VolumeHead works uniformly across RAID1 and EC topologies.
//
// EC snapshot lvols carry no replica-prefix (unlike v2 replication), so no
// name stripping is required; only the head identity is rewritten.
//
// Children is allocated fresh rather than mutated in place: api.LvolToProtoLvol
// copies the Children map reference, so in-place rewriting would alias back
// into the in-memory sg.Head.Children / sg.SnapshotMap[*].Children and corrupt
// the cache across calls.
func serviceShardGroupLvolToProtoLvol(headLvolName string, lvol *api.Lvol) *spdkrpc.Lvol {
	p := api.LvolToProtoLvol(lvol)
	if p == nil {
		return nil
	}
	if p.Name == headLvolName {
		p.Name = types.VolumeHead
	}
	if p.Parent == headLvolName {
		p.Parent = types.VolumeHead
	}
	if len(p.Children) > 0 {
		newChildren := make(map[string]bool, len(p.Children))
		for k, v := range p.Children {
			if k == headLvolName {
				newChildren[types.VolumeHead] = v
			} else {
				newChildren[k] = v
			}
		}
		p.Children = newChildren
	}
	return p
}

func instanceStateToProcessState(s types.InstanceState) string {
	switch s {
	case types.InstanceStateRunning:
		return "running"
	case types.InstanceStateError:
		return "error"
	default:
		return "stopped"
	}
}

func copyEcCountersFromBdevInfo(status *spdkrpc.EcStatus, info *spdktypes.BdevEcInfo) {
	status.UnmapsSubmitted = info.UnmapsSubmitted
	status.UnmapsCompleted = info.UnmapsCompleted
	status.UnmapsDeferredBusy = info.UnmapsDeferredBusy
	status.UnmapsViaWriteZeros = info.UnmapsViaWriteZeros
	status.UnmapFanoutMisses = info.UnmapFanoutMisses
	status.UnmappedStripes = info.UnmappedStripes
	status.DegradedReadEioDirty = info.DegradedReadEioDirty
	status.DegradedReadsReconstructed = info.DegradedReadsReconstructed
	status.RmwTotal = info.RmwTotal
	status.RmwDeferredScrub = info.RmwDeferredScrub
	status.RmwDeferredDirty = info.RmwDeferredDirty
	status.RmwDeferredInflight = info.RmwDeferredInflight
	status.FullStripeWrites = info.FullStripeWrites
	status.FullStripeWritesDeferred = info.FullStripeWritesDeferred
	status.UnmapsFailed = info.UnmapsFailed
	status.UnmappedReadsSynthesized = info.UnmappedReadsSynthesized
	status.WritesIntoUnmapped = info.WritesIntoUnmapped
	status.WritesIntoUnmappedFailed = info.WritesIntoUnmappedFailed
}

// copyEcHealthFromBdevInfo copies the health of the EC bdev into the
// response: its overall state, whether a rebuild is running, and the state
// of each slot. The controller finds a failed shard by reading the slot
// states. Without them, a failed shard looks healthy.
func copyEcHealthFromBdevInfo(status *spdkrpc.EcStatus, info *spdktypes.BdevEcInfo) {
	status.State = bdevEcStateToProto(info.State)
	status.RebuildInProgress = info.RebuildInProgress

	status.Slots = make([]*spdkrpc.EcSlot, 0, len(info.BaseBdevs))
	for _, baseBdev := range info.BaseBdevs {
		status.Slots = append(status.Slots, &spdkrpc.EcSlot{
			SlotIndex: baseBdev.Slot,
			Role:      bdevEcSlotRoleToProto(baseBdev.Role),
			State:     bdevEcSlotStateToProto(string(baseBdev.State)),
			BdevName:  baseBdev.Name,
		})
	}
}

// getEcBdevInfoNoLock returns the single EC bdev backing this shardgroup,
// erroring if SPDK reports zero or multiple. The caller must hold sg's lock.
func (sg *ShardGroup) getEcBdevInfoNoLock(spdkClient *spdkclient.Client) (*spdktypes.BdevEcInfo, error) {
	bdevList, err := shardGroupGetEcBdevs(spdkClient, sg.EcBdevName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get EC bdev info for shardgroup %s", sg.Name)
	}
	if len(bdevList) != 1 {
		return nil, fmt.Errorf("expected exactly one EC bdev for shardgroup %s, got %d", sg.Name, len(bdevList))
	}
	return &bdevList[0], nil
}

// ecUsableFitsSpec reports whether the EC bdev can hold specSize bytes. SPDK
// already subtracts the front reservation from blockcnt, so numBlocks*blockSize
// is the usable size.
func ecUsableFitsSpec(numBlocks uint64, blockSize uint32, specSize uint64) bool {
	return numBlocks*uint64(blockSize) >= specSize
}

// lvstoreUsableFitsSpec reports whether totalDataClusters clusters of
// clusterSize bytes can back specSize bytes.
func lvstoreUsableFitsSpec(totalDataClusters, clusterSize, specSize uint64) bool {
	return totalDataClusters*clusterSize >= specSize
}

// exceedsInPlaceGrowthCeiling reports whether newSize is beyond the
// in-place growth ceiling (EcLvstoreMaxGrowthFactor x creationSize).
// creationSize == 0 means unknown; the ceiling is not enforced.
func exceedsInPlaceGrowthCeiling(newSize, creationSize uint64) bool {
	return creationSize > 0 && newSize > spdktypes.EcLvstoreMaxGrowthFactor*creationSize
}

// ecUsableExceedsCreationCap reports whether the EC bdev is too large for
// SPDK to size lvstore metadata for (EcLvstoreMaxCreationSize). SPDK floors
// the device to whole clusters before sizing, and admission
// (ValidateECCreationSize) compares the same way, so compare in clusters to
// agree with both at the boundary.
func ecUsableExceedsCreationCap(usable uint64) bool {
	return usable/spdktypes.EcLvstoreClusterSize > uint64(spdktypes.EcLvstoreMaxCreationSize)/spdktypes.EcLvstoreClusterSize
}

// discoveredSpecSize returns the spec size to resume with after discovering
// an existing head lvol. A head smaller than the requested spec means an
// interrupted expansion (the manager already requests the new size while the
// head is still the old size); resume from the head so the Expand fast path
// does not treat the expansion as complete. A head at or above the spec is
// the clean path or SPDK cluster rounding; keep the requested size.
func discoveredSpecSize(headSize, requestedSpecSize uint64) uint64 {
	if headSize < requestedSpecSize {
		return headSize
	}
	return requestedSpecSize
}

// assertEcUsableFitsSpecNoLock fails if the EC bdev cannot host the lvstore
// and head lvol. Two checks:
//
//   - The device size, floored to whole clusters, must not exceed
//     EcLvstoreMaxCreationSize. SPDK sizes lvstore metadata from the device
//     size, not SpecSize, and the shards over-provision, so the device is
//     what must stay under the limit.
//   - The device size must be at least SpecSize.
//
// The size comes from BdevGetBdevs because BdevEcInfo has no size field.
// The caller must hold sg's lock.
func (sg *ShardGroup) assertEcUsableFitsSpecNoLock(spdkClient *spdkclient.Client) error {
	bdevList, err := spdkClient.BdevGetBdevs(sg.EcBdevName, 0)
	if err != nil {
		return errors.Wrapf(err, "failed to query EC bdev %s for capacity check", sg.EcBdevName)
	}
	if len(bdevList) != 1 {
		return fmt.Errorf("expected exactly one EC bdev %s for capacity check, got %d", sg.EcBdevName, len(bdevList))
	}
	bdev := bdevList[0]
	usable := bdev.NumBlocks * uint64(bdev.BlockSize)
	if ecUsableExceedsCreationCap(usable) {
		return fmt.Errorf("EC bdev %s size %d bytes exceeds the lvstore creation limit %d bytes: volume size %d is too large",
			sg.EcBdevName, usable, uint64(spdktypes.EcLvstoreMaxCreationSize), sg.SpecSize)
	}
	if modeled := spdktypes.EcUsableSize(int64(sg.SpecSize), int(sg.DataChunks), int(sg.StripSizeKb)); modeled != usable {
		sg.log.Warnf("EC bdev %s usable size %d bytes differs from the %d bytes modeled from the shard sizing formula; "+
			"a shard was sized off-formula, so admission and this check may disagree near the creation cap",
			sg.EcBdevName, usable, modeled)
	}
	if !ecUsableFitsSpec(bdev.NumBlocks, bdev.BlockSize, sg.SpecSize) {
		return fmt.Errorf("EC bdev %s usable capacity %d bytes < required %d bytes "+
			"(shards under-sized: per-shard size must budget the EC front reservation and lvstore metadata)",
			sg.EcBdevName, usable, sg.SpecSize)
	}
	return nil
}

// Get returns the proto ShardGroup enriched with live EC state queried from
// SPDK. Base fields come from the in-memory cache via
// ServiceShardGroupToProtoShardGroup; EcStatus is populated only when the
// ShardGroup is Running (the EC bdev exists only after a successful Create),
// and is otherwise nil - which the controller treats as "no live state
// available yet" rather than as healthy/degraded.
func (sg *ShardGroup) Get(spdkClient *spdkclient.Client) (*spdkrpc.ShardGroup, error) {
	sg.RLock()
	defer sg.RUnlock()

	proto := ServiceShardGroupToProtoShardGroup(sg)
	if sg.State != types.InstanceStateRunning {
		return proto, nil
	}

	ecStatus := &spdkrpc.EcStatus{}

	ecInfo, err := sg.getEcBdevInfoNoLock(spdkClient)
	if err != nil {
		return nil, err
	}
	copyEcCountersFromBdevInfo(ecStatus, ecInfo)
	copyEcHealthFromBdevInfo(ecStatus, ecInfo)

	// bdev_ec_get_bdevs does not return the rebuild percentage, so read
	// the progress from bdev_ec_get_rebuild_progress.
	if ecInfo.RebuildInProgress {
		progress, err := spdkClient.BdevEcGetRebuildProgress(sg.EcBdevName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get rebuild progress for shardgroup %s", sg.Name)
		}
		// The rebuild can finish between the two SPDK calls. SPDK then
		// reports no rebuild, the progress is empty, and the state is
		// idle. Leave RebuildProgress unset in that case.
		if progress.RebuildState != spdktypes.BdevEcRebuildStateIdle {
			ecStatus.RebuildProgress = &spdkrpc.EcRebuildProgress{
				CurrentSlot:     progress.CurrentSlot,
				CurrentStripe:   progress.CurrentStripe,
				NumStripes:      progress.NumStripes,
				StripesRebuilt:  progress.StripesRebuilt,
				SlotsToRebuild:  progress.SlotsToRebuild,
				PercentComplete: progress.PercentComplete,
			}
		}
	}

	unmapStatus, err := spdkClient.BdevEcGetUnmapStatus(sg.EcBdevName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get unmap bitmap status for shardgroup %s", sg.Name)
	}
	ecStatus.UnmapBitmapStatus = &spdkrpc.EcUnmapBitmapStatus{
		Generation:     unmapStatus.Generation,
		BlobBytes:      unmapStatus.BlobBytes,
		NumStripes:     unmapStatus.NumStripes,
		ActiveCopy:     unmapStatus.ActiveCopy,
		PersistPending: unmapStatus.PersistPending,
	}

	proto.EcStatus = ecStatus
	return proto, nil
}

// refreshECSnapshotMapNoLock rebuilds the in-memory head/snapshot cache for
// ShardGroupGet from live lvol bdevs in this ShardGroup's lvstore. Caller must
// hold the shardgroup lock.
//
// Per-lvol ActualSize is NumAllocatedClusters * sg.ClusterSize. The cluster
// size MUST come from the lvstore (queried at create/discovery time, cached on
// sg.ClusterSize) rather than from the defaultClusterSize constant - the
// EC-stack lvstore uses EcLvstoreClusterSize (4 MiB), pinned at create, so
// using defaultClusterSize (1 MiB) would silently report every size as 1/4
// of physical.
//
// sg.ActualSize sums head + all snapshots, mirroring the RAID1 path at
// replica.go. Reporting only the head's allocation would drop to ~0 every
// time a snapshot moves the previously-written data into a snapshot lvol.
func (sg *ShardGroup) refreshECSnapshotMapNoLock(spdkClient *spdkclient.Client) error {
	if sg.ClusterSize == 0 {
		return fmt.Errorf("BUG: shardgroup %s has zero ClusterSize; must be populated before refreshECSnapshotMapNoLock", sg.Name)
	}

	filter := func(b *spdktypes.BdevInfo) bool {
		if b.DriverSpecific == nil || b.DriverSpecific.Lvol == nil {
			return false
		}
		return b.DriverSpecific.Lvol.LvolStoreUUID == sg.LvsUUID
	}

	bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, filter)
	if err != nil {
		return errors.Wrapf(err, "failed to get lvol map for shardgroup %s", sg.Name)
	}

	newSnapshotMap := map[string]*api.Lvol{}
	var newHead *api.Lvol

	for lvolName, bdev := range bdevLvolMap {
		if bdev.DriverSpecific == nil || bdev.DriverSpecific.Lvol == nil {
			continue
		}

		svcLvol := &api.Lvol{
			Name:              lvolName,
			SpecSize:          bdev.NumBlocks * uint64(bdev.BlockSize),
			ActualSize:        bdev.DriverSpecific.Lvol.NumAllocatedClusters * sg.ClusterSize,
			Parent:            bdev.DriverSpecific.Lvol.BaseSnapshot,
			Children:          map[string]bool{},
			CreationTime:      bdev.CreationTime,
			UserCreated:       bdev.DriverSpecific.Lvol.Xattrs[spdkclient.UserCreated] == strconv.FormatBool(true),
			SnapshotTimestamp: bdev.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotTimestamp],
			SnapshotChecksum:  bdev.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotChecksum],
		}

		if lvolName == sg.HeadLvolName {
			newHead = svcLvol
			continue
		}

		if bdev.DriverSpecific.Lvol.Snapshot {
			newSnapshotMap[lvolName] = svcLvol
		}
	}

	if newHead == nil {
		return fmt.Errorf("failed to find head lvol %s in lvstore %s", sg.HeadLvolName, sg.LvsName)
	}

	for snapshotName, snapshotLvol := range newSnapshotMap {
		if snapshotLvol.Parent == "" {
			continue
		}
		if snapshotLvol.Parent == sg.HeadLvolName {
			continue
		}

		if parentSnapshot, ok := newSnapshotMap[snapshotLvol.Parent]; ok {
			parentSnapshot.Children[snapshotName] = true
		}
	}

	// Link the head as a child of its parent snapshot. Without this, consumers
	// walking the tree from the latest snapshot can't find the head - they'd
	// only see snapshot-to-snapshot edges. Matches RAID1's pattern in
	// replica.go. The proto converter (serviceShardGroupLvolToProtoLvol)
	// renames the head bdev to types.VolumeHead at the wire boundary, so the
	// engine sees the same shape as RAID1.
	if newHead.Parent != "" {
		if parentSnapshot, ok := newSnapshotMap[newHead.Parent]; ok {
			parentSnapshot.Children[sg.HeadLvolName] = true
		}
	}

	sg.Head = newHead
	sg.SnapshotMap = newSnapshotMap
	sg.ActualSize = newHead.ActualSize
	for _, snapLvol := range newSnapshotMap {
		sg.ActualSize += snapLvol.ActualSize
	}

	return nil
}

// Create materializes the EC stack on disk: NVMe-attach to all k+m shards,
// create bdev_ec, create lvstore on bdev_ec, create the head lvol, expose via
// NVMe-oF. The lvstore + head lvol persist across volume detach/re-attach;
// they are removed only by Delete with cleanupRequired=true.
func (sg *ShardGroup) Create(spdkClient *spdkclient.Client, superiorPortAllocator *commonbitmap.Bitmap) (ret *spdkrpc.ShardGroup, err error) {
	updateRequired := true

	sg.Lock()
	defer func() {
		sg.Unlock()
		if updateRequired {
			sg.UpdateCh <- nil
		}
	}()

	if sg.State == types.InstanceStateRunning {
		updateRequired = false
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "shardgroup %s already exists and running", sg.Name)
	}
	// Stopped is the post-detach (cleanupRequired=false) state. Re-Create
	// from Stopped is the same-node re-attach path: bdev_ec is rebuilt, the
	// preserved lvstore on encoded blocks is re-discovered via examine, and
	// the head lvol is re-exposed. Mirrors Replica.Create which accepts
	// Pending and Stopped for the same reason.
	if sg.State != types.InstanceStatePending && sg.State != types.InstanceStateStopped {
		updateRequired = false
		return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shardgroup %s creation", sg.State, sg.Name)
	}

	// Create stops exposing the local shards so that bdev_ec can use them.
	// If Create then fails, the ShardGroup goes to Error and nothing
	// retries it, so the shards have to be exposed again.
	//
	// shardsInUse decides who does that:
	//   - false: no bdev_ec is using the shards, so Create exposes them
	//     again right away.
	//   - true: the bdev_ec created by this call is using the shards, and
	//     exposing them now would give them a second writer. Delete exposes
	//     them again after it removes the bdev_ec.
	shardsInUse := false

	defer func() {
		if err != nil {
			sg.log.WithError(err).Errorf("Failed to create shardgroup %s", sg.Name)
			if !shardsInUse {
				sg.exposeLocalShardsNoLock(spdkClient)
			}
			sg.State = types.InstanceStateError
			sg.ErrorMsg = err.Error()
			ret = ServiceShardGroupToProtoShardGroup(sg)
			err = nil
		} else {
			sg.ErrorMsg = ""
			sg.log.Info("Created shardgroup")
		}
	}()

	total := sg.DataChunks + sg.ParityChunks
	if uint32(len(sg.Shards)) != total {
		return nil, fmt.Errorf("shardgroup %s requires %d shards (dataChunks=%d + parityChunks=%d), got %d",
			sg.Name, total, sg.DataChunks, sg.ParityChunks, len(sg.Shards))
	}

	// Connect to all k+m shards and build base bdev list ordered by slot index.
	// In salvage mode, tolerate per-shard connection failures by passing "" at
	// the slot's position - the EC module marks that slot FAILED but continues
	// in degraded mode.
	baseBdevs := make([]string, total)
	for shardName, endpoint := range sg.Shards {
		if endpoint.SlotIndex >= total {
			return nil, fmt.Errorf("shard %s slot index %d exceeds dataChunks+parityChunks=%d", shardName, endpoint.SlotIndex, total)
		}
		if baseBdevs[endpoint.SlotIndex] != "" {
			return nil, fmt.Errorf("duplicate slot index %d: shard %s conflicts with an earlier shard", endpoint.SlotIndex, shardName)
		}
		connectAttempts := maxRetries
		if sg.SalvageRequested {
			connectAttempts = salvageConnectMaxRetries
		}
		bdevName, connErr := sg.attachShardBaseBdev(spdkClient, endpoint, connectAttempts)
		if connErr != nil {
			if sg.SalvageRequested {
				sg.log.WithError(connErr).Warnf("Salvage: marking slot %d (shard %s) as missing", endpoint.SlotIndex, shardName)
				baseBdevs[endpoint.SlotIndex] = ""
				continue
			}
			return nil, connErr
		}
		baseBdevs[endpoint.SlotIndex] = bdevName
	}

	// Pre-check for a stale bdev_ec from a prior OFFLINE failure: SPDK's
	// bdev_ec OFFLINE cleanup closes descriptors to dead base bdevs but
	// leaves the EC bdev itself registered, so a recovery Create on the
	// same name otherwise fails with EEXIST. Delete the stale instance
	// first; the surviving shard lvols are untouched, and bdev_examine
	// on the new bdev_ec will rediscover the lvstore via
	// tryDiscoverExistingLvstore below.
	existing, err := spdkClient.BdevEcGetBdevs(sg.EcBdevName)
	if err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, errors.Wrapf(err, "failed to check for existing EC bdev %s", sg.EcBdevName)
	}
	if len(existing) > 0 {
		sg.log.Warnf("Existing EC bdev %s found before create; deleting stale instance before recreating", sg.EcBdevName)
		if _, err := spdkClient.BdevEcDelete(sg.EcBdevName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return nil, errors.Wrapf(err, "failed to delete stale EC bdev %s", sg.EcBdevName)
		}
	}

	// Create bdev_ec on the k+m base bdevs.
	sg.log.Infof("Creating EC bdev with dataChunks=%d parityChunks=%d stripSizeKb=%d baseBdevs=%v",
		sg.DataChunks, sg.ParityChunks, sg.StripSizeKb, baseBdevs)
	if _, err := spdkClient.BdevEcCreate(sg.EcBdevName, sg.DataChunks, sg.ParityChunks, sg.StripSizeKb, baseBdevs,
		sg.SalvageRequested); err != nil {
		return nil, errors.Wrapf(err, "failed to create EC bdev %s", sg.EcBdevName)
	}
	// The new bdev_ec is now using the shards.
	shardsInUse = true

	// Always try to discover an existing lvstore/head first. This makes
	// clean detach -> recreate idempotent even when salvage_requested=false.
	// If nothing exists yet, fall back to fresh create.
	foundExistingLvstore, err := sg.tryDiscoverExistingLvstore(spdkClient)
	if err != nil {
		return nil, err
	}

	if !foundExistingLvstore {
		if sg.SalvageRequested {
			return nil, fmt.Errorf("salvage requested but existing lvstore/head not found for shardgroup %s", sg.Name)
		}

		// Fail fast if the EC bdev cannot host the head: too large for
		// SPDK's lvstore md-pages sizing bound, or too small for SpecSize
		// (the head is thin, so SPDK would otherwise accept an undersized
		// volume and only fail later on write). Only the fresh-create path
		// is guarded: an existing lvstore was created under the bound.
		if err := sg.assertEcUsableFitsSpecNoLock(spdkClient); err != nil {
			return nil, err
		}

		// Fresh-create path: create lvstore + head lvol on bdev_ec. The
		// cluster size and md-pages ratio are pinned to the constants that
		// ComputeShardSize sizes shards from, so an SPDK default change
		// cannot alter the geometry. The cluster size is still queried back
		// for the NumAllocatedClusters -> bytes math in
		// refreshECSnapshotMapNoLock.
		lvsUUID, err := spdkClient.BdevLvolCreateLvstoreWithMdRatio(sg.EcBdevName, sg.LvsName,
			spdktypes.EcLvstoreClusterSize, spdktypes.EcLvstoreMdPagesPerClusterRatio)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create lvstore %s on EC bdev %s", sg.LvsName, sg.EcBdevName)
		}
		sg.LvsUUID = lvsUUID

		lvstoreList, err := spdkClient.BdevLvolGetLvstore("", lvsUUID)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to query freshly-created lvstore %s for shardgroup %s", sg.LvsName, sg.Name)
		}
		if len(lvstoreList) != 1 {
			return nil, fmt.Errorf("expected exactly one lvstore for uuid %s, found %d", lvsUUID, len(lvstoreList))
		}
		sg.ClusterSize = lvstoreList[0].ClusterSize

		// The head lvol is thin, so SPDK would accept it even when the
		// lvstore cannot back it, deferring the failure to an ENOSPC
		// mid-workload. Fail the create here instead. Delete the fresh,
		// empty lvstore first: leaving it behind would make the next
		// Create find a lvstore without a head lvol and fail in
		// tryDiscoverExistingLvstore, so the create would never become
		// retryable.
		if !lvstoreUsableFitsSpec(lvstoreList[0].TotalDataClusters, lvstoreList[0].ClusterSize, sg.SpecSize) {
			capacityErr := fmt.Errorf("lvstore %s on EC bdev %s can back only %d of %d bytes "+
				"(shards under-sized: blobstore metadata not budgeted)",
				sg.LvsName, sg.EcBdevName, lvstoreList[0].TotalDataClusters*lvstoreList[0].ClusterSize, sg.SpecSize)
			// Clear LvsUUID only after a successful delete. Delete with
			// cleanupRequired=true removes the lvstore only when LvsUUID
			// is set, so clearing it here would leak the lvstore for good.
			if _, deleteErr := spdkClient.BdevLvolDeleteLvstore("", lvsUUID); deleteErr != nil {
				return nil, errors.Wrapf(deleteErr, "failed to delete under-sized lvstore %s (uuid %s): %v",
					sg.LvsName, lvsUUID, capacityErr)
			}
			sg.LvsUUID = ""
			sg.ClusterSize = 0
			return nil, capacityErr
		}

		headLvolUUID, err := spdkClient.BdevLvolCreate("", lvsUUID, sg.HeadLvolName, util.BytesToMiB(sg.SpecSize), "", true)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to create head lvol %s in lvstore %s", sg.HeadLvolName, sg.LvsName)
		}
		sg.HeadLvolUUID = headLvolUUID
	}

	// Expose the head lvol via NVMe-oF.
	if err := sg.prepareIPAndPort(superiorPortAllocator); err != nil {
		return nil, err
	}
	if err := spdkClient.StartExposeBdev(sg.Nqn, sg.HeadLvolUUID, generateNGUID(sg.HeadLvolName),
		sg.IP, strconv.Itoa(int(sg.Port)), helpertypes.InternalHostNQN); err != nil {
		return nil, errors.Wrapf(err, "failed to expose head lvol for shardgroup %s", sg.Name)
	}
	sg.IsExposed = true

	if err := sg.refreshECSnapshotMapNoLock(spdkClient); err != nil {
		return nil, errors.Wrapf(err, "failed to refresh shardgroup snapshot/head cache for %s", sg.Name)
	}

	sg.State = types.InstanceStateRunning
	return ServiceShardGroupToProtoShardGroup(sg), nil
}

// exposeLocalShardsNoLock exposes every local shard over NVMe-oF again. A
// failure is logged and the remaining shards are still handled.
//
// The caller must hold the ShardGroup lock. Call it only when bdev_ec is not
// using the shards, because an exposed shard must not have a second writer.
func (sg *ShardGroup) exposeLocalShardsNoLock(spdkClient *spdkclient.Client) {
	for shardName, endpoint := range sg.Shards {
		if endpoint.LocalShard == nil {
			continue
		}
		if err := endpoint.LocalShard.StartExposeAfterLocalDetach(spdkClient); err != nil {
			sg.log.WithError(err).Warnf("Failed to expose local shard %s again", shardName)
		}
	}
}

// StopExposeForLocalAttach stops exposing the head lvol over NVMe-oF and
// returns its alias. An engine in the same SPDK process can then use the
// head lvol directly as a base bdev of its raid bdev.
//
// The head lvol has to be unexposed before the engine creates the raid
// bdev. While it is exposed, the NVMe-oF target holds an exclusive write
// claim on it, and the raid bdev cannot open it.
//
// The ShardGroup keeps its IP, port, and NQN, so
// StartExposeAfterLocalDetach can expose the head lvol again at the same
// address. The ShardGroup stays Running, and only IsExposed changes.
func (sg *ShardGroup) StopExposeForLocalAttach(spdkClient *spdkclient.Client) (string, error) {
	sg.Lock()
	defer sg.Unlock()

	if sg.State != types.InstanceStateRunning {
		return "", grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"cannot attach shardgroup %s locally in state %s (local attach requires a running shardgroup)", sg.Name, sg.State)
	}

	if sg.IsExposed {
		if err := shardGroupStopExposeBdev(spdkClient, sg.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return "", errors.Wrapf(err, "failed to stop exposing shardgroup %s for local attach", sg.Name)
		}
		sg.IsExposed = false
		sg.log.Info("Stopped exposing head lvol for local attach")
	}

	return sg.HeadAlias, nil
}

// StartExposeAfterLocalDetach exposes the head lvol over NVMe-oF again,
// undoing StopExposeForLocalAttach. Call it only after the local engine has
// deleted its raid bdev.
//
// If the head lvol cannot be exposed, the ShardGroup stays Running and
// unexposed. It does not move to the Error state, because the manager never
// recreates a ShardGroup in the Error state. An engine in this SPDK process
// can still attach. An engine on another node cannot connect until a later
// call exposes the head lvol.
func (sg *ShardGroup) StartExposeAfterLocalDetach(spdkClient *spdkclient.Client) error {
	sg.Lock()
	defer sg.Unlock()

	if sg.State != types.InstanceStateRunning || sg.IsExposed {
		return nil
	}

	if err := shardGroupStartExposeBdev(spdkClient, sg.Nqn, sg.HeadLvolUUID, generateNGUID(sg.HeadLvolName),
		sg.IP, strconv.Itoa(int(sg.Port))); err != nil {
		// A failed expose can leave the subsystem behind without a
		// namespace. Remove it, or the next expose fails because the
		// subsystem already exists.
		if stopErr := shardGroupStopExposeBdev(spdkClient, sg.Nqn); stopErr != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(stopErr) {
			sg.log.WithError(stopErr).Warn("Failed to remove the NVMe-oF subsystem after a failed expose")
		}
		return errors.Wrapf(err, "failed to expose shardgroup %s after local detach", sg.Name)
	}
	sg.IsExposed = true
	sg.log.Info("Exposed head lvol again after local detach")

	return nil
}

// Delete tears down the EC stack with cleanupRequired discipline.
//
// cleanupRequired=true (volume deletion path): full teardown - delete head
// lvol, delete lvstore, delete bdev_ec, disconnect NVMe-oF controllers.
//
// cleanupRequired=false (detach path): unexpose NVMe-oF + delete bdev_ec +
// disconnect NVMe-oF controllers, but **leave the lvstore and head lvol
// intact on the encoded shard blocks**. This is the central mechanism that
// prevents the EC-volume detach data-loss bug - re-attach reconstructs
// bdev_ec via SPDK's bdev_examine, which auto-imports the existing lvstore.
func (sg *ShardGroup) Delete(spdkClient *spdkclient.Client, cleanupRequired bool, superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	updateRequired := false

	sg.Lock()
	defer func() {
		if err != nil {
			sg.log.WithError(err).Errorf("Failed to delete shardgroup with cleanupRequired=%v", cleanupRequired)
			if sg.State != types.InstanceStateError {
				sg.State = types.InstanceStateError
				sg.ErrorMsg = err.Error()
			}
		} else {
			// cleanupRequired=true: in-memory record is about to be removed
			// from shardGroupMap by the caller, so Terminating is the
			// correct transient state.
			// cleanupRequired=false (detach): the record stays in the map
			// for future re-attach. Use Stopped so Create's precondition
			// admits the re-Create path (mirrors Replica.Delete semantics).
			if cleanupRequired {
				sg.State = types.InstanceStateTerminating
			} else {
				sg.State = types.InstanceStateStopped
			}
			sg.ErrorMsg = ""
		}

		updateRequired = true

		sg.Unlock()

		if updateRequired {
			sg.UpdateCh <- nil
		}
	}()

	sg.log.Infof("Deleting shardgroup with cleanupRequired=%v", cleanupRequired)

	// 1. Stop NVMe-oF expose of the head lvol.
	if sg.IsExposed {
		if err := spdkClient.StopExposeBdev(sg.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Wrapf(err, "failed to stop exposing shardgroup %s", sg.Name)
		}
		sg.IsExposed = false
	}

	// 2. Release allocated port.
	if sg.Port != 0 {
		if err := superiorPortAllocator.ReleaseRange(sg.Port, sg.Port); err != nil {
			return errors.Wrapf(err, "failed to release port %d during shardgroup %s deletion", sg.Port, sg.Name)
		}
		sg.Port = 0
	}

	// 3. Conditionally delete head lvol + lvstore (THE BUG FIX).
	//    cleanupRequired=false leaves the lvstore + head lvol on the encoded
	//    shard blocks so that re-attach can re-discover them via bdev_examine.
	if cleanupRequired {
		if sg.HeadLvolUUID != "" {
			if _, err := spdkClient.BdevLvolDelete(sg.HeadAlias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				return errors.Wrapf(err, "failed to delete head lvol %s for shardgroup %s", sg.HeadAlias, sg.Name)
			}
			sg.HeadLvolUUID = ""
		}
		if sg.LvsUUID != "" {
			if _, err := spdkClient.BdevLvolDeleteLvstore(sg.LvsName, ""); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				return errors.Wrapf(err, "failed to delete lvstore %s for shardgroup %s", sg.LvsName, sg.Name)
			}
			sg.LvsUUID = ""
			sg.ClusterSize = 0
		}
	} else {
		sg.log.Info("Preserving lvstore + head lvol on bdev_ec for re-attach (cleanupRequired=false)")
	}

	// 4. Delete bdev_ec.
	if _, err := spdkClient.BdevEcDelete(sg.EcBdevName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to delete EC bdev %s for shardgroup %s", sg.EcBdevName, sg.Name)
	}

	// 5. Detach all shards: disconnect the NVMe-oF controller of each
	//    remote shard, and expose each local shard again. A failure is
	//    logged and does not fail Delete. A local shard that cannot be
	//    exposed moves to the Error state.
	for shardName, endpoint := range sg.Shards {
		if detachErr := sg.detachShardBaseBdev(spdkClient, endpoint); detachErr != nil {
			sg.log.WithError(detachErr).Warnf("Failed to detach shard %s; continuing", shardName)
		}
	}

	sg.log.Info("Deleted shardgroup")
	return nil
}

// Sync is a passive validator that mirrors Shard.Sync and Replica.Sync. It
// never re-allocates a port and never re-exposes the bdev. Behavior depends
// on the current state:
//
//   - Pending: walks to Stopped (newly discovered after process restart;
//     the ShardGroup controller's recovery path replaces or re-provisions).
//   - Running: validates that the live exposed port matches sg.Port; any
//     drift transitions to Error.
//   - other: no-op.
//
// In-place re-expose is deliberately avoided. Recovery of a ShardGroup
// process on a different node uses the salvage path (ShardGroupCreate with
// salvage_requested=true), not Sync.
func (sg *ShardGroup) Sync(spdkClient *spdkclient.Client) (err error) {
	sg.Lock()
	prevState, prevErrorMsg := sg.State, sg.ErrorMsg
	defer func() {
		updateRequired := sg.State != prevState || sg.ErrorMsg != prevErrorMsg
		sg.Unlock()
		if updateRequired {
			sg.UpdateCh <- nil
		}
	}()
	// Only a real state mismatch moves the shardgroup to Error. Transient
	// query failures return the error without touching state, like
	// Replica.Sync.
	failSync := func(e error) error {
		sg.log.WithError(e).Errorf("Failed to sync shardgroup %s", sg.Name)
		sg.State = types.InstanceStateError
		sg.ErrorMsg = e.Error()
		return e
	}

	if sg.State == types.InstanceStatePending {
		sg.State = types.InstanceStateStopped
		sg.ErrorMsg = ""
		sg.log.Debug("Synced shardgroup")
		return nil
	}

	if sg.State != types.InstanceStateRunning {
		return nil
	}

	subsystemMap, err := GetNvmfSubsystemMap(spdkClient)
	if err != nil {
		return err
	}

	exposedPort, exposedPortErr := getExposedPort(subsystemMap[sg.Nqn])
	if sg.IsExposed {
		if exposedPortErr != nil {
			return failSync(errors.Wrapf(exposedPortErr, "failed to find the actual port in subsystem NQN %s for shardgroup %s, which should be exposed at %d", sg.Nqn, sg.Name, sg.Port))
		}
		if exposedPort != sg.Port {
			return failSync(fmt.Errorf("found mismatching between the actual exposed port %d and the recorded port %d for exposed shardgroup %s", exposedPort, sg.Port, sg.Name))
		}
	} else if exposedPortErr == nil {
		return failSync(fmt.Errorf("found the actual port %d in subsystem NQN %s for shardgroup %s, which should not be exposed", exposedPort, sg.Nqn, sg.Name))
	}

	if err := sg.refreshECSnapshotMapNoLock(spdkClient); err != nil {
		return errors.Wrapf(err, "failed to refresh lvol cache for shardgroup %s", sg.Name)
	}

	sg.ErrorMsg = ""
	sg.log.Debug("Synced shardgroup")
	return nil
}

// SetErrorState marks a non-stopped, non-error shardgroup as Error, mirroring
// Replica.SetErrorState.
func (sg *ShardGroup) SetErrorState() {
	needUpdate := false

	sg.Lock()
	defer func() {
		sg.Unlock()

		if needUpdate {
			sg.UpdateCh <- nil
		}
	}()

	if sg.State != types.InstanceStateStopped && sg.State != types.InstanceStateError {
		sg.State = types.InstanceStateError
		needUpdate = true
	}
}

// Expand grows the EC stack to newSize in place. It resizes bdev_ec, grows
// the lvstore, and then resizes the head lvol.
//
// All k+m shards must already be at the new size. This function does not
// resize them.
//
// creationSize is the volume size when the ShardGroup was created. The
// function rejects a newSize above EcLvstoreMaxGrowthFactor times
// creationSize. A creationSize of zero means unknown and skips this check.
//
// On failure, Expand returns the error and does not change the state. A
// failure before the head lvol resize leaves the ShardGroup serving at the
// old size, and Expand can be called again. A failure during the resize of
// an exposed head lvol leaves the NVMe-oF subsystem down, and the next Sync
// sets Error.
//
// Do not set Error on every failure as Shard.Expand does. An early failure
// would then put a working ShardGroup into Error, and Expand could not be
// called again.
func (sg *ShardGroup) Expand(spdkClient *spdkclient.Client, newSize, creationSize uint64) (err error) {
	sg.Lock()
	prevSpecSize := sg.SpecSize
	defer func() {
		specSizeChanged := sg.SpecSize != prevSpecSize
		sg.Unlock()
		if specSizeChanged {
			sg.UpdateCh <- nil
		}
	}()

	if sg.State != types.InstanceStateRunning {
		return grpcstatus.Errorf(grpccodes.FailedPrecondition, "cannot expand shardgroup %s in state %s", sg.Name, sg.State)
	}

	roundedSize := util.RoundUp(newSize, helpertypes.MiB)
	if roundedSize != newSize {
		return fmt.Errorf("shardgroup %s: new size %d is not a multiple of MiB", sg.Name, newSize)
	}
	if sg.SpecSize > newSize {
		return fmt.Errorf("cannot shrink shardgroup %s from %d to %d", sg.Name, sg.SpecSize, newSize)
	}
	// SpecSize is committed only after the whole chain succeeds, so
	// equality here means the expansion completed.
	if sg.SpecSize == newSize {
		sg.log.Infof("Shardgroup %s already at size %d", sg.Name, newSize)
		return nil
	}

	// Backstop for callers that skipped ExpandPrecheck. By this point the
	// shard lvols were already resized by per-shard ShardExpand calls; this
	// check only keeps the EC bdev, lvstore, and head lvol at their current
	// size. The oversized shard lvols are thin, so the extra allocation is
	// harmless and is reused if a legal-size expansion follows.
	if exceedsInPlaceGrowthCeiling(newSize, creationSize) {
		return grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"shardgroup %s: new size %d exceeds the %dx in-place growth ceiling %d (creation size %d); shard-group rebuild required",
			sg.Name, newSize, spdktypes.EcLvstoreMaxGrowthFactor,
			uint64(spdktypes.EcLvstoreMaxGrowthFactor)*creationSize, creationSize)
	}

	sg.log.Infof("Expanding shardgroup to size %d", newSize)

	// 1. Refresh the cached size of each remote base bdev by resetting its
	// NVMe controller.
	//
	// ShardExpand tears down and re-exposes the shard's NVMe-oF subsystem
	// while resizing the lvol, which drops the AER that would have carried
	// the new size here. The initiator bdev keeps the blockcnt cached at
	// attach time. The reset reconnects, and the SPDK reset-refresh patch
	// (bdev_nvme_reconnect_ctrlr_poll) updates the blockcnt in place. The
	// bdev stays registered, so bdev_ec sees BDEV_EVENT_RESIZE rather than
	// BDEV_EVENT_REMOVE and the slot stays NORMAL.
	//
	// Without the patch nothing is refreshed, BdevEcResize below is a
	// no-op, and the post-grow capacity check rejects the expansion.
	//
	// A shard in this process needs no reset: its lvol resize already
	// delivered BDEV_EVENT_RESIZE to bdev_ec.
	for shardName, endpoint := range sg.Shards {
		if endpoint.LocalShard != nil {
			continue
		}
		controllerName := GetShardLvolName(sg.VolumeName, endpoint.SlotIndex)
		if _, err := spdkClient.BdevNvmeResetController(controllerName); err != nil {
			return errors.Wrapf(err, "failed to reset nvme controller %s for shard %s before EC resize", controllerName, shardName)
		}
	}

	// 2. Resize bdev_ec to pick up the larger base bdevs. A no-op
	// (resized=false) means the EC bdev already matches its base bdevs:
	// a retry, or shards that did not grow. The post-grow capacity check
	// decides which.
	resizeResp, err := spdkClient.BdevEcResize(sg.EcBdevName)
	if err != nil {
		return errors.Wrapf(err, "failed to resize EC bdev %s", sg.EcBdevName)
	}
	if !resizeResp.Resized {
		sg.log.Warnf("EC bdev %s resize was a no-op; resuming a prior expansion or shards did not grow", sg.EcBdevName)
	}

	// 3. Grow the lvstore to fill the resized bdev_ec.
	if _, err := spdkClient.BdevLvolGrowLvstore(sg.LvsName, ""); err != nil {
		return errors.Wrapf(err, "failed to grow lvstore %s", sg.LvsName)
	}

	// The grown lvstore must back the new size before the head lvol grows
	// into it. Failing here leaves the volume exposed and serving at the
	// old size.
	lvstoreList, err := spdkClient.BdevLvolGetLvstore("", sg.LvsUUID)
	if err != nil {
		return errors.Wrapf(err, "failed to query lvstore %s after grow for shardgroup %s", sg.LvsName, sg.Name)
	}
	if len(lvstoreList) != 1 {
		return fmt.Errorf("expected exactly one lvstore for uuid %s, found %d", sg.LvsUUID, len(lvstoreList))
	}
	if !lvstoreUsableFitsSpec(lvstoreList[0].TotalDataClusters, lvstoreList[0].ClusterSize, newSize) {
		return grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"lvstore %s on EC bdev %s can back only %d of %d bytes after grow: "+
				"shards may be under-sized, or the lvstore metadata cannot support this size",
			sg.LvsName, sg.EcBdevName, lvstoreList[0].TotalDataClusters*lvstoreList[0].ClusterSize, newSize)
	}

	// 4. Resize the head lvol. If the head lvol is exposed, its NVMe-oF
	// subsystem is torn down for the resize.
	//
	// The teardown is needed when an engine is attached to the head lvol
	// over NVMe-oF. With the subsystem up, bdev_lvol_resize was seen to
	// hang. After about 60s the keep-alive timers expired, the controller
	// reset, and the SPDK process corrupted its heap. This was seen at the
	// shard layer when the same teardown was removed there. The cause in
	// SPDK has not been confirmed.
	//
	// What the engine sees depends on how it is attached:
	//   - Direct base bdev, the normal case: the head lvol is not exposed,
	//     so there is no teardown. The engine keeps sending I/O during the
	//     resize. SPDK holds new I/O on the lvol while the blob resizes and
	//     then runs it (blob_freeze_io).
	//   - NVMe-oF: the teardown drops the size change event (AER) for the
	//     engine's nvme bdev. The bdev keeps its cached size until the
	//     controller is reset or reconnects.
	if sg.IsExposed {
		if err := spdkClient.StopExposeBdev(sg.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Wrapf(err, "failed to stop exposing shardgroup %s before head-lvol resize", sg.Name)
		}
	}

	resized, err := spdkClient.BdevLvolResize(sg.HeadAlias, util.BytesToMiB(newSize))
	if err != nil {
		return errors.Wrapf(err, "failed to resize head lvol %s", sg.HeadAlias)
	}
	if !resized {
		return fmt.Errorf("head lvol %s was not resized", sg.HeadAlias)
	}

	if sg.IsExposed {
		if err := spdkClient.StartExposeBdev(sg.Nqn, sg.HeadLvolUUID, generateNGUID(sg.HeadLvolName),
			sg.IP, strconv.Itoa(int(sg.Port)), helpertypes.InternalHostNQN); err != nil {
			return errors.Wrapf(err, "failed to re-expose shardgroup %s after head-lvol resize", sg.Name)
		}
	}

	if err := sg.refreshECSnapshotMapNoLock(spdkClient); err != nil {
		return errors.Wrapf(err, "failed to refresh shardgroup snapshot/head cache after expansion for %s", sg.Name)
	}

	// Commit the new size only after the whole chain succeeded. Every
	// step above is idempotent, so a retried Expand resumes the chain
	// after any mid-chain failure.
	sg.SpecSize = newSize

	sg.log.Info("Expanded shardgroup")
	return nil
}

// ExpandPrecheck validates that the ShardGroup's EC stack is in a state where
// expansion can proceed: the target is within the in-place growth ceiling, no
// rebuild in progress, no scrub in progress, all slots NORMAL. Returns
// expansionRequired=true if the new size is larger than the current size and
// preconditions are met.
//
// creationSize is the volume size when the ShardGroup was created; pass 0
// when unknown to skip the ceiling check.
func (sg *ShardGroup) ExpandPrecheck(spdkClient *spdkclient.Client, newSize, creationSize uint64) (expansionRequired bool, err error) {
	sg.RLock()
	defer sg.RUnlock()

	if sg.SpecSize >= newSize {
		return false, nil
	}

	// Gate the ceiling here, before the caller resizes any shard lvol. The
	// matching check in Expand is only a backstop for callers that skip the
	// precheck.
	if exceedsInPlaceGrowthCeiling(newSize, creationSize) {
		return false, grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"shardgroup %s: new size %d exceeds the %dx in-place growth ceiling %d (creation size %d); shard-group rebuild required",
			sg.Name, newSize, spdktypes.EcLvstoreMaxGrowthFactor,
			uint64(spdktypes.EcLvstoreMaxGrowthFactor)*creationSize, creationSize)
	}

	ecInfo, err := sg.getEcBdevInfoNoLock(spdkClient)
	if err != nil {
		return false, err
	}

	if ecInfo.RebuildInProgress {
		return false, fmt.Errorf("shardgroup %s has a rebuild in progress", sg.Name)
	}
	if ecInfo.ReplaceInProgress {
		return false, fmt.Errorf("shardgroup %s has a slot replacement in progress", sg.Name)
	}
	if ecInfo.FailedCount > 0 {
		return false, fmt.Errorf("shardgroup %s has %d failed slot(s); cannot expand while degraded", sg.Name, ecInfo.FailedCount)
	}

	scrubProgress, err := spdkClient.BdevEcGetScrubProgress(sg.EcBdevName)
	if err != nil {
		return false, errors.Wrapf(err, "failed to query scrub progress for shardgroup %s", sg.Name)
	}
	if scrubProgress != nil {
		return false, fmt.Errorf("shardgroup %s has a scrub in progress", sg.Name)
	}

	return true, nil
}

// SnapshotCreate snapshots the head lvol under the given user-visible name.
// Returns the new snapshot's UUID.
func (sg *ShardGroup) SnapshotCreate(spdkClient *spdkclient.Client, snapshotName string) (snapshotUUID string, err error) {
	sg.Lock()
	updateRequired := false
	defer func() {
		sg.Unlock()
		if updateRequired {
			sg.UpdateCh <- nil
		}
	}()

	if sg.State != types.InstanceStateRunning {
		return "", grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shardgroup %s snapshot create", sg.State, sg.Name)
	}

	uuid, err := spdkClient.BdevLvolSnapshot(sg.HeadAlias, snapshotName, nil)
	if err != nil {
		return "", errors.Wrapf(err, "failed to create snapshot %s on shardgroup %s", snapshotName, sg.Name)
	}
	updateRequired = true

	if err := sg.refreshECSnapshotMapNoLock(spdkClient); err != nil {
		return "", errors.Wrapf(err, "failed to refresh shardgroup snapshot/head cache after snapshot create for %s", sg.Name)
	}

	sg.log.Infof("Created snapshot %s (uuid=%s)", snapshotName, uuid)
	return uuid, nil
}

// SnapshotDelete deletes the named snapshot from the lvstore. Idempotent for
// missing snapshots.
func (sg *ShardGroup) SnapshotDelete(spdkClient *spdkclient.Client, snapshotName string) error {
	sg.Lock()
	updateRequired := false
	defer func() {
		sg.Unlock()
		if updateRequired {
			sg.UpdateCh <- nil
		}
	}()

	if sg.State != types.InstanceStateRunning {
		return grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shardgroup %s snapshot delete", sg.State, sg.Name)
	}

	snapAlias := spdktypes.GetLvolAlias(sg.LvsName, snapshotName)
	deleted, err := spdkClient.BdevLvolDelete(snapAlias)
	if err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to delete snapshot %s from shardgroup %s", snapshotName, sg.Name)
	}
	updateRequired = deleted

	if err := sg.refreshECSnapshotMapNoLock(spdkClient); err != nil {
		return errors.Wrapf(err, "failed to refresh shardgroup snapshot/head cache after snapshot delete for %s", sg.Name)
	}

	sg.log.Infof("Deleted snapshot %s", snapshotName)
	return nil
}

// SnapshotRevert replaces the head lvol with a fresh clone of the named
// snapshot. The caller (Volume controller) must ensure the engine has
// disconnected from this ShardGroup's NVMe-oF endpoint before invoking
// (FrontendEmpty guard at the engine layer).
//
// Sequence: unexpose head -> delete head lvol -> clone from snapshot ->
// re-expose with the new head lvol UUID.
//
// The new head lvol is exposed only if the old one was. An unexposed head
// lvol means that an engine in this SPDK process uses it directly. After
// the revert, the engine adds the new head lvol to its raid bdev, and a
// raid bdev cannot use an exposed lvol.
func (sg *ShardGroup) SnapshotRevert(spdkClient *spdkclient.Client, snapshotName string) error {
	sg.Lock()
	prevHeadLvolUUID := sg.HeadLvolUUID
	defer func() {
		updateRequired := sg.HeadLvolUUID != prevHeadLvolUUID
		sg.Unlock()
		if updateRequired {
			sg.UpdateCh <- nil
		}
	}()

	if sg.State != types.InstanceStateRunning {
		return grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shardgroup %s snapshot revert", sg.State, sg.Name)
	}

	// Validate the snapshot against live SPDK state before tearing anything
	// down: the head lvol is deleted mid-sequence, so a bad snapshot name
	// must not destroy it with nothing left to clone from.
	if err := sg.refreshECSnapshotMapNoLock(spdkClient); err != nil {
		return errors.Wrapf(err, "failed to refresh shardgroup snapshot/head cache before snapshot revert for %s", sg.Name)
	}
	if sg.SnapshotMap[snapshotName] == nil {
		return grpcstatus.Errorf(grpccodes.NotFound, "cannot revert shardgroup %s to non-existing snapshot %s", sg.Name, snapshotName)
	}

	sg.log.Infof("Reverting shardgroup to snapshot %s", snapshotName)

	if sg.IsExposed {
		// Do not clear IsExposed here. It records that the head lvol
		// should be exposed. If the re-expose below fails, Sync sees
		// the missing endpoint and moves the ShardGroup to Error.
		if err := spdkClient.StopExposeBdev(sg.Nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return errors.Wrapf(err, "failed to stop exposing shardgroup %s before revert", sg.Name)
		}
	}

	if _, err := spdkClient.BdevLvolDelete(sg.HeadAlias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to delete head lvol %s during revert", sg.HeadAlias)
	}
	sg.HeadLvolUUID = ""

	snapAlias := spdktypes.GetLvolAlias(sg.LvsName, snapshotName)
	newHeadUUID, err := spdkClient.BdevLvolClone(snapAlias, sg.HeadLvolName)
	if err != nil {
		return errors.Wrapf(err, "failed to clone snapshot %s as new head", snapshotName)
	}
	sg.HeadLvolUUID = newHeadUUID

	if sg.IsExposed {
		if err := spdkClient.StartExposeBdev(sg.Nqn, sg.HeadLvolUUID, generateNGUID(sg.HeadLvolName),
			sg.IP, strconv.Itoa(int(sg.Port)), helpertypes.InternalHostNQN); err != nil {
			return errors.Wrapf(err, "failed to re-expose head lvol after revert")
		}
	}

	if err := sg.refreshECSnapshotMapNoLock(spdkClient); err != nil {
		return errors.Wrapf(err, "failed to refresh shardgroup snapshot/head cache after snapshot revert for %s", sg.Name)
	}

	sg.log.Infof("Reverted to snapshot %s (new head uuid=%s)", snapshotName, sg.HeadLvolUUID)
	return nil
}

// SnapshotPurge deletes orphan snapshots (snapshots with no clones) from this
// ShardGroup's lvstore. Snapshots that still have child clones are preserved.
// User-vs-system distinction is left to a future refinement.
func (sg *ShardGroup) SnapshotPurge(spdkClient *spdkclient.Client) error {
	sg.Lock()
	updateRequired := false
	defer func() {
		sg.Unlock()
		if updateRequired {
			sg.UpdateCh <- nil
		}
	}()

	if sg.State != types.InstanceStateRunning {
		return grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shardgroup %s snapshot purge", sg.State, sg.Name)
	}

	filter := func(b *spdktypes.BdevInfo) bool {
		if b.DriverSpecific == nil || b.DriverSpecific.Lvol == nil {
			return false
		}
		return b.DriverSpecific.Lvol.LvolStoreUUID == sg.LvsUUID
	}
	bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, filter)
	if err != nil {
		return errors.Wrapf(err, "failed to list lvols for shardgroup %s", sg.Name)
	}

	var purged int
	for lvolName, bdev := range bdevLvolMap {
		lvol := bdev.DriverSpecific.Lvol
		if !lvol.Snapshot || len(lvol.Clones) > 0 {
			continue
		}
		snapAlias := spdktypes.GetLvolAlias(sg.LvsName, lvolName)
		if _, err := spdkClient.BdevLvolDelete(snapAlias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			sg.log.WithError(err).Warnf("Failed to purge snapshot %s; continuing", lvolName)
			continue
		}
		sg.log.Infof("Purged orphan snapshot %s", lvolName)
		purged++
	}
	updateRequired = purged > 0

	if err := sg.refreshECSnapshotMapNoLock(spdkClient); err != nil {
		return errors.Wrapf(err, "failed to refresh shardgroup snapshot/head cache after snapshot purge for %s", sg.Name)
	}

	sg.log.Infof("Snapshot purge complete: deleted %d orphan snapshot(s)", purged)
	return nil
}

// ShardReplace replaces the shard in a FAILED slot with a new shard.
//
// The slot moves from FAILED to REPLACING, and the new shard starts
// receiving writes immediately. It does not yet contain the data written
// before the failure. Call ShardRebuildStart afterwards to rebuild that data
// from the remaining shards.
//
// Parameters:
//   - shardName: the Shard CR name, <volumeName>-<slotIndex>. It identifies
//     the slot.
//   - newAddress: the NVMe-oF address of the new shard.
//   - newLocalShard: the new shard if it runs in this SPDK process, otherwise
//     nil. A local shard is attached directly, not through newAddress.
func (sg *ShardGroup) ShardReplace(spdkClient *spdkclient.Client, shardName, newAddress string, newLocalShard *Shard) (slotState string, err error) {
	sg.Lock()
	defer sg.Unlock()

	if sg.State != types.InstanceStateRunning {
		return "", grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shardgroup %s shard replace", sg.State, sg.Name)
	}

	currentEndpoint, ok := sg.Shards[shardName]
	if !ok {
		return "", grpcstatus.Errorf(grpccodes.NotFound, "shard %s not found in shardgroup %s", shardName, sg.Name)
	}

	// Remote shard: detach the NVMe-oF controller that backs the FAILED
	// slot. The controller is usually already gone. A failure is logged and
	// does not stop the replace.
	//
	// Local shard: there is no controller to detach. The shard is exposed
	// again after the replace succeeds.
	if currentEndpoint.LocalShard == nil {
		if err := sg.detachShardBaseBdev(spdkClient, currentEndpoint); err != nil {
			sg.log.WithError(err).Warnf("Pre-replace detach of shard %s failed; continuing", shardName)
		}
	}

	// Attach the replacement shard as a base bdev.
	replacementEndpoint := &ShardEndpoint{Address: newAddress, SlotIndex: currentEndpoint.SlotIndex, LocalShard: newLocalShard}
	bdevName, err := sg.attachShardBaseBdev(spdkClient, replacementEndpoint, maxRetries)
	if err != nil {
		return "", errors.Wrapf(err, "failed to attach replacement for shard %s", shardName)
	}

	// A slot can be replaced by the local shard it already uses. That shard
	// is not exposed afterwards: on success bdev_ec uses it again, and on
	// failure bdev_ec may still have it open.
	sameLocalShard := currentEndpoint.LocalShard != nil && currentEndpoint.LocalShard == replacementEndpoint.LocalShard

	// Point the EC slot at the new base bdev.
	resp, err := spdkClient.BdevEcReplaceBaseBdev(sg.EcBdevName, replacementEndpoint.SlotIndex, bdevName)
	if err != nil {
		if replacementEndpoint.LocalShard != nil && !sameLocalShard {
			if exposeErr := replacementEndpoint.LocalShard.StartExposeAfterLocalDetach(spdkClient); exposeErr != nil {
				sg.log.WithError(exposeErr).Warnf("Failed to expose replacement shard %s again after a failed replace", shardName)
			}
		}
		return "", errors.Wrapf(err, "failed to replace base bdev at slot %d for shardgroup %s", replacementEndpoint.SlotIndex, sg.Name)
	}

	// Update sg.Shards last. If attach or replace fails, the record still
	// matches the shard bdev_ec is serving.
	sg.Shards[shardName] = replacementEndpoint

	// The old shard was local and the slot now uses a different shard.
	// bdev_ec has closed the old shard, so expose it again.
	if currentEndpoint.LocalShard != nil && !sameLocalShard {
		if exposeErr := currentEndpoint.LocalShard.StartExposeAfterLocalDetach(spdkClient); exposeErr != nil {
			sg.log.WithError(exposeErr).Warnf("Failed to expose replaced local shard %s again", shardName)
		}
	}

	sg.log.Infof("Replaced shard %s at slot %d; the slot now uses base bdev %s and is in state %s", shardName, replacementEndpoint.SlotIndex, bdevName, resp.State)
	return string(resp.State), nil
}

// Poll bounds for ForceFailShard. The slot is marked FAILED immediately;
// the poll waits for bdev_ec to finish releasing the old base bdev.
const (
	ecShardForceFailPollInterval = 100 * time.Millisecond
	ecShardForceFailPollTimeout  = 5 * time.Second
)

// ForceFailShard marks a shard's slot as FAILED immediately, without waiting
// for bdev_nvme's ctrlr_loss_timeout_sec to expire. The manager calls it when
// it already knows the shard is gone for good (Shard CR deleted, node
// evicted), so that a replacement can follow.
//
// A remote shard is failed by detaching its NVMe controller. A shard in this
// process has no controller, so bdev_ec is told to fail the slot directly;
// the shard lvol itself is left untouched.
//
// Returns once the slot is FAILED and bdev_ec has released the old base bdev.
// A ShardReplace issued before that point gets EBUSY.
//
// The call is safe to retry. A reconcile loop may repeat it, or a stale
// request may arrive after the slot has already been replaced:
//   - Slot already FAILED: there is nothing to fail. The call only waits
//     for bdev_ec to finish releasing the old base bdev, if it has not
//     yet, and then returns the current state.
//   - Slot REPLACING: rejected with FailedPrecondition. A rebuild is in
//     flight and failing the slot again would discard its progress.
//   - Unknown shard name: NotFound.
//
// Failure accounting (failed_count, dirty-region bookkeeping, degraded-mode
// gating) is NOT touched here; it flows through the standard BDEV_EVENT_REMOVE
// path that bdev_ec already handles for unintentional failures.
func (sg *ShardGroup) ForceFailShard(spdkClient *spdkclient.Client, shardName string) (slotState string, err error) {
	sg.Lock()
	defer sg.Unlock()

	if sg.State != types.InstanceStateRunning {
		return "", grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shardgroup %s shard force-fail", sg.State, sg.Name)
	}

	endpoint, ok := sg.Shards[shardName]
	if !ok {
		return "", grpcstatus.Errorf(grpccodes.NotFound, "shard %s not found in shardgroup %s", shardName, sg.Name)
	}

	current, err := sg.readSlotNoLock(spdkClient, endpoint.SlotIndex)
	if err != nil {
		return "", err
	}
	switch current.State {
	case spdktypes.BdevEcSlotStateFailed:
		if !current.CleanupPending {
			return string(current.State), nil
		}
		sg.log.Infof("Slot %d of shard %s is already FAILED; waiting for bdev_ec to release the old base bdev", endpoint.SlotIndex, shardName)
	case spdktypes.BdevEcSlotStateReplacing:
		return "", grpcstatus.Errorf(grpccodes.FailedPrecondition,
			"shard %s in shardgroup %s is REPLACING; refuse to force-fail", shardName, sg.Name)
	default:
		if endpoint.LocalShard != nil {
			sg.log.Infof("Failing slot %d directly through bdev_ec because shard %s is local", endpoint.SlotIndex, shardName)
			if err := shardGroupFailEcBaseBdev(spdkClient, sg.EcBdevName, endpoint.SlotIndex); err != nil {
				return "", errors.Wrapf(err, "failed to fail slot %d for local shard %s", endpoint.SlotIndex, shardName)
			}
		} else {
			sg.log.Infof("Failing slot %d by detaching the NVMe controller of remote shard %s", endpoint.SlotIndex, shardName)
			if err := sg.detachShardBaseBdev(spdkClient, endpoint); err != nil {
				return "", errors.Wrapf(err, "failed to detach remote shard %s", shardName)
			}
		}
	}

	deadline := time.Now().Add(ecShardForceFailPollTimeout)
	for {
		observed, err := sg.readSlotNoLock(spdkClient, endpoint.SlotIndex)
		if err != nil {
			return "", err
		}
		if observed.State == spdktypes.BdevEcSlotStateFailed && !observed.CleanupPending {
			sg.log.Infof("Force-failed shard %s at slot %d", shardName, endpoint.SlotIndex)
			return string(observed.State), nil
		}
		if time.Now().After(deadline) {
			return "", fmt.Errorf("timed out after %s waiting for shard %s at slot %d to fail: state=%s cleanupPending=%v",
				ecShardForceFailPollTimeout, shardName, endpoint.SlotIndex, observed.State, observed.CleanupPending)
		}
		time.Sleep(ecShardForceFailPollInterval)
	}
}

// readSlotNoLock returns the EC slot record at slotIndex. The caller must
// hold sg's lock.
func (sg *ShardGroup) readSlotNoLock(spdkClient *spdkclient.Client, slotIndex uint32) (spdktypes.EcBaseBdev, error) {
	ecInfo, err := sg.getEcBdevInfoNoLock(spdkClient)
	if err != nil {
		return spdktypes.EcBaseBdev{}, err
	}
	for _, base := range ecInfo.BaseBdevs {
		if base.Slot == slotIndex {
			return base, nil
		}
	}
	return spdktypes.EcBaseBdev{}, fmt.Errorf("slot %d not found in EC bdev %s for shardgroup %s", slotIndex, sg.EcBdevName, sg.Name)
}

// ShardRebuildStart starts the background rebuild poller for all REPLACING
// slots. Returns the total stripe count and the first slot being rebuilt for
// observability.
func (sg *ShardGroup) ShardRebuildStart(spdkClient *spdkclient.Client) (numStripes uint64, firstSlot uint32, err error) {
	sg.Lock()
	defer sg.Unlock()

	if sg.State != types.InstanceStateRunning {
		return 0, 0, grpcstatus.Errorf(grpccodes.FailedPrecondition, "invalid state %s for shardgroup %s rebuild start", sg.State, sg.Name)
	}

	resp, err := spdkClient.BdevEcStartRebuild(sg.EcBdevName)
	if err != nil {
		return 0, 0, errors.Wrapf(err, "failed to start rebuild on shardgroup %s", sg.Name)
	}
	sg.log.Infof("Started rebuild for shardgroup %s: numStripes=%d firstSlot=%d", sg.Name, resp.NumStripes, resp.FirstSlot)
	return resp.NumStripes, resp.FirstSlot, nil
}

func (sg *ShardGroup) ShardRebuildProgress(spdkClient *spdkclient.Client) (*spdktypes.BdevEcRebuildProgress, error) {
	sg.RLock()
	defer sg.RUnlock()

	progress, err := spdkClient.BdevEcGetRebuildProgress(sg.EcBdevName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get rebuild progress for shardgroup %s", sg.Name)
	}
	return &progress, nil
}

// ShardRebuildStop returns an error if no rebuild is in progress (-ENOENT from SPDK).
func (sg *ShardGroup) ShardRebuildStop(spdkClient *spdkclient.Client) error {
	sg.Lock()
	defer sg.Unlock()

	if _, err := spdkClient.BdevEcStopRebuild(sg.EcBdevName); err != nil {
		return errors.Wrapf(err, "failed to stop rebuild on shardgroup %s", sg.Name)
	}
	sg.log.Info("Stopped rebuild")
	return nil
}

// ShardRebuildQosSet sets the rebuild rate limit. maxStripesPerSec=0 means
// unlimited. paused=true suspends the rebuild poller without cancelling.
// Applied immediately to any in-progress rebuild.
func (sg *ShardGroup) ShardRebuildQosSet(spdkClient *spdkclient.Client, maxStripesPerSec uint32, paused bool) error {
	sg.Lock()
	defer sg.Unlock()

	if _, err := spdkClient.BdevEcSetRebuildQos(sg.EcBdevName, maxStripesPerSec, paused); err != nil {
		return errors.Wrapf(err, "failed to set rebuild QoS on shardgroup %s", sg.Name)
	}
	sg.log.Infof("Set rebuild QoS: maxStripesPerSec=%d paused=%v", maxStripesPerSec, paused)
	return nil
}

// tryDiscoverExistingLvstore queries SPDK for an already-imported per-volume
// lvstore/head pair after bdev_ec_create.
//
// Returns (true, nil) when both lvstore and head are found and cached on sg.
// Returns (false, nil) when no lvstore exists yet (fresh volume path).
// Returns error for malformed/mismatched existing state or RPC failures.
func (sg *ShardGroup) tryDiscoverExistingLvstore(spdkClient *spdkclient.Client) (bool, error) {
	lvstoreList, err := spdkClient.BdevLvolGetLvstore(sg.LvsName, "")
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return false, nil
		}
		return false, errors.Wrapf(err, "failed to discover lvstore %s on EC bdev %s", sg.LvsName, sg.EcBdevName)
	}
	if len(lvstoreList) == 0 {
		return false, nil
	}
	if len(lvstoreList) != 1 {
		return false, fmt.Errorf("expected exactly one lvstore named %s, found %d", sg.LvsName, len(lvstoreList))
	}
	sg.LvsUUID = lvstoreList[0].UUID
	sg.ClusterSize = lvstoreList[0].ClusterSize

	headBdev, err := spdkClient.BdevLvolGetByName(sg.HeadAlias, 0)
	if err != nil {
		if jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return false, fmt.Errorf("found lvstore %s but missing head lvol %s", sg.LvsName, sg.HeadAlias)
		}
		return false, errors.Wrapf(err, "failed to discover head lvol %s after auto-import", sg.HeadAlias)
	}
	sg.HeadLvolUUID = headBdev.UUID

	// The discovered head is the only truthful resume point for an
	// interrupted expansion; see discoveredSpecSize.
	if headSize := headBdev.NumBlocks * uint64(headBdev.BlockSize); discoveredSpecSize(headSize, sg.SpecSize) != sg.SpecSize {
		sg.log.Warnf("Discovered head lvol %s size %d bytes is below the requested spec size %d bytes; "+
			"resuming from the head size so the pending expansion re-runs",
			sg.HeadAlias, headSize, sg.SpecSize)
		sg.SpecSize = headSize
	}

	// An existing lvstore may be too small to back the spec size. Warn
	// instead of failing so the data stays accessible.
	if !lvstoreUsableFitsSpec(lvstoreList[0].TotalDataClusters, lvstoreList[0].ClusterSize, sg.SpecSize) {
		sg.log.Warnf("Discovered lvstore %s can back only %d of %d bytes; "+
			"volume may hit ENOSPC when fully written",
			sg.LvsName, lvstoreList[0].TotalDataClusters*lvstoreList[0].ClusterSize, sg.SpecSize)
	}

	sg.log.Infof("Discovered existing lvstore %s (uuid=%s) and head lvol %s (uuid=%s)",
		sg.LvsName, sg.LvsUUID, sg.HeadLvolName, sg.HeadLvolUUID)
	return true, nil
}

// prepareIPAndPort sets the ShardGroup's IP and allocates its single NVMe-oF port
// (for the head lvol). A ShardGroup serves one target, so it always uses one port
// and does not use the request's port_count.
func (sg *ShardGroup) prepareIPAndPort(superiorPortAllocator *commonbitmap.Bitmap) error {
	// nolint: staticcheck // Keep legacy IP selection until callers migrate to the new helpers.
	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return err
	}
	sg.IP = podIP

	port, _, err := superiorPortAllocator.AllocateRange(1)
	if err != nil {
		return err
	}
	sg.Port = port

	sg.log.Infof("Prepared IP %s and port %d for shardgroup", sg.IP, sg.Port)
	return nil
}
