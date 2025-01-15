package spdk

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/backupstore"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	btypes "github.com/longhorn/backupstore/types"
	butil "github.com/longhorn/backupstore/util"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
	commonutils "github.com/longhorn/go-common-libs/utils"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"

	safelog "github.com/longhorn/longhorn-spdk-engine/pkg/log"
)

const (
	restorePeriodicRefreshInterval = 2 * time.Second
)

type Replica struct {
	sync.RWMutex

	ctx context.Context

	// Head should be the only writable lvol in the regular Replica lvol chain/map.
	// And it is the last entry of ActiveChain if it is not nil.
	Head *Lvol
	// ActiveChain stores the backing image info in index 0.
	// If a replica does not contain a backing image, the first entry will be nil.
	// The last entry of the chain should be the head lvol if it exists.
	ActiveChain []*Lvol
	// SnapshotLvolMap map[<snapshot lvol name>]. <snapshot lvol name> consists of `<replica name>-snap-<snapshot name>`
	SnapshotLvolMap map[string]*Lvol
	BackingImage    *Lvol

	Name       string
	Alias      string
	LvsName    string
	LvsUUID    string
	SpecSize   uint64
	ActualSize uint64
	IP         string
	PortStart  int32
	PortEnd    int32

	State    types.InstanceState
	ErrorMsg string

	IsExposed               bool
	SnapshotChecksumEnabled bool

	// reconstructRequired will be set to true when stopping an errored replica
	reconstructRequired bool

	// The rebuilding destination replica should cache this info
	isRebuilding       bool
	rebuildingDstCache RebuildingDstCache

	// The rebuilding source replica should cache this info
	rebuildingSrcCache RebuildingSrcCache

	isRestoring bool
	restore     *Restore

	portAllocator *commonbitmap.Bitmap
	// UpdateCh should not be protected by the replica lock
	UpdateCh chan interface{}

	log *safelog.SafeLogger

	// TODO: Record error message
}

type RebuildingDstCache struct {
	rebuildingLvol        *Lvol
	rebuildingPort        int32
	rebuildingLvolAddress string

	srcReplicaName           string
	srcReplicaAddress        string
	externalSnapshotName     string
	externalSnapshotBdevName string

	// rebuildingSnapshotMap is map[<snapshot name>]
	rebuildingSnapshotMap map[string]*api.Lvol
	rebuildingSize        uint64
	rebuildingError       string
	rebuildingState       string

	processedSnapshotList  []string
	processedSnapshotsSize uint64

	processingSnapshotName string
	processingState        string
	processingSize         uint64
}

type RebuildingSrcCache struct {
	dstReplicaName string
	// dstRebuildingBdev is the result of attaching the rebuilding lvol exposed by the dst replica
	dstRebuildingBdevName string

	exposedSnapshotAlias string
	exposedSnapshotPort  int32

	shallowCopySnapshotName string
	shallowCopyOpID         uint32
	shallowCopyStatus       spdktypes.ShallowCopyStatus
}

func ServiceReplicaToProtoReplica(r *Replica) *spdkrpc.Replica {
	res := &spdkrpc.Replica{
		Name:      r.Name,
		LvsName:   r.LvsName,
		LvsUuid:   r.LvsUUID,
		SpecSize:  r.SpecSize,
		Snapshots: map[string]*spdkrpc.Lvol{},
		Ip:        r.IP,
		PortStart: r.PortStart,
		PortEnd:   r.PortEnd,
		State:     string(r.State),
		ErrorMsg:  r.ErrorMsg,
	}

	res.Head = ServiceLvolToProtoLvol(r.Name, r.Head)
	// spdkrpc.Replica.Snapshots is map[<snapshot name>] rather than map[<snapshot lvol name>]
	for lvolName, lvol := range r.SnapshotLvolMap {
		res.Snapshots[GetSnapshotNameFromReplicaSnapshotLvolName(r.Name, lvolName)] = ServiceLvolToProtoLvol(r.Name, lvol)
	}

	if r.BackingImage != nil {
		backingImageName, _, err := ExtractBackingImageAndDiskUUID(r.BackingImage.Name)
		if err != nil {
			// The BackingImageName will be "" when getting the result from grpc if there is an error.
			// We handle the empty backing image name in the caller.
			// This field is currently only used when engine updating info from replicas or rebuilding the replica.
			r.log.WithError(err).Warnf("Failed to extract backing image name from %v", r.BackingImage.Name)
		}
		res.BackingImageName = backingImageName
	}

	return res
}

func NewReplica(ctx context.Context, replicaName, lvsName, lvsUUID string, specSize, actualSize uint64, updateCh chan interface{}) *Replica {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"replicaName": replicaName,
		"lvsName":     lvsName,
		"lvsUUID":     lvsUUID,
	})

	roundedSpecSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSpecSize != specSize {
		log.Infof("Rounded up spec size from %v to %v since the specSize should be multiple of MiB", specSize, roundedSpecSize)
	}
	log.WithField("specSize", roundedSpecSize)

	return &Replica{
		ctx: ctx,

		Head: nil,
		ActiveChain: []*Lvol{
			nil,
		},
		SnapshotLvolMap: map[string]*Lvol{},
		Name:            replicaName,
		Alias:           spdktypes.GetLvolAlias(lvsName, replicaName),
		LvsName:         lvsName,
		LvsUUID:         lvsUUID,
		SpecSize:        roundedSpecSize,
		State:           types.InstanceStatePending,

		SnapshotChecksumEnabled: true,

		rebuildingDstCache: RebuildingDstCache{
			rebuildingSnapshotMap: map[string]*api.Lvol{},
			processedSnapshotList: []string{},
		},
		rebuildingSrcCache: RebuildingSrcCache{},

		restore: &Restore{},

		UpdateCh: updateCh,

		log: safelog.NewSafeLogger(log),
	}
}

func (r *Replica) GetAddress() string {
	r.RLock()
	defer r.RUnlock()
	return net.JoinHostPort(r.IP, strconv.Itoa(int(r.PortStart)))
}

func (r *Replica) IsRebuilding() bool {
	r.RLock()
	defer r.RUnlock()
	return r.State == types.InstanceStateRunning && r.isRebuilding
}

func (r *Replica) replicaLvolFilter(bdev *spdktypes.BdevInfo) bool {
	if bdev == nil || len(bdev.Aliases) < 1 || bdev.DriverSpecific.Lvol == nil {
		return false
	}
	lvolName := spdktypes.GetLvolNameFromAlias(bdev.Aliases[0])
	// it is okay to have backing image snapshot in the results, because we exclude it when finding root or construct the snapshot map
	return IsReplicaLvol(r.Name, lvolName) || IsBackingImageSnapLvolName(lvolName)
}

func (r *Replica) Sync(spdkClient *spdkclient.Client) (err error) {
	r.Lock()
	defer r.Unlock()
	// It's better to let the server send the update signal

	// This lvol and nvmf subsystem fetch should be protected by replica lock, in case of snapshot operations happened during the sync-up.
	bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
	if err != nil {
		return err
	}

	if r.SnapshotChecksumEnabled {
		for _, bdevLvol := range bdevLvolMap {
			if !bdevLvol.DriverSpecific.Lvol.Snapshot {
				continue
			}
			if bdevLvol.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotChecksum] != "" {
				continue
			}
			// TODO: Use a goroutine pool
			go func() {
				logrus.Debugf("Replica %v is registering checksum for snapshot %v", r.Name, bdevLvol.Aliases[0])
				_, err := spdkClient.BdevLvolRegisterSnapshotChecksum(bdevLvol.Aliases[0])
				if err != nil {
					logrus.Errorf("Replica %v failed to register checksum for snapshot %v: %v", r.Name, bdevLvol.Name, err)
				}
			}()
		}
	}

	if r.State == types.InstanceStatePending {
		return r.construct(bdevLvolMap)
	}

	subsystemMap, err := GetNvmfSubsystemMap(spdkClient)
	if err != nil {
		return err
	}

	return r.validateAndUpdate(bdevLvolMap, subsystemMap)
}

// construct build Replica with the SnapshotLvolMap and SnapshotChain from the bdev lvol list.
// This function is typically invoked for the existing lvols after node/service restart and device add.
func (r *Replica) construct(bdevLvolMap map[string]*spdktypes.BdevInfo) (err error) {
	defer func() {
		if err != nil {
			r.State = types.InstanceStateError
			r.ErrorMsg = err.Error()
		} else {
			if r.State != types.InstanceStateError {
				r.ErrorMsg = ""
			}
		}
	}()

	switch r.State {
	case types.InstanceStatePending:
		break
	case types.InstanceStateStopped:
		if r.reconstructRequired {
			break
		}
		return fmt.Errorf("invalid state %s for reconstructing required flag %v for replica %s construct", r.State, r.reconstructRequired, r.Name)
	case types.InstanceStateRunning:
		if r.isRebuilding {
			break
		}
		fallthrough
	default:
		return fmt.Errorf("invalid state %s with rebuilding %v for replica %s construct", r.State, r.isRebuilding, r.Name)
	}

	if err := r.validateReplicaHead(bdevLvolMap[r.Name]); err != nil {
		return err
	}

	newSnapshotLvolMap, err := constructSnapshotLvolMap(r.Name, bdevLvolMap)
	if err != nil {
		return err
	}
	newChain, err := constructActiveChainFromSnapshotLvolMap(r.Name, newSnapshotLvolMap, bdevLvolMap)
	if err != nil {
		return err
	}

	r.Head = newChain[len(newChain)-1]
	r.ActiveChain = newChain
	r.SnapshotLvolMap = newSnapshotLvolMap
	r.BackingImage = newChain[0]
	r.reconstructRequired = false

	if r.State == types.InstanceStatePending {
		r.State = types.InstanceStateStopped
	}

	return nil
}

func (r *Replica) validateAndUpdate(bdevLvolMap map[string]*spdktypes.BdevInfo, subsystemMap map[string]*spdktypes.NvmfSubsystem) (err error) {
	defer func() {
		if err != nil {
			if r.State != types.InstanceStateError {
				r.State = types.InstanceStateError
				r.log.WithError(err).Error("Found error during validation and update")
			}
			r.ErrorMsg = err.Error()
		} else {
			if r.State != types.InstanceStateError {
				r.ErrorMsg = ""
			}
		}
	}()

	// Stop syncing with the SPDK TGT server if the replica does not contain any valid SPDK components.
	if r.State != types.InstanceStateRunning {
		return nil
	}

	// Should not sync a rebuilding destination replica since the snapshot map as well as the active chain is not ready.
	if r.isRebuilding {
		return nil
	}

	if err := r.validateReplicaHead(bdevLvolMap[r.Name]); err != nil {
		return err
	}

	newSnapshotLvolMap, err := constructSnapshotLvolMap(r.Name, bdevLvolMap)
	if err != nil {
		return err
	}
	if len(r.SnapshotLvolMap) != len(newSnapshotLvolMap) {
		return fmt.Errorf("replica current active snapshot lvol map length %d is not the same as the latest snapshot lvol map length %d", len(r.SnapshotLvolMap), len(newSnapshotLvolMap))
	}
	for snapshotLvolName := range r.SnapshotLvolMap {
		if err := compareSvcLvols(r.SnapshotLvolMap[snapshotLvolName], newSnapshotLvolMap[snapshotLvolName], true, true); err != nil {
			return err
		}
	}

	newChain, err := constructActiveChainFromSnapshotLvolMap(r.Name, newSnapshotLvolMap, bdevLvolMap)
	if err != nil {
		return err
	}

	if len(r.ActiveChain) != len(newChain) {
		return fmt.Errorf("replica current active chain length %d is not the same as the latest chain length %d", len(r.ActiveChain), len(newChain))
	}

	for idx, svcLvol := range r.ActiveChain {
		newSvcLvol := newChain[idx]
		// Handle nil backing image separately
		if idx == 0 {
			if svcLvol == nil && newSvcLvol == nil {
				continue
			}
			if svcLvol != nil && newSvcLvol == nil {
				return fmt.Errorf("replica current backing image is %v while the latest chain contains a nil backing image", svcLvol.Name)
			}
			if svcLvol == nil && newSvcLvol != nil {
				return fmt.Errorf("replica current backing image is nil while the latest chain contains backing image %v", newSvcLvol.Name)
			}
			// no need to compare the backing image
			continue
		}

		if err := compareSvcLvols(svcLvol, newSvcLvol, true, svcLvol.Name != r.Name); err != nil {
			return err
		}
		// Then update the actual size for the head lvol
		if svcLvol.Name == r.Name {
			svcLvol.ActualSize = newSvcLvol.ActualSize
		}
	}

	replicaActualSize := newChain[len(newChain)-1].ActualSize
	for _, snapLvol := range newSnapshotLvolMap {
		replicaActualSize += snapLvol.ActualSize
	}
	r.ActualSize = replicaActualSize

	if r.State == types.InstanceStateRunning {
		if r.IP == "" {
			return fmt.Errorf("found invalid IP %s for replica %s", r.IP, r.Name)
		}
		if r.PortStart == 0 || r.PortEnd == 0 || r.PortStart > r.PortEnd {
			return fmt.Errorf("found invalid Ports [%d, %d] for the running replica %s", r.PortStart, r.PortEnd, r.Name)
		}
	}

	// In case of a stopped replica being wrongly exposed, this function will check the exposing state anyway.
	if r.isRestoring {
		r.log.Info("Replica is being restored, skip the exposing state check")
		return nil
	}

	nqn := helpertypes.GetNQN(r.Name)
	exposedPort, exposedPortErr := getExposedPort(subsystemMap[nqn])
	if r.IsExposed {
		if exposedPortErr != nil {
			return errors.Wrapf(err, "failed to find the actual port in subsystem NQN %s for replica %s, which should be exposed at %d", nqn, r.Name, r.PortStart)
		}
		if exposedPort != r.PortStart {
			return fmt.Errorf("found mismatching between the actual exposed port %d and the recorded port %d for exposed replica %s", exposedPort, r.PortStart, r.Name)
		}
	} else {
		if exposedPortErr == nil {
			return fmt.Errorf("found the actual port %d in subsystem NQN %s for replica %s, which should not be exposed", exposedPort, nqn, r.Name)
		}
	}

	return nil
}

func compareSvcLvols(prev, cur *Lvol, checkChildren, checkActualSize bool) error {
	if prev == nil && cur == nil {
		return nil
	}
	if prev == nil {
		return fmt.Errorf("cannot find the corresponding prev lvol")
	}
	if cur == nil {
		return fmt.Errorf("cannot find the corresponding cur lvol")
	}
	if prev.Name != cur.Name || prev.UUID != cur.UUID || prev.SnapshotTimestamp != cur.SnapshotTimestamp || prev.SpecSize != cur.SpecSize || prev.Parent != cur.Parent || len(prev.Children) != len(cur.Children) {
		return fmt.Errorf("found mismatching lvol %+v with recorded prev lvol %+v", cur, prev)
	}
	if checkChildren {
		for childName := range prev.Children {
			if cur.Children[childName] == nil {
				return fmt.Errorf("found mismatching lvol children %+v with recorded prev lvol children %+v when validating lvol %s", cur.Children, prev.Children, prev.Name)
			}
		}
	}

	// TODO:
	// When deleting a snapshot lvol, the merge of lvols results in a change of actual size. Do not return error to prevent a false alarm.
	// Need to revisit the actual size check.
	if checkActualSize && prev.ActualSize != cur.ActualSize {
		logrus.Warnf("Found mismatching lvol actual size %v with recorded prev lvol actual size %v when validating lvol %s", cur.ActualSize, prev.ActualSize, prev.Name)
	}

	if prev.SnapshotChecksum == "" {
		prev.SnapshotChecksum = cur.SnapshotChecksum
	}
	if cur.SnapshotChecksum == "" {
		prev.SnapshotChecksum = ""
	}
	if prev.SnapshotChecksum != cur.SnapshotChecksum {
		return fmt.Errorf("found mismatching lvol snapshot checksum %v with recorded prev lvol snapshot checksum %v when validating lvol %s", cur.SnapshotChecksum, prev.SnapshotChecksum, prev.Name)
	}

	return nil
}

func getExposedPort(subsystem *spdktypes.NvmfSubsystem) (exposedPort int32, err error) {
	if subsystem == nil || len(subsystem.ListenAddresses) == 0 {
		return 0, fmt.Errorf("cannot find the NVMf subsystem")
	}

	port := 0
	for _, listenAddr := range subsystem.ListenAddresses {
		if !strings.EqualFold(string(listenAddr.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
			!strings.EqualFold(string(listenAddr.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
			continue
		}
		port, err = strconv.Atoi(listenAddr.Trsvcid)
		if err != nil {
			return 0, err
		}
		return int32(port), nil
	}

	return 0, fmt.Errorf("cannot find a exposed port in the NVMf subsystem")
}

func (r *Replica) validateReplicaHead(headBdevLvol *spdktypes.BdevInfo) (err error) {
	if headBdevLvol == nil {
		return fmt.Errorf("found nil head bdev lvol for replica %s", r.Name)
	}
	if headBdevLvol.DriverSpecific.Lvol.Snapshot {
		return fmt.Errorf("found the head bdev lvol is a snapshot lvol for replica %s", r.Name)
	}
	if r.LvsUUID != headBdevLvol.DriverSpecific.Lvol.LvolStoreUUID {
		return fmt.Errorf("found mismatching lvol LvsUUID %v with recorded LvsUUID %v for replica %s", headBdevLvol.DriverSpecific.Lvol.LvolStoreUUID, r.LvsUUID, r.Name)
	}
	bdevLvolSpecSize := headBdevLvol.NumBlocks * uint64(headBdevLvol.BlockSize)
	if r.SpecSize != 0 && r.SpecSize != bdevLvolSpecSize {
		return fmt.Errorf("found mismatching lvol spec size %v with recorded spec size %v for replica %s", bdevLvolSpecSize, r.SpecSize, r.Name)
	}

	return nil
}

func (r *Replica) IsHeadLvolAvailable(spdkClient *spdkclient.Client) (isAvailable bool, err error) {
	headBdevLvol, err := spdkClient.BdevLvolGetByName(r.Alias, 0)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return false, err
		}
		return false, nil
	}

	if validateErr := r.validateReplicaHead(&headBdevLvol); validateErr != nil {
		r.log.WithError(validateErr).Warnf("Found invalid head lvol %v for replica %v, will delete it first", headBdevLvol.Name, r.Name)
		if _, deleteErr := spdkClient.BdevLvolDelete(headBdevLvol.UUID); deleteErr != nil {
			return false, deleteErr
		}
		return false, nil
	}

	return true, nil
}

func (r *Replica) updateHeadCache(spdkClient *spdkclient.Client) (err error) {
	headBdevLvol, err := spdkClient.BdevLvolGetByName(r.Alias, 0)
	if err != nil {
		return err
	}
	r.Head = BdevLvolInfoToServiceLvol(&headBdevLvol)

	if len(r.ActiveChain) == 1 || (r.ActiveChain[len(r.ActiveChain)-1] != nil && r.ActiveChain[len(r.ActiveChain)-1].Name != r.Name) {
		r.ActiveChain = append(r.ActiveChain, r.Head)
	} else {
		r.ActiveChain[len(r.ActiveChain)-1] = r.Head
	}

	index := len(r.ActiveChain) - 2
	if index < 0 {
		return fmt.Errorf("invalid active chain length %d when updating head cache", len(r.ActiveChain))
	}
	if index == 0 && r.BackingImage != nil {
		r.BackingImage.Lock()
		defer r.BackingImage.Unlock()
	}
	if r.ActiveChain[index] != nil {
		if r.ActiveChain[index].Name != r.Head.Parent {
			return fmt.Errorf("found the last entry of the active chain %v is not the head parent %v", r.ActiveChain[index].Name, r.Head.Parent)
		}
		r.ActiveChain[index].Children[r.Head.Name] = r.Head
	}

	return nil
}

func (r *Replica) prepareHead(spdkClient *spdkclient.Client, backingImage *BackingImage) (err error) {
	isHeadAvailable, err := r.IsHeadLvolAvailable(spdkClient)
	if err != nil {
		return err
	}

	if backingImage != nil {
		r.ActiveChain[0] = backingImage.Snapshot
		r.BackingImage = r.ActiveChain[0]
	}

	if !isHeadAvailable {
		var headParentLvol *Lvol
		if r.ActiveChain[len(r.ActiveChain)-1] != nil {
			if r.ActiveChain[len(r.ActiveChain)-1].Name == r.Name {
				if len(r.ActiveChain) < 2 {
					return fmt.Errorf("found invalid active chain %+v when preparing head for replica %s", len(r.ActiveChain), r.Name)
				}
				headParentLvol = r.ActiveChain[len(r.ActiveChain)-2]
			} else {
				headParentLvol = r.ActiveChain[len(r.ActiveChain)-1]
			}
		} else {
			if len(r.ActiveChain) > 1 { // The only possible case is that r.ActiveChain[len(r.ActiveChain)-1] is a nil head
				r.ActiveChain = r.ActiveChain[:len(r.ActiveChain)-1]
				headParentLvol = r.ActiveChain[len(r.ActiveChain)-1]
			}
		}
		if headParentLvol != nil { // The replica has a backing image or somehow there are already snapshots in the chain
			if _, err := spdkClient.BdevLvolClone(headParentLvol.Alias, r.Name); err != nil {
				return err
			}
			if headParentLvol.SpecSize != r.SpecSize {
				if _, err := spdkClient.BdevLvolResize(r.Alias, util.BytesToMiB(r.SpecSize)); err != nil {
					return err
				}
			}
			r.log.Infof("Replica cloned a new head lvol from the parent lvol %s", headParentLvol.Name)
		} else {
			if _, err := spdkClient.BdevLvolCreate("", r.LvsUUID, r.Name, util.BytesToMiB(r.SpecSize), "", true); err != nil {
				return err
			}
			r.log.Info("Replica created a new head lvol")
		}
	}

	// Blindly clean up then update the caches for the head
	r.Head = nil
	if r.ActiveChain[len(r.ActiveChain)-1] != nil &&
		r.ActiveChain[len(r.ActiveChain)-1].Name == r.Name {
		r.ActiveChain = r.ActiveChain[:len(r.ActiveChain)-1]
	}

	return r.updateHeadCache(spdkClient)
}

// getRootLvolName relies on the lvol name to identify if a lvol belongs to the replica,
// then figuring out whether it is the root by checking the parent
func getRootLvolName(replicaName string, bdevLvolMap map[string]*spdktypes.BdevInfo) (rootLvolName string) {
	for lvolName, bdevLvol := range bdevLvolMap {
		if lvolName != replicaName && !IsReplicaSnapshotLvol(replicaName, lvolName) {
			continue
		}
		// Consider that a backing image can be the parent of the replica root
		if bdevLvol.DriverSpecific.Lvol.BaseSnapshot != "" && IsReplicaSnapshotLvol(replicaName, bdevLvol.DriverSpecific.Lvol.BaseSnapshot) {
			continue
		}
		return lvolName
	}

	return ""
}

func constructSnapshotLvolMap(replicaName string, bdevLvolMap map[string]*spdktypes.BdevInfo) (res map[string]*Lvol, err error) {
	rootLvolName := getRootLvolName(replicaName, bdevLvolMap)
	if rootLvolName == "" {
		return nil, fmt.Errorf("cannot find the root of the replica during snapshot lvol map construction")
	}
	res = map[string]*Lvol{}

	queue := []*Lvol{BdevLvolInfoToServiceLvol(bdevLvolMap[rootLvolName])}
	for ; len(queue) > 0; queue = queue[1:] {
		curSvcLvol := queue[0]
		if curSvcLvol == nil || curSvcLvol.Name == replicaName {
			continue
		}
		if !IsReplicaSnapshotLvol(replicaName, curSvcLvol.Name) {
			continue
		}
		res[curSvcLvol.Name] = curSvcLvol

		if bdevLvolMap[curSvcLvol.Name].DriverSpecific.Lvol.Clones == nil {
			continue
		}
		for _, childLvolName := range bdevLvolMap[curSvcLvol.Name].DriverSpecific.Lvol.Clones {
			if bdevLvolMap[childLvolName] == nil {
				return nil, fmt.Errorf("cannot find child lvol %v for lvol %v during the snapshot lvol map construction", childLvolName, curSvcLvol.Name)
			}
			// Exclude the children lvols that does not belong to this replica. For example, the leftover rebuilding lvols of the previous rebuilding failed replicas.
			if !IsReplicaLvol(replicaName, childLvolName) {
				continue
			}
			curSvcLvol.Children[childLvolName] = BdevLvolInfoToServiceLvol(bdevLvolMap[childLvolName])
			queue = append(queue, curSvcLvol.Children[childLvolName])
		}
	}

	return res, nil
}

// constructActiveChainFromSnapshotLvolMap retrieves the chain bottom up (from the head to the ancestor snapshot/backing image).
func constructActiveChainFromSnapshotLvolMap(replicaName string, snapshotLvolMap map[string]*Lvol, bdevLvolMap map[string]*spdktypes.BdevInfo) (res []*Lvol, err error) {
	headBdevLvol := bdevLvolMap[replicaName]
	if headBdevLvol == nil {
		return nil, fmt.Errorf("found nil head bdev lvol for replica %s", replicaName)
	}

	var headSvcLvol *Lvol
	headParentSnapshotLvolName := headBdevLvol.DriverSpecific.Lvol.BaseSnapshot
	if IsReplicaSnapshotLvol(replicaName, headParentSnapshotLvolName) {
		headParentSnapSvcLvol := snapshotLvolMap[headParentSnapshotLvolName]
		if headParentSnapSvcLvol == nil {
			return nil, fmt.Errorf("cannot find the parent snapshot %s of the head for replica %s", headParentSnapshotLvolName, replicaName)
		}
		headSvcLvol = headParentSnapSvcLvol.Children[replicaName]
	} else { // The parent of the head is nil or a backing image
		headSvcLvol = BdevLvolInfoToServiceLvol(headBdevLvol)
	}
	if headSvcLvol == nil {
		return nil, fmt.Errorf("found nil head svc lvol for replica %s", replicaName)
	}

	newChain := []*Lvol{headSvcLvol}
	// TODO: Considering the clone, this function or `constructSnapshotMap` may need to construct the children map for the head

	// Build the majority of the chain with `snapshotMap` so that it does not need to worry about the snap svc lvol children map maintenance.
	for curSvcLvol := snapshotLvolMap[headSvcLvol.Parent]; curSvcLvol != nil; curSvcLvol = snapshotLvolMap[curSvcLvol.Parent] {
		newChain = append(newChain, curSvcLvol)
	}

	// Check if the root snap/head lvol has a parent. If YES, it means that this replica contains a backing image
	var biSvcLvol *Lvol
	rootLvol := newChain[len(newChain)-1]
	if rootLvol.Parent != "" {
		// Here we won't maintain the complete children map for the backing image Lvol since it may contain root lvols of other replicas
		biBdevLvol := bdevLvolMap[rootLvol.Parent]
		if biBdevLvol == nil {
			return nil, fmt.Errorf("cannot find backing image lvol %v for the current bdev lvol map for replica %s", rootLvol.Parent, replicaName)
		}
		biSvcLvol = BdevLvolInfoToServiceLvol(biBdevLvol)
		biSvcLvol.Children[rootLvol.Name] = rootLvol
	}
	newChain = append(newChain, biSvcLvol)

	// Need to flip r.ActiveSnapshotChain. By convention the oldest one (backing image) should be at index 0
	for head, tail := 0, len(newChain)-1; head < tail; head, tail = head+1, tail-1 {
		newChain[head], newChain[tail] = newChain[tail], newChain[head]
	}

	return newChain, nil
}

// Create initiates the replica, prepares the head lvol bdev then blindly exposes it for the replica.
func (r *Replica) Create(spdkClient *spdkclient.Client, portCount int32, superiorPortAllocator *commonbitmap.Bitmap, backingImage *BackingImage) (ret *spdkrpc.Replica, err error) {
	updateRequired := true

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State == types.InstanceStateRunning {
		updateRequired = false
		return nil, grpcstatus.Errorf(grpccodes.AlreadyExists, "replica %v already exists and running", r.Name)
	}
	if r.State != types.InstanceStatePending && r.State != types.InstanceStateStopped {
		updateRequired = false
		return nil, fmt.Errorf("invalid state %s for replica %s creation", r.State, r.Name)
	}

	defer func() {
		if err != nil {
			r.log.WithError(err).Errorf("Failed to create replica %s", r.Name)
			if r.State != types.InstanceStateError {
				r.State = types.InstanceStateError
			}
			r.ErrorMsg = err.Error()

			ret = ServiceReplicaToProtoReplica(r)
			err = nil
		} else {
			if r.State != types.InstanceStateError {
				r.ErrorMsg = ""
			}
		}
	}()

	// Create bdev lvol if the replica is the new one
	if r.State == types.InstanceStatePending {
		if len(r.ActiveChain) != 1 {
			return nil, fmt.Errorf("invalid chain length %d for new replica creation", len(r.ActiveChain))
		}
	}

	var lvsList []spdktypes.LvstoreInfo
	if r.LvsUUID != "" {
		lvsList, err = spdkClient.BdevLvolGetLvstore("", r.LvsUUID)
	} else if r.LvsName != "" {
		lvsList, err = spdkClient.BdevLvolGetLvstore(r.LvsName, "")
	}
	if err != nil {
		return nil, err
	}
	if len(lvsList) != 1 {
		return nil, fmt.Errorf("found zero or multiple lvstore with name %s and UUID %s during replica %s creation", r.LvsName, r.LvsUUID, r.Name)
	}
	if r.LvsName == "" {
		r.LvsName = lvsList[0].Name
	}
	if r.LvsUUID == "" {
		r.LvsUUID = lvsList[0].UUID
	}
	if r.LvsName != lvsList[0].Name || r.LvsUUID != lvsList[0].UUID {
		return nil, fmt.Errorf("found mismatching between the actual lvstore name %s with UUID %s and the recorded lvstore name %s with UUID %s during replica %s creation", lvsList[0].Name, lvsList[0].UUID, r.LvsName, r.LvsUUID, r.Name)
	}

	// A stopped replica may be a broken one. We need to make sure the head lvol is ready first.
	if err := r.prepareHead(spdkClient, backingImage); err != nil {
		return nil, err
	}

	// In case of failed replica reuse/restart being errored by r.validateAndUpdate(), we should make sure the caches are correct.
	if r.State == types.InstanceStatePending && r.reconstructRequired {
		bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
		if err != nil {
			return nil, err
		}
		if err := r.construct(bdevLvolMap); err != nil {
			return nil, err
		}
		r.State = types.InstanceStateStopped
	}

	podIP, err := commonnet.GetIPForPod()
	if err != nil {
		return nil, err
	}
	r.IP = podIP

	r.PortStart, r.PortEnd, err = superiorPortAllocator.AllocateRange(portCount)
	if err != nil {
		return nil, err
	}
	// Always reserved the 1st port for replica expose and the rest for rebuilding
	bitmap, err := commonbitmap.NewBitmap(r.PortStart+1, r.PortEnd)
	if err != nil {
		return nil, err
	}
	r.portAllocator = bitmap

	nqn := helpertypes.GetNQN(r.Name)

	// Blindly stop exposing the bdev if it exists. This is to avoid potential inconsistencies during salvage case.
	if err := spdkClient.StopExposeBdev(nqn); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, errors.Wrapf(err, "failed to stop expose replica %v", r.Name)
	}

	nguid := commonutils.RandomID(nvmeNguidLength)
	if err := spdkClient.StartExposeBdev(nqn, r.Head.UUID, nguid, podIP, strconv.Itoa(int(r.PortStart))); err != nil {
		return nil, err
	}
	r.IsExposed = true
	r.State = types.InstanceStateRunning

	r.log.Info("Created replica")

	return ServiceReplicaToProtoReplica(r), nil
}

func (r *Replica) Delete(spdkClient *spdkclient.Client, cleanupRequired bool, superiorPortAllocator *commonbitmap.Bitmap) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		// Considering that there may be still pending validations, it's better to update the state after the deletion.
		prevState := r.State
		if err != nil {
			r.log.WithError(err).Errorf("Failed to delete replica with cleanupRequired flag %v", cleanupRequired)
			if r.isRestoring {
				// This is not a real error. No need to update the state.
			} else if r.State != types.InstanceStateError {
				r.State = types.InstanceStateError
				r.ErrorMsg = err.Error()
			}
		} else {
			if !r.isRestoring {
				if cleanupRequired {
					r.State = types.InstanceStateTerminating
				} else {
					r.State = types.InstanceStateStopped
				}
			}
		}

		if r.State != types.InstanceStateError {
			r.ErrorMsg = ""
		}

		if prevState == types.ProgressStateError {
			r.reconstructRequired = true
		}

		if prevState != r.State {
			updateRequired = true
		}

		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.isRestoring {
		r.log.Info("Canceling volume restoration before replica deletion")
		r.restore.Stop()
		return fmt.Errorf("waiting for volume restoration to stop")
	}

	if r.IsExposed {
		r.log.Info("Unexposing bdev for replica deletion")
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.Name)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		r.IsExposed = false
		updateRequired = true
	}

	// Clean up the rebuilding cached info first
	r.doCleanupForRebuildingSrc(spdkClient)
	_ = r.doCleanupForRebuildingDst(spdkClient)
	if r.isRebuilding {
		r.rebuildingDstCache.rebuildingError = "replica is being deleted"
		r.rebuildingDstCache.rebuildingState = types.ProgressStateError
		r.isRebuilding = false
	}

	// The port can be released once the rebuilding and expose are stopped.
	if r.PortStart != 0 {
		if err := superiorPortAllocator.ReleaseRange(r.PortStart, r.PortEnd); err != nil {
			return errors.Wrapf(err, "failed to release port %d to %d during replica deletion with cleanup flag %v", r.PortStart, r.PortEnd, cleanupRequired)
		}
		r.portAllocator = nil
		r.PortStart, r.PortEnd = 0, 0
		updateRequired = true
	}

	if !cleanupRequired {
		return nil
	}

	// Use r.Alias here since we don't know if an errored replicas still contains the head lvol
	if _, err := spdkClient.BdevLvolDelete(r.Alias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	updateRequired = true

	// Clean up the valid snapshot tree as well as all possible leftovers or out of track lvols
	if len(r.ActiveChain) > 1 {
		bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
		if err != nil {
			return err
		}
		for lvolName, bdevLvol := range bdevLvolMap {
			if IsBackingImageSnapLvolName(lvolName) {
				for _, childLvolName := range bdevLvol.DriverSpecific.Lvol.Clones {
					if !IsReplicaLvol(r.Name, childLvolName) {
						continue
					}
					r.CleanupLvolTree(spdkClient, childLvolName, bdevLvolMap)
				}
				continue
			}
			r.CleanupLvolTree(spdkClient, lvolName, bdevLvolMap)
		}
	}

	r.log.Info("Deleted replica with all possible lvols")

	return nil
}

func (r *Replica) Get() (pReplica *spdkrpc.Replica) {
	r.RLock()
	defer r.RUnlock()
	return ServiceReplicaToProtoReplica(r)
}

func (r *Replica) SnapshotCreate(spdkClient *spdkclient.Client, snapshotName string, opts *api.SnapshotOptions) (pReplica *spdkrpc.Replica, err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateStopped && r.State != types.InstanceStateRunning {
		return nil, fmt.Errorf("invalid state %v for replica %s snapshot creation", r.State, r.Name)
	}

	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	if _, exists := r.SnapshotLvolMap[snapLvolName]; exists {
		return nil, fmt.Errorf("snapshot %s(%s) already exists in replica %s", snapshotName, snapLvolName, r.Name)
	}

	defer func() {
		if err != nil {
			if r.State != types.InstanceStateError {
				r.State = types.InstanceStateError
				updateRequired = true
			}
			r.ErrorMsg = err.Error()
		} else {
			if r.State != types.InstanceStateError {
				r.ErrorMsg = ""
			}
		}
	}()

	if r.Head == nil {
		return nil, fmt.Errorf("nil head for replica snapshot creation")
	}

	var xattrs []spdkclient.Xattr
	if opts != nil {
		userCreated := spdkclient.Xattr{
			Name:  spdkclient.UserCreated,
			Value: strconv.FormatBool(opts.UserCreated),
		}
		xattrs = append(xattrs, userCreated)

		snapshotTimestamp := spdkclient.Xattr{
			Name:  spdkclient.SnapshotTimestamp,
			Value: opts.Timestamp,
		}
		xattrs = append(xattrs, snapshotTimestamp)
	}

	snapUUID, err := spdkClient.BdevLvolSnapshot(r.Head.UUID, snapLvolName, xattrs)
	if err != nil {
		return nil, err
	}

	snapBdevLvol, err := spdkClient.BdevLvolGetByName(snapUUID, 0)
	if err != nil {
		return nil, err
	}
	snapSvcLvol := BdevLvolInfoToServiceLvol(&snapBdevLvol)

	headBdevLvol, err := spdkClient.BdevLvolGetByName(r.Head.UUID, 0)
	if err != nil {
		return nil, err
	}
	r.Head = BdevLvolInfoToServiceLvol(&headBdevLvol)
	snapSvcLvol.Children[r.Head.Name] = r.Head

	// Already contain a valid snapshot lvol or backing image lvol before this snapshot creation
	if len(r.ActiveChain) > 1 && r.ActiveChain[len(r.ActiveChain)-2] != nil {
		prevSvcLvol := r.ActiveChain[len(r.ActiveChain)-2]
		prevSvcLvol.Lock()
		delete(prevSvcLvol.Children, r.Head.Name)
		prevSvcLvol.Children[snapSvcLvol.Name] = snapSvcLvol
		prevSvcLvol.Unlock()
	}
	r.ActiveChain[len(r.ActiveChain)-1] = snapSvcLvol
	r.ActiveChain = append(r.ActiveChain, r.Head)
	r.SnapshotLvolMap[snapLvolName] = snapSvcLvol
	updateRequired = true

	r.log.Infof("Replica created snapshot %s(%s)", snapshotName, snapSvcLvol.Alias)

	return ServiceReplicaToProtoReplica(r), err
}

func (r *Replica) SnapshotDelete(spdkClient *spdkclient.Client, snapshotName string) (pReplica *spdkrpc.Replica, err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateStopped && r.State != types.InstanceStateRunning {
		return nil, fmt.Errorf("invalid state %v for replica %s snapshot deletion", r.State, r.Name)
	}

	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	snapSvcLvol := r.SnapshotLvolMap[snapLvolName]
	if snapSvcLvol == nil {
		return ServiceReplicaToProtoReplica(r), nil
	}
	if len(snapSvcLvol.Children) > 1 {
		return nil, fmt.Errorf("cannot delete snapshot %s(%s) since it has %d children", snapshotName, snapLvolName, len(snapSvcLvol.Children))
	}

	defer func() {
		if err != nil {
			if r.State != types.InstanceStateError {
				r.State = types.InstanceStateError
				updateRequired = true
			}
			r.ErrorMsg = err.Error()
		} else {
			if r.State != types.InstanceStateError {
				r.ErrorMsg = ""
			}
		}
	}()

	if _, err := spdkClient.BdevLvolDelete(snapSvcLvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, err
	}
	r.removeLvolFromSnapshotLvolMapWithoutLock(snapLvolName)
	r.removeLvolFromActiveChainWithoutLock(snapLvolName)
	for _, childSvcLvol := range snapSvcLvol.Children {
		bdevLvol, err := spdkClient.BdevLvolGetByName(childSvcLvol.UUID, 0)
		if err != nil {
			return nil, err
		}
		childSvcLvol.ActualSize = bdevLvol.DriverSpecific.Lvol.NumAllocatedClusters * defaultClusterSize
		childSvcLvol.SnapshotChecksum = ""
	}

	updateRequired = true

	r.log.Infof("Replica deleted snapshot %s(%s)", snapshotName, snapSvcLvol.Alias)

	return ServiceReplicaToProtoReplica(r), nil
}

func (r *Replica) removeLvolFromSnapshotLvolMapWithoutLock(snapsLvolName string) {
	var deletingSvcLvol, parentSvcLvol, childSvcLvol *Lvol

	deletingSvcLvol = r.SnapshotLvolMap[snapsLvolName]
	if IsReplicaSnapshotLvol(r.Name, deletingSvcLvol.Parent) {
		parentSvcLvol = r.SnapshotLvolMap[deletingSvcLvol.Parent]
	} else {
		// Parent is either backing image or nil
		parentSvcLvol = r.ActiveChain[0]
	}
	if parentSvcLvol != nil {
		delete(parentSvcLvol.Children, deletingSvcLvol.Name)
	}
	for _, childSvcLvol = range deletingSvcLvol.Children {
		if parentSvcLvol != nil {
			parentSvcLvol.Children[childSvcLvol.Name] = childSvcLvol
			childSvcLvol.Parent = parentSvcLvol.Name
		} else {
			childSvcLvol.Parent = ""
		}
	}

	delete(r.SnapshotLvolMap, snapsLvolName)
}

func (r *Replica) removeLvolFromActiveChainWithoutLock(snapLvolName string) int {
	pos := -1
	for idx, lvol := range r.ActiveChain {
		// Cannot remove the backing image from the chain
		if idx == 0 {
			continue
		}
		if lvol.Name == snapLvolName {
			pos = idx
			break
		}
	}

	// Cannot remove backing image lvol or head lvol
	prevChain := r.ActiveChain
	if pos >= 1 && pos < len(r.ActiveChain)-1 {
		r.ActiveChain = append([]*Lvol{}, prevChain[:pos]...)
		r.ActiveChain = append(r.ActiveChain, prevChain[pos+1:]...)
	}

	return pos
}

func (r *Replica) SnapshotRevert(spdkClient *spdkclient.Client, snapshotName string) (pReplica *spdkrpc.Replica, err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateStopped && r.State != types.InstanceStateRunning {
		return nil, fmt.Errorf("invalid state %v for replica %s snapshot revert", r.State, r.Name)
	}

	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	snapSvcLvol := r.SnapshotLvolMap[snapLvolName]
	if snapSvcLvol == nil {
		return nil, fmt.Errorf("cannot revert to a non-existing snapshot %s(%s)", snapshotName, snapLvolName)
	}

	defer func() {
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
			updateRequired = true
		}
	}()

	if len(r.ActiveChain) < 2 {
		return nil, fmt.Errorf("invalid chain length %d for replica snapshot revert", len(r.ActiveChain))
	}

	if _, err := spdkClient.BdevLvolDelete(r.Alias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, err
	}
	// The parent of the old head lvol is a valid snapshot lvol or backing image lvol
	if r.ActiveChain[len(r.ActiveChain)-2] != nil {
		delete(r.ActiveChain[len(r.ActiveChain)-2].Children, r.Name)
	}
	r.Head = nil
	r.ActiveChain = r.ActiveChain[:len(r.ActiveChain)-1]

	// TODO: If the below steps fail, there will be no head lvol for the replica. Need to guarantee that the replica can be cleaned up correctly in this case

	headLvolUUID, err := spdkClient.BdevLvolClone(snapSvcLvol.UUID, r.Name)
	if err != nil {
		return nil, err
	}

	bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
	if err != nil {
		return nil, err
	}

	newSnapshotLvolMap, err := constructSnapshotLvolMap(r.Name, bdevLvolMap)
	if err != nil {
		return nil, err
	}
	newChain, err := constructActiveChainFromSnapshotLvolMap(r.Name, newSnapshotLvolMap, bdevLvolMap)
	if err != nil {
		return nil, err
	}

	r.Head = newChain[len(newChain)-1]
	r.ActiveChain = newChain
	r.SnapshotLvolMap = newSnapshotLvolMap

	if r.IsExposed {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.Name)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return nil, err
		}
		r.IsExposed = false

		nguid := commonutils.RandomID(nvmeNguidLength)
		if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(r.Name), headLvolUUID, nguid, r.IP, strconv.Itoa(int(r.PortStart))); err != nil {
			return nil, err
		}
		r.IsExposed = true
	}

	updateRequired = true

	r.log.Infof("Replica reverted snapshot %s(%s)", snapshotName, snapSvcLvol.Alias)

	return ServiceReplicaToProtoReplica(r), nil
}

// SnapshotPurge asks the replica to delete all system created snapshots
func (r *Replica) SnapshotPurge(spdkClient *spdkclient.Client) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	defer func() {
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
			updateRequired = true
		}
	}()

	if len(r.ActiveChain) < 2 {
		return fmt.Errorf("invalid chain length %d for replica snapshot purge", len(r.ActiveChain))
	}

	// delete all non-user-created snapshots
	for snapshotLvolName, snapSvcLvol := range r.SnapshotLvolMap {
		if snapSvcLvol.UserCreated {
			continue
		}
		if len(snapSvcLvol.Children) > 1 {
			continue
		}
		if snapSvcLvol.Children[r.Name] != nil {
			continue
		}
		if _, err := spdkClient.BdevLvolDelete(snapSvcLvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		r.removeLvolFromSnapshotLvolMapWithoutLock(snapshotLvolName)
		r.removeLvolFromActiveChainWithoutLock(snapshotLvolName)

		for _, childSvcLvol := range snapSvcLvol.Children {
			bdevLvol, err := spdkClient.BdevLvolGetByName(childSvcLvol.UUID, 0)
			if err != nil {
				return err
			}
			childSvcLvol.ActualSize = bdevLvol.DriverSpecific.Lvol.NumAllocatedClusters * defaultClusterSize
		}

		updateRequired = true
	}
	return nil
}

// RebuildingSrcStart asks the source replica to check the parent snapshot of the head and expose it as a NVMf bdev if necessary.
// If the source replica and the destination replicas have different IPs, the API will expose the snapshot lvol as a NVMf bdev and return the address <IP>:<Port>.
// Otherwise, the API will directly return the snapshot lvol alias.
// It's not responsible for attaching rebuilding lvol of the dst replica.
func (r *Replica) RebuildingSrcStart(spdkClient *spdkclient.Client, dstReplicaName, dstReplicaAddress, exposedSnapshotName string) (exposedSnapshotLvolAddress string, err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateRunning {
		return "", fmt.Errorf("invalid state %v for replica %s rebuilding src start", r.State, r.Name)
	}
	if r.isRebuilding {
		return "", fmt.Errorf("replica %s is being rebuilding hence it cannot be the source of rebuilding replica %s with snapshot %s", r.Name, dstReplicaName, exposedSnapshotName)
	}

	snapLvol := r.SnapshotLvolMap[GetReplicaSnapshotLvolName(r.Name, exposedSnapshotName)]
	if snapLvol == nil {
		return "", fmt.Errorf("cannot find snapshot %s for the replica %s rebuilding src start", exposedSnapshotName, r.Name)
	}

	if r.rebuildingSrcCache.dstReplicaName != "" || r.rebuildingSrcCache.exposedSnapshotAlias != "" {
		if r.rebuildingSrcCache.dstReplicaName != dstReplicaName || r.rebuildingSrcCache.exposedSnapshotAlias != snapLvol.Alias {
			return "", fmt.Errorf("replica %s is helping rebuilding replica %s with the rebuilding snapshot %s, hence it cannot be the source of rebuilding replica %s with snapshot %s", r.Name, r.rebuildingSrcCache.dstReplicaName, r.rebuildingSrcCache.exposedSnapshotAlias, dstReplicaName, exposedSnapshotName)
		}
		if r.rebuildingSrcCache.exposedSnapshotPort != 0 {
			return net.JoinHostPort(r.IP, strconv.Itoa(int(r.rebuildingSrcCache.exposedSnapshotPort))), nil
		}
		// No exposed snapshot port, need to expose the snapshot lvol again
	}

	port, _, err := r.portAllocator.AllocateRange(1)
	if err != nil {
		return "", err
	}
	nguid := commonutils.RandomID(nvmeNguidLength)
	if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(snapLvol.Name), snapLvol.UUID, nguid, r.IP, strconv.Itoa(int(port))); err != nil {
		return "", err
	}
	exposedSnapshotLvolAddress = net.JoinHostPort(r.IP, strconv.Itoa(int(port)))

	r.rebuildingSrcCache.dstReplicaName = dstReplicaName
	r.rebuildingSrcCache.exposedSnapshotAlias = snapLvol.Alias
	r.rebuildingSrcCache.exposedSnapshotPort = port
	updateRequired = true

	r.log.Infof("Replica exposed snapshot %s(%s) to address %s for replica %s rebuilding start", exposedSnapshotName, snapLvol.UUID, exposedSnapshotLvolAddress, dstReplicaName)

	return exposedSnapshotLvolAddress, nil
}

// RebuildingSrcFinish asks the source replica to detach the rebuilding lvolof the dst replica, stop exposing the snapshot lvol (if necessary), and clean up the dst replica related cache
func (r *Replica) RebuildingSrcFinish(spdkClient *spdkclient.Client, dstReplicaName string) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.rebuildingSrcCache.dstReplicaName != "" && r.rebuildingSrcCache.dstReplicaName != dstReplicaName {
		return fmt.Errorf("found mismatching between the required dst replica name %s and the recorded dst replica name %s for replica %s rebuilding src finish", dstReplicaName, r.rebuildingSrcCache.dstReplicaName, r.Name)
	}

	r.doCleanupForRebuildingSrc(spdkClient)
	updateRequired = true

	return
}

func (r *Replica) doCleanupForRebuildingSrc(spdkClient *spdkclient.Client) {
	r.rebuildingSrcCache.shallowCopySnapshotName = ""
	r.rebuildingSrcCache.shallowCopyOpID = 0
	r.rebuildingSrcCache.shallowCopyStatus = spdktypes.ShallowCopyStatus{}

	if r.rebuildingSrcCache.dstRebuildingBdevName != "" {
		if err := disconnectNVMfBdev(spdkClient, r.rebuildingSrcCache.dstRebuildingBdevName); err != nil {
			r.log.WithError(err).Errorf("Failed to disconnect the rebuilding dst bdev %s for rebuilding src cleanup, will continue", r.rebuildingSrcCache.dstRebuildingBdevName)
		} else {
			r.rebuildingSrcCache.dstRebuildingBdevName = ""
		}
	}

	if r.rebuildingSrcCache.exposedSnapshotPort != 0 {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(spdktypes.GetLvolNameFromAlias(r.rebuildingSrcCache.exposedSnapshotAlias))); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			r.log.WithError(err).Errorf("Failed to stop exposing the snapshot %s for rebuilding src cleanup, will continue", r.rebuildingSrcCache.exposedSnapshotAlias)
		} else {
			if err := r.portAllocator.ReleaseRange(r.rebuildingSrcCache.exposedSnapshotPort, r.rebuildingSrcCache.exposedSnapshotPort); err != nil {
				r.log.WithError(err).Errorf("Failed to release exposed snapshot port %d for rebuilding src cleanup, will continue", r.rebuildingSrcCache.exposedSnapshotPort)
			} else {
				r.rebuildingSrcCache.exposedSnapshotPort = 0
				r.rebuildingSrcCache.exposedSnapshotAlias = ""
			}
		}
	}

	r.rebuildingSrcCache.dstReplicaName = ""
}

// rebuildingSrcAttachNoLock blindly attaches the rebuilding lvol of the dst replica as NVMf controller no matter if src and dst are on different nodes
func (r *Replica) rebuildingSrcAttachNoLock(spdkClient *spdkclient.Client, dstReplicaName, dstRebuildingLvolAddress string) (err error) {
	dstRebuildingLvolName := GetReplicaRebuildingLvolName(dstReplicaName)
	if r.rebuildingSrcCache.dstRebuildingBdevName != "" {
		controllerName := helperutil.GetNvmeControllerNameFromNamespaceName(r.rebuildingSrcCache.dstRebuildingBdevName)
		if dstRebuildingLvolName != controllerName {
			return fmt.Errorf("found mismatching between the required dst bdev NVMe controller name %s and the expected dst controller name %s for replica %s rebuilding src attach", dstRebuildingLvolName, controllerName, r.Name)
		}
		return nil
	}

	r.rebuildingSrcCache.dstRebuildingBdevName, err = connectNVMfBdev(spdkClient, dstRebuildingLvolName, dstRebuildingLvolAddress,
		replicaCtrlrLossTimeoutSec, replicaFastIOFailTimeoutSec)
	if err != nil {
		return errors.Wrapf(err, "failed to connect rebuilding lvol %s with address %s as a NVMe bdev for replica %s rebuilding src attach", dstRebuildingLvolName, dstRebuildingLvolAddress, r.Name)
	}

	return nil
}

// rebuildingSrcDetachNoLock detaches the rebuilding lvol of the dst replica as NVMf controller if src and dst are on different nodes
func (r *Replica) rebuildingSrcDetachNoLock(spdkClient *spdkclient.Client) (err error) {
	if r.rebuildingSrcCache.dstRebuildingBdevName == "" {
		return nil
	}
	if err := disconnectNVMfBdev(spdkClient, r.rebuildingSrcCache.dstRebuildingBdevName); err != nil {
		return err
	}
	r.rebuildingSrcCache.dstRebuildingBdevName = ""

	return nil
}

// RebuildingSrcShallowCopyStart asks the src replica to attach the dst rebuilding lvol, start a shallow copy from its snapshot lvol to it, then detach it.
func (r *Replica) RebuildingSrcShallowCopyStart(spdkClient *spdkclient.Client, snapshotName, dstRebuildingLvolAddress string) (err error) {
	r.Lock()
	defer r.Unlock()

	if r.rebuildingSrcCache.shallowCopyStatus.State == types.ProgressStateInProgress || r.rebuildingSrcCache.shallowCopyStatus.State == types.ProgressStateStarting {
		return fmt.Errorf("cannot start a shallow copy from snapshot %s for the src replica %s since there is already a shallow copy starting or in progress", snapshotName, r.Name)
	}

	if err = r.rebuildingSrcDetachNoLock(spdkClient); err != nil {
		return errors.Wrapf(err, "failed to detach the rebuilding lvol of the dst replica %s before src replica %s shallow copy start", r.rebuildingSrcCache.dstReplicaName, r.Name)
	}
	if err = r.rebuildingSrcAttachNoLock(spdkClient, r.rebuildingSrcCache.dstReplicaName, dstRebuildingLvolAddress); err != nil {
		return errors.Wrapf(err, "failed to attach the rebuilding lvol of the dst replica %s before src replica %s shallow copy start", r.rebuildingSrcCache.dstReplicaName, r.Name)
	}

	var shallowCopyOpID uint32
	defer func() {
		if err != nil || shallowCopyOpID == 0 {
			return
		}
		go func() {
			timer := time.NewTimer(MaxShallowCopyWaitTime)
			defer timer.Stop()
			ticker := time.NewTicker(ShallowCopyCheckInterval)
			defer ticker.Stop()
			continuousRetryCount := 0
			for stopWaiting := false; !stopWaiting; {
				select {
				case <-timer.C:
					r.log.Errorf("Timeout waiting for the src replica %s shallow copy %v complete before detaching the rebuilding lvol of the dst replica %s, will give up", r.Name, shallowCopyOpID, r.rebuildingSrcCache.dstReplicaName)
					stopWaiting = true
					break
				case <-ticker.C:
					r.Lock()
					if r.rebuildingSrcCache.shallowCopyOpID != shallowCopyOpID || r.rebuildingSrcCache.shallowCopySnapshotName != snapshotName {
						r.Unlock()
						stopWaiting = true
						break
					}
					status, err := r.rebuildingSrcShallowCopyStatusUpdateAndHandlingNoLock(spdkClient)
					r.Unlock()
					if err != nil {
						continuousRetryCount++
						if continuousRetryCount > maxNumRetries {
							r.log.WithError(err).Errorf("Failed to check the src replica %s shallow copy %v status over %d times before detaching the rebuilding lvol of the dst replica %s, will give up", r.Name, shallowCopyOpID, maxNumRetries, r.rebuildingSrcCache.dstReplicaName)
							stopWaiting = true
							break
						}
						logrus.WithError(err).Errorf("Failed to check the src replica %s shallow copy %v status before detaching the rebuilding lvol of the dst replica %s", r.Name, shallowCopyOpID, r.rebuildingSrcCache.dstReplicaName)
						continue
					}
					continuousRetryCount = 0
					if status.State == types.ProgressStateError || status.State == types.ProgressStateComplete {
						stopWaiting = true
						break
					}
				}
			}
		}()
	}()

	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)

	if r.rebuildingSrcCache.dstRebuildingBdevName == "" {
		return fmt.Errorf("no destination bdev for src replica %s shallow copy start", r.Name)
	}
	if r.SnapshotLvolMap[snapLvolName] == nil {
		return fmt.Errorf("cannot find snapshot %s for src replica %s shallow copy start", snapshotName, r.Name)
	}

	if shallowCopyOpID, err = spdkClient.BdevLvolStartShallowCopy(r.SnapshotLvolMap[snapLvolName].UUID, r.rebuildingSrcCache.dstRebuildingBdevName); err != nil {
		return err
	}
	r.rebuildingSrcCache.shallowCopySnapshotName = snapshotName
	r.rebuildingSrcCache.shallowCopyOpID = shallowCopyOpID
	r.rebuildingSrcCache.shallowCopyStatus = spdktypes.ShallowCopyStatus{}

	if _, err = r.rebuildingSrcShallowCopyStatusUpdateAndHandlingNoLock(spdkClient); err != nil {
		return err
	}

	return
}

func (r *Replica) rebuildingSrcShallowCopyStatusUpdateAndHandlingNoLock(spdkClient *spdkclient.Client) (status *spdktypes.ShallowCopyStatus, err error) {
	if r.rebuildingSrcCache.shallowCopyOpID == 0 {
		return &spdktypes.ShallowCopyStatus{}, nil
	}
	// For a complete or errored shallow copy, spdk_tgt will clean up its status after the first check returns.
	// Hence we need to directly use the cached status here.
	if r.rebuildingSrcCache.shallowCopyStatus.State == types.ProgressStateError || r.rebuildingSrcCache.shallowCopyStatus.State == types.ProgressStateComplete {
		return &spdktypes.ShallowCopyStatus{
			State:          r.rebuildingSrcCache.shallowCopyStatus.State,
			CopiedClusters: r.rebuildingSrcCache.shallowCopyStatus.CopiedClusters,
			TotalClusters:  r.rebuildingSrcCache.shallowCopyStatus.TotalClusters,
			Error:          r.rebuildingSrcCache.shallowCopyStatus.Error,
		}, nil
	}

	status, err = spdkClient.BdevLvolCheckShallowCopy(r.rebuildingSrcCache.shallowCopyOpID)
	if err != nil {
		return nil, err
	}
	if status.State == types.SPDKShallowCopyStateInProgress {
		status.State = types.ProgressStateInProgress
	}

	r.rebuildingSrcCache.shallowCopyStatus = *status

	// The status update and the detachment should be done atomically
	// Otherwise, the next shallow copy will be started before this detachment complete. In other words, the next shallow copy will be failed by this detachment.
	if status.State == types.ProgressStateError || status.State == types.ProgressStateComplete {
		err = r.rebuildingSrcDetachNoLock(spdkClient)
		if err != nil {
			r.log.WithError(err).Errorf("Failed to detach the rebuilding lvol of the dst replica %s after src replica %s shallow copy %v from snapshot %s finish, will continue", r.rebuildingSrcCache.dstReplicaName, r.Name, r.rebuildingSrcCache.shallowCopyOpID, r.rebuildingSrcCache.shallowCopySnapshotName)
		}
	}

	return status, nil
}

// RebuildingSrcShallowCopyCheck asks the src replica to check the shallow copy progress and status via the snapshot name.
func (r *Replica) RebuildingSrcShallowCopyCheck(snapshotName string) (status spdktypes.ShallowCopyStatus, err error) {
	r.RLock()
	recordedSnapshotName := r.rebuildingSrcCache.shallowCopySnapshotName
	status = r.rebuildingSrcCache.shallowCopyStatus
	r.RUnlock()

	if snapshotName != recordedSnapshotName {
		status = spdktypes.ShallowCopyStatus{
			State: types.ProgressStateError,
			Error: fmt.Sprintf("found mismatching between the required snapshot name %v and the recorded snapshotName %v for src replica %s shallow copy check", snapshotName, recordedSnapshotName, r.Name),
		}
	}

	return status, nil
}

// RebuildingDstStart asks the dst replica to create a new head lvol based on the external snapshot of the src replica and blindly expose it as a NVMf bdev.
// It returns the new head lvol address <IP>:<Port>.
// Notice that input `externalSnapshotAddress` is the alias of the external snapshot lvol if src and dst have on the same IP, otherwise it's the NVMf address of the external snapshot lvol.
func (r *Replica) RebuildingDstStart(spdkClient *spdkclient.Client, srcReplicaName, srcReplicaAddress, externalSnapshotName, externalSnapshotAddress string, rebuildingSnapshotList []*api.Lvol) (address string, err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateRunning {
		return "", fmt.Errorf("invalid state %v for dst replica %s rebuilding start", r.State, r.Name)
	}
	if r.isRebuilding {
		return "", fmt.Errorf("replica %s rebuilding is in process", r.Name)
	}

	defer func() {
		if err != nil {
			if r.State != types.InstanceStateError {
				r.State = types.InstanceStateError
			}
			r.ErrorMsg = err.Error()
			if r.rebuildingDstCache.rebuildingError == "" {
				r.rebuildingDstCache.rebuildingError = err.Error()
				r.rebuildingDstCache.rebuildingState = types.ProgressStateError
			}
		} else {
			if r.State != types.InstanceStateError {
				r.ErrorMsg = ""
			}
		}

		updateRequired = true
	}()

	// Replica.Delete and Replica.Create do not guarantee that the previous rebuilding src replica info is cleaned up
	if r.rebuildingDstCache.srcReplicaName != "" || r.rebuildingDstCache.srcReplicaAddress != "" || r.rebuildingDstCache.externalSnapshotName != "" || r.rebuildingDstCache.externalSnapshotBdevName != "" {
		if err := r.doCleanupForRebuildingDst(spdkClient); err != nil {
			return "", errors.Wrapf(err, "failed to clean up the previous rebuilding dst info for dst replica rebuilding start, src replica name %s, address %s, external snapshot name %s, or external snapshot bdev name %s", r.rebuildingDstCache.srcReplicaName, r.rebuildingDstCache.srcReplicaAddress, r.rebuildingDstCache.externalSnapshotName, r.rebuildingDstCache.externalSnapshotBdevName)
		}
	}
	r.rebuildingDstCache.srcReplicaName = srcReplicaName
	r.rebuildingDstCache.srcReplicaAddress = srcReplicaAddress
	for _, apiLvol := range rebuildingSnapshotList {
		r.rebuildingDstCache.rebuildingSnapshotMap[apiLvol.Name] = apiLvol
		r.rebuildingDstCache.rebuildingSize += apiLvol.ActualSize
	}

	externalSnapshotLvolName := GetReplicaSnapshotLvolName(srcReplicaName, externalSnapshotName)
	externalSnapshotBdevName, err := connectNVMfBdev(spdkClient, externalSnapshotLvolName, externalSnapshotAddress,
		replicaCtrlrLossTimeoutSec, replicaFastIOFailTimeoutSec)
	if err != nil {
		return "", errors.Wrapf(err, "failed to connect the external src snapshot lvol %s with address %s as a NVMf bdev for dst replica %v rebuilding start", externalSnapshotLvolName, externalSnapshotAddress, r.Name)
	}
	if r.rebuildingDstCache.externalSnapshotBdevName != "" && r.rebuildingDstCache.externalSnapshotBdevName != externalSnapshotBdevName {
		return "", fmt.Errorf("found mismatching between the required src snapshot bdev name %s and the expected src snapshot bdev name %s for dst replica %s rebuilding start", externalSnapshotBdevName, r.rebuildingDstCache.externalSnapshotBdevName, r.Name)
	}
	r.rebuildingDstCache.externalSnapshotName = externalSnapshotName
	r.rebuildingDstCache.externalSnapshotBdevName = externalSnapshotBdevName

	// Prepare a rebuilding port so that the dst replica can expose a rebuilding lvol in RebuildingDstSnapshotRevert
	if r.rebuildingDstCache.rebuildingPort == 0 {
		if r.rebuildingDstCache.rebuildingPort, _, err = r.portAllocator.AllocateRange(1); err != nil {
			return "", errors.Wrapf(err, "failed to allocate a rebuilding port for dst replica %v rebuilding start", r.Name)
		}
	}

	if r.IsExposed {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.Name)); err != nil {
			return "", err
		}
		r.IsExposed = false
	}
	// TODO: Uncomment below code after the delta shallow copy API is ready
	//// For the old head, if it's a non-empty one, rename it for reuse later.
	//// Otherwise, directly remove it
	//if r.Head.ActualSize > 0 {
	//	expiredLvolName := GenerateReplicaExpiredLvolName(r.Name)
	//	if _, err := spdkClient.BdevLvolRename(r.Head.UUID, expiredLvolName); err != nil {
	//		r.log.WithError(err).Warnf("Failed to rename the previous head lvol %s to %s for dst replica %v rebuilding start, will try to remove it instead", r.Head.Alias, expiredLvolName, r.Name)
	//	}
	//}
	if _, err := spdkClient.BdevLvolDelete(r.Alias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return "", err
	}

	// Retain the backing image in the active chain. All unverified lvols should be removed first.
	r.Head = nil
	r.ActiveChain = []*Lvol{r.ActiveChain[0]}

	// Create a new head lvol based on the external src snapshot lvol then
	headLvolUUID, err := spdkClient.BdevLvolCloneBdev(r.rebuildingDstCache.externalSnapshotBdevName, r.LvsName, r.Name)
	if err != nil {
		return "", err
	}
	headBdevLvol, err := spdkClient.BdevLvolGetByName(headLvolUUID, 0)
	if err != nil {
		return "", err
	}
	r.Head = BdevLvolInfoToServiceLvol(&headBdevLvol)
	r.ActiveChain = append(r.ActiveChain, r.Head)

	nguid := commonutils.RandomID(nvmeNguidLength)
	if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(r.Name), r.Head.UUID, nguid, r.IP, strconv.Itoa(int(r.PortStart))); err != nil {
		return "", err
	}
	r.IsExposed = true
	dstHeadLvolAddress := net.JoinHostPort(r.IP, strconv.Itoa(int(r.PortStart)))

	// Delete extra snapshots if any
	//rebuildingLvolName := GetReplicaRebuildingLvolName(r.Name)
	bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
	if err != nil {
		return "", err
	}
	for lvolName, lvol := range bdevLvolMap {
		if lvolName == r.Name || (r.ActiveChain[0] != nil && lvolName == r.ActiveChain[0].Name) {
			continue
		}
		if IsReplicaExpiredLvol(r.Name, lvolName) {
			continue
		}
		if r.rebuildingDstCache.rebuildingSnapshotMap[GetSnapshotNameFromReplicaSnapshotLvolName(r.Name, lvolName)] != nil {
			continue
		}

		// TODO: Uncomment below code after the delta shallow copy API is ready
		//// Rename the non-empty previous rebuilding lvol so that it can be reused later
		//if lvolName == rebuildingLvolName {
		//	if lvol.DriverSpecific.Lvol.NumAllocatedClusters > 0 {
		//		expiredLvolName := GenerateReplicaExpiredLvolName(r.Name)
		//		if _, err := spdkClient.BdevLvolRename(lvol.UUID, expiredLvolName); err != nil {
		//			r.log.WithError(err).Warnf("Failed to rename the previous rebuilding lvol %s to %s for dst replica %v rebuilding start, will try to remove it instead", lvolName, expiredLvolName, r.Name)
		//		} else {
		//			continue
		//		}
		//	}
		//}

		// If an extra snapshot lvol has multiple children, decoupling it from its children before deletion
		if len(lvol.DriverSpecific.Lvol.Clones) > 1 {
			for _, childLvolName := range lvol.DriverSpecific.Lvol.Clones {
				if _, err := spdkClient.BdevLvolDetachParent(childLvolName); err != nil {
					return "", err
				}
			}
		}
		if _, err := spdkClient.BdevLvolDelete(lvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return "", err
		}
		r.log.Debugf("Replica found and deleted the redundant lvol %s(%s) for dst replica %v rebuilding start", lvol.Aliases[0], lvol.UUID, r.Name)
	}

	r.rebuildingDstCache.rebuildingError = ""
	r.rebuildingDstCache.rebuildingState = types.ProgressStateInProgress

	r.rebuildingDstCache.processingSnapshotName = ""
	r.rebuildingDstCache.processingState = types.ProgressStateStarting
	r.rebuildingDstCache.processingSize = 0
	r.rebuildingDstCache.processedSnapshotList = make([]string, 0, len(rebuildingSnapshotList))
	r.rebuildingDstCache.processedSnapshotsSize = 0

	r.isRebuilding = true

	r.log.Infof("Replica created a new head %s(%s) based on the external snapshot %s(%s)(%s) from healthy replica %s for rebuilding start", r.Head.Alias, dstHeadLvolAddress, externalSnapshotName, r.rebuildingDstCache.externalSnapshotBdevName, externalSnapshotAddress, srcReplicaName)

	return dstHeadLvolAddress, nil
}

// RebuildingDstFinish asks the dst replica to switch the parent of earliest lvol of the dst replica from the external src snapshot to the rebuilt snapshot then detach that external src snapshot (if necessary).
// The engine should guarantee that there is no IO during the parent switch.
func (r *Replica) RebuildingDstFinish(spdkClient *spdkclient.Client) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateRunning {
		return fmt.Errorf("invalid state %v for replica %s rebuilding finish", r.State, r.Name)
	}
	if !r.isRebuilding {
		return fmt.Errorf("replica %s is not in rebuilding", r.Name)
	}

	defer func() {
		if err != nil {
			if r.State != types.InstanceStateError {
				r.State = types.InstanceStateError
			}
			r.ErrorMsg = err.Error()
			if r.rebuildingDstCache.rebuildingError == "" {
				r.rebuildingDstCache.rebuildingError = err.Error()
				r.rebuildingDstCache.rebuildingState = types.ProgressStateError
			}
		} else {
			if r.State != types.InstanceStateError {
				r.ErrorMsg = ""
			}
		}

		// Mark the rebuilding as complete after construction done
		r.isRebuilding = false

		updateRequired = true
	}()

	if len(r.ActiveChain) < 2 {
		return fmt.Errorf("invalid chain length %d for dst replica %v rebuilding finish", len(r.ActiveChain), r.Name)
	}

	// Switch from the external snapshot to use rebuilt snapshots
	if r.rebuildingDstCache.rebuildingError == "" {
		// Probably this lvol is the head
		firstLvolAfterRebuilding := r.ActiveChain[1]
		if firstLvolAfterRebuilding == nil {
			return fmt.Errorf("cannot find the head or the first snapshot since rebuilding start for replica %s rebuilding finish", r.Name)
		}
		if _, err := spdkClient.BdevLvolSetParent(firstLvolAfterRebuilding.Alias, spdktypes.GetLvolAlias(r.LvsName, GetReplicaSnapshotLvolName(r.Name, r.rebuildingDstCache.externalSnapshotName))); err != nil {
			return err
		}
		firstLvolAfterRebuilding.Parent = GetReplicaSnapshotLvolName(r.Name, r.rebuildingDstCache.externalSnapshotName)

		if r.rebuildingDstCache.processedSnapshotsSize != r.rebuildingDstCache.rebuildingSize {
			r.log.Warnf("Replica rebuilding spec size %d does not match the total processed snapshots size %d when during the dst rebuilding finish", r.rebuildingDstCache.rebuildingSize, r.rebuildingDstCache.processedSnapshotsSize)
			r.rebuildingDstCache.processedSnapshotsSize = r.rebuildingDstCache.rebuildingSize
		}
	}

	_ = r.doCleanupForRebuildingDst(spdkClient)

	bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
	if err != nil {
		return err
	}
	if err = r.construct(bdevLvolMap); err != nil {
		return err
	}

	r.rebuildingDstCache.processingState = types.ProgressStateComplete
	r.rebuildingDstCache.rebuildingState = types.ProgressStateComplete

	return nil
}

// doCleanupForRebuildingDst blindly cleans up the dst replica rebuilding cache and all redundant lvols if any
// Option cleanupRequired should be set to true if Longhorn does not want to reuse this dst replica for the next fast rebuilding, which typically means the replica removal
func (r *Replica) doCleanupForRebuildingDst(spdkClient *spdkclient.Client) error {
	aggregatedErrors := []error{}
	if r.rebuildingDstCache.externalSnapshotBdevName != "" {
		if err := disconnectNVMfBdev(spdkClient, r.rebuildingDstCache.externalSnapshotBdevName); err != nil {
			r.log.WithError(err).Errorf("Failed to disconnect the external src snapshot bdev %s for rebuilding dst cleanup, will continue", r.rebuildingDstCache.externalSnapshotBdevName)
			aggregatedErrors = append(aggregatedErrors, err)
		} else {
			r.rebuildingDstCache.srcReplicaName = ""
			r.rebuildingDstCache.srcReplicaAddress = ""
			r.rebuildingDstCache.externalSnapshotName = ""
			r.rebuildingDstCache.externalSnapshotBdevName = ""
		}
	}

	// Blindly clean up the rebuilding lvol and the exposed port
	rebuildingLvolName := GetReplicaRebuildingLvolName(r.Name)
	if r.rebuildingDstCache.rebuildingLvol != nil && r.rebuildingDstCache.rebuildingLvol.Name != rebuildingLvolName {
		err := fmt.Errorf("BUG: replica %s rebuilding lvol actual name %s does not match the expected name %v, will use the actual name for the cleanup", r.Name, r.rebuildingDstCache.rebuildingLvol.Name, rebuildingLvolName)
		r.log.Error(err)
		aggregatedErrors = append(aggregatedErrors, err)
		rebuildingLvolName = r.rebuildingDstCache.rebuildingLvol.Name
	}
	if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(rebuildingLvolName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		r.log.WithError(err).Errorf("Failed to stop exposing the rebuilding lvol %s for rebuilding dst cleanup, will continue", rebuildingLvolName)
		aggregatedErrors = append(aggregatedErrors, err)
	}
	if r.rebuildingDstCache.rebuildingPort != 0 {
		if err := r.portAllocator.ReleaseRange(r.rebuildingDstCache.rebuildingPort, r.rebuildingDstCache.rebuildingPort); err != nil {
			r.log.WithError(err).Errorf("Failed to release the rebuilding port %d for rebuilding dst cleanup, will continue", r.rebuildingDstCache.rebuildingPort)
			aggregatedErrors = append(aggregatedErrors, err)
		} else {
			r.rebuildingDstCache.rebuildingPort = 0
			r.rebuildingDstCache.rebuildingLvolAddress = ""
		}
	}
	if _, err := spdkClient.BdevLvolDelete(spdktypes.GetLvolAlias(r.LvsName, rebuildingLvolName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		r.log.WithError(err).Errorf("Failed to delete the rebuilding lvol %s for rebuilding dst cleanup, will continue", rebuildingLvolName)
		aggregatedErrors = append(aggregatedErrors, err)
	} else {
		r.rebuildingDstCache.rebuildingLvol = nil
	}

	// Remove redundant lvols if any at the end of a rebuilding.
	if len(r.rebuildingDstCache.rebuildingSnapshotMap) > 0 {
		bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
		if err != nil {
			return err
		}
		chainLvolMap := map[string]*Lvol{}
		for _, inChainLvol := range r.ActiveChain {
			if inChainLvol == nil {
				continue
			}
			chainLvolMap[inChainLvol.Name] = inChainLvol
		}
		for lvolName, lvol := range bdevLvolMap {
			if lvolName == r.Name || IsRebuildingLvol(lvolName) || IsReplicaExpiredLvol(r.Name, lvolName) {
				continue
			}
			if chainLvolMap[lvolName] != nil {
				continue
			}
			if r.rebuildingDstCache.rebuildingSnapshotMap[GetSnapshotNameFromReplicaSnapshotLvolName(r.Name, lvolName)] != nil {
				continue
			}
			if len(lvol.DriverSpecific.Lvol.Clones) > 1 {
				for _, childLvolName := range lvol.DriverSpecific.Lvol.Clones {
					if childLvolName == r.Name || r.rebuildingDstCache.rebuildingSnapshotMap[GetSnapshotNameFromReplicaSnapshotLvolName(r.Name, childLvolName)] != nil {
						return fmt.Errorf("found a valid lvol %s in the redundant lvol %s children list for replica %s rebuilding cleanup", childLvolName, lvolName, r.Name)
					}
					if _, err := spdkClient.BdevLvolDetachParent(childLvolName); err != nil {
						return err
					}
				}
			}
			if _, err := spdkClient.BdevLvolDelete(lvol.Aliases[0]); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				return err
			}
			r.log.Debugf("Replica found and deleted the redundant lvol %s(%s) for dst replica %v rebuilding cleanup", lvol.Aliases[0], lvol.UUID, r.Name)
		}
	}

	r.rebuildingDstCache.rebuildingSnapshotMap = map[string]*api.Lvol{}
	r.rebuildingDstCache.rebuildingSize = 0
	r.rebuildingDstCache.rebuildingError = ""
	r.rebuildingDstCache.rebuildingState = ""
	r.rebuildingDstCache.processedSnapshotList = make([]string, 0)
	r.rebuildingDstCache.processedSnapshotsSize = 0
	r.rebuildingDstCache.processingSnapshotName = ""
	r.rebuildingDstCache.processingSize = 0
	r.rebuildingDstCache.processingState = ""

	return util.CombineErrors(aggregatedErrors...)
}

// rebuildingDstShallowCopyPrepare creates a new rebuilding lvol or renames an existing expired lvol as the rebuilding lvol for the dst replica.
func (r *Replica) rebuildingDstShallowCopyPrepare(spdkClient *spdkclient.Client, snapshotName string) (dstRebuildingLvolAddress string, err error) {
	rebuildingLvolName := GetReplicaRebuildingLvolName(r.Name)

	dstSnapshotParentLvolName := ""
	srcSnapSvcLvol := r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName]
	if srcSnapSvcLvol == nil {
		return "", fmt.Errorf("cannot find snapshot %s in the rebuilding snapshot list for replica %s shallow copy prepare", snapshotName, r.Name)
	}
	if srcSnapSvcLvol.Parent != "" {
		if r.BackingImage != nil && r.BackingImage.Name == srcSnapSvcLvol.Parent {
			dstSnapshotParentLvolName = srcSnapSvcLvol.Parent
		} else {
			dstSnapshotParentLvolName = GetReplicaSnapshotLvolName(r.Name, srcSnapSvcLvol.Parent)
		}
	}

	// Blindly clean up the existing rebuilding lvol
	if r.rebuildingDstCache.rebuildingPort != 0 {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(rebuildingLvolName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return "", err
		}
	}
	if r.rebuildingDstCache.rebuildingLvol != nil {
		if _, err := spdkClient.BdevLvolDelete(r.rebuildingDstCache.rebuildingLvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return "", err
		}
	}
	r.rebuildingDstCache.rebuildingLvol = nil
	rebuildingLvolCreated := false

	bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
	if err != nil {
		return "", err
	}
	dstSnapshotLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	if bdevLvolMap[dstSnapshotLvolName] != nil { // If there is an existing snapshot lvol, clone a rebuilding lvol behinds it
		// Otherwise, try to reuse the existing lvol
		// TODO: For a corrupted or outdated snapshot lvol, we should create a rebuilding lvol before delete it. So that its clusters will be coalesced into the rebuilding lvol, then we can start a delta shallow copy
		// TODO: In the future, we can move the below logic to r.RebuildingDstShallowCopyStart after the delta shallow copy API is ready
		dstSnapBdevLvol := bdevLvolMap[dstSnapshotLvolName]
		snaplvolSnapshotTimestamp := dstSnapBdevLvol.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotTimestamp]
		snaplvolActualSize := dstSnapBdevLvol.DriverSpecific.Lvol.NumAllocatedClusters * defaultClusterSize
		isIntactSnap := srcSnapSvcLvol.SnapshotTimestamp == snaplvolSnapshotTimestamp &&
			srcSnapSvcLvol.ActualSize == snaplvolActualSize &&
			srcSnapSvcLvol.SnapshotChecksum != "" && srcSnapSvcLvol.SnapshotChecksum == dstSnapBdevLvol.DriverSpecific.Lvol.Xattrs[spdkclient.SnapshotChecksum]
		// For now directly delete the corrupted or outdated snapshot lvol and start a full shallow copy since we cannot validate existing data during the shallow copy
		if !isIntactSnap {
			for _, childLvolName := range dstSnapBdevLvol.DriverSpecific.Lvol.Clones {
				if _, err := spdkClient.BdevLvolDetachParent(spdktypes.GetLvolAlias(r.LvsName, childLvolName)); err != nil {
					return "", errors.Wrapf(err, "failed to decouple the child lvol %s from the corrupted or outdated snapshot lvol %s for dst replica %v rebuilding snapshot %s shallow copy prepare", childLvolName, dstSnapshotLvolName, r.Name, snapshotName)
				}
			}
			if _, err := spdkClient.BdevLvolDelete(dstSnapBdevLvol.Aliases[0]); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				return "", errors.Wrapf(err, "failed to delete the corrupted or outdated snapshot lvol %s for dst replica %v rebuilding snapshot %s shallow copy prepare", dstSnapshotLvolName, r.Name, snapshotName)
			}
			r.log.Infof("Replica found a corrupted or outdated snapshot lvol %s and delete it before the shallow copy", GetReplicaRebuildingLvolName(r.Name))
		} else {
			snapLvolAlias := spdktypes.GetLvolAlias(r.LvsName, dstSnapshotLvolName)
			if _, err = spdkClient.BdevLvolClone(snapLvolAlias, rebuildingLvolName); err != nil {
				return "", errors.Wrapf(err, "failed to clone rebuilding lvol %s behinds the existing snapshot lvol %s for dst replica %v rebuilding snapshot %s shallow copy prepare", rebuildingLvolName, snapLvolAlias, r.Name, snapshotName)
			}
			r.log.Infof("Replica found an intact snapshot lvol %s before the shallow copy", snapLvolAlias)
			rebuildingLvolCreated = true
		}
	} else { // Then check if there is an expired lvol available
		if bdevLvolMap[dstSnapshotParentLvolName] != nil { // For non-ancestor snapshot, check if dstSnapshotParentLvol has an expired lvol as child
			for _, childLvolName := range bdevLvolMap[dstSnapshotParentLvolName].DriverSpecific.Lvol.Clones {
				if IsReplicaExpiredLvol(r.Name, childLvolName) {
					if _, err := spdkClient.BdevLvolRename(spdktypes.GetLvolAlias(r.LvsName, childLvolName), rebuildingLvolName); err != nil {
						r.log.WithError(err).Warnf("Failed to rename the previous expired lvol %s to rebuilding lvol %s for dst replica %v rebuilding snapshot %s shallow copy prepare, will ignore it and continue", childLvolName, rebuildingLvolName, r.Name, snapshotName)
					} else {
						rebuildingLvolCreated = true
						r.log.Infof("Replica found an expired lvol %s (with parent %s) and renamed it to rebuilding lvol %s for dst replica %v rebuilding snapshot %s shallow copy prepare", childLvolName, dstSnapshotParentLvolName, rebuildingLvolName, r.Name, snapshotName)
					}
					break
				}
			}
		} else { // For the ancestor snapshot, check if there is an orphan expired lvol
			for lvolName, bdevLvol := range bdevLvolMap {
				if IsReplicaExpiredLvol(r.Name, lvolName) && bdevLvol.DriverSpecific.Lvol.BaseSnapshot == "" {
					if _, err := spdkClient.BdevLvolRename(spdktypes.GetLvolAlias(r.LvsName, lvolName), rebuildingLvolName); err != nil {
						r.log.WithError(err).Warnf("Failed to rename the previous expired lvol %s to rebuilding lvol %s for dst replica %v rebuilding snapshot %s shallow copy prepare, will ignore it and continue", lvolName, rebuildingLvolName, r.Name, snapshotName)
					} else {
						rebuildingLvolCreated = true
						r.log.Infof("Replica found an orphan expired lvol %s (with empty parent) and renamed it to rebuilding lvol %s for dst replica %v rebuilding snapshot %s shallow copy prepare", lvolName, rebuildingLvolName, r.Name, snapshotName)
					}
					break
				}
			}
		}
	}

	if !rebuildingLvolCreated {
		if dstSnapshotParentLvolName != "" && bdevLvolMap[dstSnapshotParentLvolName] != nil {
			if _, err = spdkClient.BdevLvolClone(spdktypes.GetLvolAlias(r.LvsName, dstSnapshotParentLvolName), rebuildingLvolName); err != nil {
				return "", err
			}
		} else {
			if _, err = spdkClient.BdevLvolCreate("", r.LvsUUID, rebuildingLvolName, util.BytesToMiB(r.SpecSize), "", true); err != nil {
				return "", err
			}
		}
	}

	rebuildingLvolAlias := spdktypes.GetLvolAlias(r.LvsName, rebuildingLvolName)
	rebuildingBdevLvol, err := spdkClient.BdevLvolGetByName(rebuildingLvolAlias, 0)
	if err != nil {
		return "", err
	}
	r.rebuildingDstCache.rebuildingLvol = BdevLvolInfoToServiceLvol(&rebuildingBdevLvol)

	if srcSnapSvcLvol.SpecSize != r.rebuildingDstCache.rebuildingLvol.SpecSize {
		if _, err := spdkClient.BdevLvolResize(r.rebuildingDstCache.rebuildingLvol.Alias, util.BytesToMiB(srcSnapSvcLvol.SpecSize)); err != nil {
			return "", err
		}
		r.rebuildingDstCache.rebuildingLvol.SpecSize = srcSnapSvcLvol.SpecSize
	}

	dstRebuildingLvolAddress = r.rebuildingDstCache.rebuildingLvol.Alias
	if r.rebuildingDstCache.rebuildingPort != 0 {
		nguid := commonutils.RandomID(nvmeNguidLength)
		if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(r.rebuildingDstCache.rebuildingLvol.Name), r.rebuildingDstCache.rebuildingLvol.UUID, nguid, r.IP, strconv.Itoa(int(r.rebuildingDstCache.rebuildingPort))); err != nil {
			return "", err
		}
		dstRebuildingLvolAddress = net.JoinHostPort(r.IP, strconv.Itoa(int(r.rebuildingDstCache.rebuildingPort)))
	}
	r.rebuildingDstCache.rebuildingLvolAddress = dstRebuildingLvolAddress

	r.log.Infof("Rebuilding destination replica prepared its rebuilding lvol %s(%s) with parent %s for snapshot %s and expose it to %s", r.rebuildingDstCache.rebuildingLvol.Alias, r.rebuildingDstCache.rebuildingLvol.UUID, r.rebuildingDstCache.rebuildingLvol.Parent, snapshotName, dstRebuildingLvolAddress)

	return dstRebuildingLvolAddress, nil
}

// RebuildingDstShallowCopyStart let the dst replica ask the src replica to start a shallow copy from a snapshot to the rebuilding lvol.
// Each time before starting a shallow copy, the dst replica will prepare a new rebuilding lvol and expose it as a NVMf bdev.
func (r *Replica) RebuildingDstShallowCopyStart(spdkClient *spdkclient.Client, snapshotName string) (err error) {
	r.Lock()
	defer r.Unlock()

	defer func() {
		if err != nil {
			r.rebuildingDstCache.rebuildingError = err.Error()
			r.rebuildingDstCache.rebuildingState = types.ProgressStateError
			r.rebuildingDstCache.processingState = types.ProgressStateError
		}
	}()

	dstRebuildingLvolAddress, err := r.rebuildingDstShallowCopyPrepare(spdkClient, snapshotName)
	if err != nil {
		return err
	}

	srcSnapSvcLvol := r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName]
	if srcSnapSvcLvol == nil {
		return fmt.Errorf("cannot find snapshot %s in the rebuilding snapshot list for replica %s shallow copy start", snapshotName, r.Name)
	}
	r.rebuildingDstCache.processingSnapshotName = snapshotName
	r.rebuildingDstCache.processingSize = 0
	r.rebuildingDstCache.processingState = types.ProgressStateInProgress

	srcReplicaServiceCli, err := GetServiceClient(r.rebuildingDstCache.srcReplicaAddress)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := srcReplicaServiceCli.Close(); errClose != nil {
			r.log.WithError(errClose).Errorf("Failed to close replica %s client with address %s during start rebuilding dst shallow copy", r.rebuildingDstCache.srcReplicaName, r.rebuildingDstCache.srcReplicaAddress)
		}
	}()

	dstSnapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	dstSnapBdevLvol, err := spdkClient.BdevLvolGetByName(spdktypes.GetLvolAlias(r.LvsName, dstSnapLvolName), 0)
	if err != nil {
		if !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		// Directly start a shallow copy when there is no existing snapshot lvol
		return srcReplicaServiceCli.ReplicaRebuildingSrcShallowCopyStart(r.rebuildingDstCache.srcReplicaName, snapshotName, dstRebuildingLvolAddress)
	}

	// Otherwise, try to reuse the existing lvol
	r.log.Infof("Replica directly reused an intact snapshot lvol %s then skipped the shallow copy", dstSnapLvolName)

	// Need to manually update the progress after reuse the existing snapshot lvol
	r.rebuildingDstCache.processingState = types.ProgressStateComplete
	r.rebuildingDstCache.processingSize = dstSnapBdevLvol.DriverSpecific.Lvol.NumAllocatedClusters * defaultClusterSize

	return nil
}

// RebuildingDstShallowCopyCheck let the dst replica ask the src replica to retrieve the shallow copy status based on the cached info.
func (r *Replica) RebuildingDstShallowCopyCheck(spdkClient *spdkclient.Client) (ret *spdkrpc.ReplicaRebuildingDstShallowCopyCheckResponse, err error) {
	r.Lock()
	defer r.Unlock()

	snapApiLvol := r.rebuildingDstCache.rebuildingSnapshotMap[r.rebuildingDstCache.processingSnapshotName]

	ret = &spdkrpc.ReplicaRebuildingDstShallowCopyCheckResponse{
		SrcReplicaName:    r.rebuildingDstCache.srcReplicaName,
		SrcReplicaAddress: r.rebuildingDstCache.srcReplicaAddress,
		SnapshotName:      r.rebuildingDstCache.processingSnapshotName,
	}

	// Allow checking the rebuilding record even if the rebuilding hasn't started or is already complete
	if !r.isRebuilding {
		ret.Error = r.rebuildingDstCache.rebuildingError
		ret.TotalState = r.rebuildingDstCache.rebuildingState
		ret.State = r.rebuildingDstCache.rebuildingState
		if r.rebuildingDstCache.rebuildingState == types.ProgressStateComplete {
			ret.Progress = 100
			ret.TotalProgress = 100
		} else {
			if snapApiLvol != nil && snapApiLvol.ActualSize != 0 {
				ret.Progress = uint32(float64(r.rebuildingDstCache.processingSize) / float64(snapApiLvol.ActualSize) * 100)
			}
			if r.rebuildingDstCache.rebuildingSize != 0 {
				ret.TotalProgress = uint32(float64(r.rebuildingDstCache.processingSize+r.rebuildingDstCache.processedSnapshotsSize) / float64(r.rebuildingDstCache.rebuildingSize) * 100)
			}
		}
		return ret, nil
	}

	// The dst replica has not started the shallow copy yet
	if r.rebuildingDstCache.processingState == types.ProgressStateStarting || r.rebuildingDstCache.processingSnapshotName == "" {
		return ret, nil
	}

	if snapApiLvol == nil {
		r.rebuildingDstCache.rebuildingError = fmt.Errorf("cannot find snapshot %s in the rebuilding snapshot list for shallow copy check", r.rebuildingDstCache.processingSnapshotName).Error()
		r.rebuildingDstCache.rebuildingState = types.ProgressStateError
		r.rebuildingDstCache.processingState = types.ProgressStateError
	}

	// If the processing shallow copy is already state complete or error, we cannot send the check request to the src replica again as spdk_tgt of the src replica has cleaned up the shallow copy op.
	if r.rebuildingDstCache.rebuildingState == types.ProgressStateInProgress &&
		r.rebuildingDstCache.processingState != types.ProgressStateComplete && r.rebuildingDstCache.processingState != types.ProgressStateError {
		srcReplicaServiceCli, err := GetServiceClient(r.rebuildingDstCache.srcReplicaAddress)
		if err != nil {
			return nil, err
		}
		defer func() {
			if errClose := srcReplicaServiceCli.Close(); errClose != nil {
				r.log.WithError(errClose).Errorf("Failed to close replica %s client with address %s during check rebuilding dst shallow copy", r.rebuildingDstCache.srcReplicaName, r.rebuildingDstCache.srcReplicaAddress)
			}
		}()

		state, copiedClusters, totalClusters, errorMsg, err := srcReplicaServiceCli.ReplicaRebuildingSrcShallowCopyCheck(r.rebuildingDstCache.srcReplicaName, r.Name, r.rebuildingDstCache.processingSnapshotName)
		if err != nil {
			errorMsg = errors.Wrapf(err, "dst replica %s failed to check the shallow copy status from src replica %s for snapshot %s", r.Name, r.rebuildingDstCache.srcReplicaName, r.rebuildingDstCache.processingSnapshotName).Error()
		}
		if errorMsg == "" && snapApiLvol.ActualSize != totalClusters*defaultClusterSize {
			errorMsg = fmt.Errorf("rebuilding dst snapshot %s recorded actual size %d does not match the shallow copy reported total size %d", r.rebuildingDstCache.processingSnapshotName, snapApiLvol.ActualSize, totalClusters*defaultClusterSize).Error()
		}
		if errorMsg != "" {
			if r.rebuildingDstCache.rebuildingError == "" {
				r.rebuildingDstCache.rebuildingError = errorMsg
			}
			r.rebuildingDstCache.rebuildingState = types.ProgressStateError
			r.rebuildingDstCache.processingState = types.ProgressStateError
		} else {
			r.rebuildingDstCache.processingState = state
			r.rebuildingDstCache.processingSize = copiedClusters * defaultClusterSize
		}
	}

	if r.rebuildingDstCache.rebuildingError == "" {
		ret.State = r.rebuildingDstCache.processingState
		ret.TotalState = types.ProgressStateInProgress
		if snapApiLvol.ActualSize == 0 {
			ret.Progress = 100
		} else {
			ret.Progress = uint32(float64(r.rebuildingDstCache.processingSize) / float64(snapApiLvol.ActualSize) * 100)
		}
		if r.rebuildingDstCache.rebuildingSize == 0 {
			ret.TotalProgress = 100
		} else {
			ret.TotalProgress = uint32(float64(r.rebuildingDstCache.processingSize+r.rebuildingDstCache.processedSnapshotsSize) / float64(r.rebuildingDstCache.rebuildingSize) * 100)
		}
	} else {
		r.rebuildingDstCache.rebuildingState = types.ProgressStateError
		ret.Error = r.rebuildingDstCache.rebuildingError
		ret.State = types.InstanceStateError
		ret.TotalState = types.ProgressStateError
		if snapApiLvol == nil || snapApiLvol.ActualSize == 0 {
			ret.Progress = 0
		} else {
			ret.Progress = uint32(float64(r.rebuildingDstCache.processingSize) / float64(snapApiLvol.ActualSize) * 100)
		}
		if r.rebuildingDstCache.rebuildingSize == 0 {
			ret.TotalProgress = 0
		} else {
			ret.TotalProgress = uint32(float64(r.rebuildingDstCache.processingSize+r.rebuildingDstCache.processedSnapshotsSize) / float64(r.rebuildingDstCache.rebuildingSize) * 100)
		}
	}

	return ret, nil
}

// RebuildingDstSnapshotCreate creates a snapshot lvol based on the rebuilding lvol for the dst replica during the rebuilding process
func (r *Replica) RebuildingDstSnapshotCreate(spdkClient *spdkclient.Client, snapshotName string, opts *api.SnapshotOptions) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateRunning {
		return fmt.Errorf("invalid state %v for dst replica %s rebuilding snapshot %s creation", r.State, r.Name, snapshotName)
	}
	if !r.isRebuilding {
		return fmt.Errorf("replica %s is not in rebuilding", r.Name)
	}
	if r.rebuildingDstCache.rebuildingLvol == nil {
		return fmt.Errorf("rebuilding lvol is not existed for dst replica %s rebuilding snapshot %s creation", r.Name, snapshotName)
	}

	defer func() {
		if err != nil {
			if r.State != types.InstanceStateError {
				r.State = types.InstanceStateError
				updateRequired = true
			}
			r.ErrorMsg = err.Error()
		} else {
			if r.State != types.InstanceStateError {
				r.ErrorMsg = ""
			}
		}
	}()

	var snapSvcLvol *Lvol
	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	snapBdevLvol, err := spdkClient.BdevLvolGetByName(spdktypes.GetLvolAlias(r.LvsName, snapLvolName), 0)
	if err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}
	if snapBdevLvol.UUID != "" { // If there is an existing snapshot lvol getting reused, check and correct its parent
		snapSvcLvol = BdevLvolInfoToServiceLvol(&snapBdevLvol)
		srcSnapSvcLvol := r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName]
		if srcSnapSvcLvol == nil {
			return fmt.Errorf("cannot find snapshot %s in the rebuilding snapshot list during dst replica %s rebuilding snapshot creation", snapshotName, r.Name)
		}
		// Guarantee the reused snapshot lvol has the correct parent
		if GetSnapshotNameFromReplicaSnapshotLvolName(r.Name, snapSvcLvol.Parent) != srcSnapSvcLvol.Parent {
			if srcSnapSvcLvol.Parent == "" {
				if _, err := spdkClient.BdevLvolDetachParent(snapSvcLvol.Alias); err != nil {
					return err
				}
			} else if !IsReplicaLvol(r.rebuildingDstCache.srcReplicaName, srcSnapSvcLvol.Parent) { // The parent should be the backing image
				if _, err := spdkClient.BdevLvolSetParent(snapSvcLvol.Alias, spdktypes.GetLvolAlias(r.LvsName, srcSnapSvcLvol.Parent)); err != nil {
					return err
				}
			} else { // The parent should be a regular snapshot lvol
				if _, err := spdkClient.BdevLvolSetParent(snapSvcLvol.Alias, spdktypes.GetLvolAlias(r.LvsName, GetReplicaSnapshotLvolName(r.Name, srcSnapSvcLvol.Parent))); err != nil {
					return err
				}
			}
		}

		r.log.Infof("Reused an existing snapshot %s(%s) for rebuilding dst", snapSvcLvol.Alias, snapSvcLvol.UUID)
	} else { // Otherwise, create a new snapshot lvol for the rebuilding lvol
		var xattrs []spdkclient.Xattr
		if opts != nil {
			userCreated := spdkclient.Xattr{
				Name:  spdkclient.UserCreated,
				Value: strconv.FormatBool(opts.UserCreated),
			}
			xattrs = append(xattrs, userCreated)

			snapshotTimestamp := spdkclient.Xattr{
				Name:  spdkclient.SnapshotTimestamp,
				Value: opts.Timestamp,
			}
			xattrs = append(xattrs, snapshotTimestamp)
		}

		snapUUID, err := spdkClient.BdevLvolSnapshot(r.rebuildingDstCache.rebuildingLvol.UUID, snapLvolName, xattrs)
		if err != nil {
			return err
		}

		snapBdevLvol, err := spdkClient.BdevLvolGetByName(snapUUID, 0)
		if err != nil {
			return err
		}
		snapSvcLvol = BdevLvolInfoToServiceLvol(&snapBdevLvol)

		if r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName] == nil {
			return fmt.Errorf("cannot find snapshot %s in the rebuilding snapshot list for replica %s rebuilding snapshot creation", snapshotName, r.Name)
		}
		if r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName].ActualSize != snapSvcLvol.ActualSize {
			return fmt.Errorf("rebuilding dst newly rebuilt snapshot %s(%s) actual size %d does not match the corresponding rebuilding src snapshot %s(%s) actual size %d", snapSvcLvol.Name, snapSvcLvol.UUID, snapSvcLvol.ActualSize, r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName].Name, r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName].UUID, r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName].ActualSize)
		}

		r.log.Infof("Created a new snapshot %s(%s) for rebuilding dst", snapSvcLvol.Alias, snapSvcLvol.UUID)
	}

	// Blindly clean up the existing rebuilding lvol after each rebuilding dst replica snapshot creation
	rebuildingLvolName := GetReplicaRebuildingLvolName(r.Name)
	if r.rebuildingDstCache.rebuildingPort != 0 {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(rebuildingLvolName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
	}
	if r.rebuildingDstCache.rebuildingLvol != nil {
		if _, err := spdkClient.BdevLvolDelete(r.rebuildingDstCache.rebuildingLvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		r.rebuildingDstCache.rebuildingLvol = nil
	}

	// Do not update r.ActiveChain for the rebuilding snapshots here.
	// The replica will directly reconstruct r.ActiveChain as well as r.SnapshotLvolMap during the rebuilding dst finish.
	r.rebuildingDstCache.processedSnapshotList = append(r.rebuildingDstCache.processedSnapshotList, snapshotName)
	r.rebuildingDstCache.processedSnapshotsSize += snapSvcLvol.ActualSize

	r.rebuildingDstCache.processingState = types.ProgressStateStarting
	r.rebuildingDstCache.processingSnapshotName = ""
	r.rebuildingDstCache.processingSize = 0
	updateRequired = true

	return nil
}

func (r *Replica) BackupRestore(spdkClient *spdkclient.Client, backupUrl, snapshotName string, credential map[string]string, concurrentLimit int32) (err error) {
	r.Lock()
	defer r.Unlock()

	defer func() {
		if err == nil {
			r.isRestoring = true
		}
	}()

	if r.isRestoring {
		return fmt.Errorf("cannot initiate backup restore as there is one already in progress")
	}

	backupType, err := butil.CheckBackupType(backupUrl)
	if err != nil {
		err = errors.Wrapf(err, "failed to check the type for restoring backup %v", backupUrl)
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	err = butil.SetupCredential(backupType, credential)
	if err != nil {
		err = errors.Wrapf(err, "failed to setup credential for restoring backup %v", backupUrl)
		return grpcstatus.Errorf(grpccodes.Internal, "%v", err)
	}

	backupName, _, _, err := backupstore.DecodeBackupURL(util.UnescapeURL(backupUrl))
	if err != nil {
		err = errors.Wrapf(err, "failed to decode backup url %v", backupUrl)
		return grpcstatus.Errorf(grpccodes.InvalidArgument, "%v", err)
	}

	if r.restore == nil {
		return grpcstatus.Errorf(grpccodes.NotFound, "restoration for backup %v is not initialized", backupUrl)
	}

	restore := r.restore.DeepCopy()
	if restore.State == btypes.ProgressStateError {
		return fmt.Errorf("cannot start restoring backup %v of the previous failed restoration", backupUrl)
	}

	if restore.LastRestored == backupName {
		return grpcstatus.Errorf(grpccodes.AlreadyExists, "already restored backup %v", backupName)
	}

	// Initialize `r.restore`
	// First restore request. It must be a normal full restore.
	if restore.LastRestored == "" && (restore.State == btypes.ProgressStateUndefined || restore.State == btypes.ProgressStateCanceled) {
		r.log.Infof("Starting a new restore for backup %v with restore state %v", backupUrl, restore.State)
		lvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
		r.restore, err = NewRestore(spdkClient, lvolName, snapshotName, backupUrl, backupName, r)
		if err != nil {
			err = errors.Wrap(err, "failed to start new restore")
			return grpcstatus.Errorf(grpccodes.Internal, "%v", err)
		}
	} else {
		r.log.Infof("Resetting the restore for backup %v", backupUrl)

		var lvolName string
		var snapshotNameToBeRestored string

		validLastRestoredBackup := r.canDoIncrementalRestore(restore, backupUrl, backupName)
		if validLastRestoredBackup {
			r.log.Infof("Starting an incremental restore for backup %v", backupUrl)
		} else {
			r.log.Infof("Starting a full restore for backup %v", backupUrl)
		}

		lvolName = GetReplicaSnapshotLvolName(r.Name, snapshotName)
		snapshotNameToBeRestored = snapshotName

		r.restore.StartNewRestore(backupUrl, backupName, lvolName, snapshotNameToBeRestored, validLastRestoredBackup)
	}

	// Initiate restore
	newRestore := r.restore.DeepCopy()
	defer func() {
		if err != nil { // nolint:staticcheck
			// TODO: Support snapshot revert for incremental restore
			r.log.WithError(err).Error("Failed to start backup restore")
		}
	}()

	isFullRestore := newRestore.LastRestored == ""

	defer func() {
		go func() {
			if err := r.completeBackupRestore(spdkClient, isFullRestore); err != nil {
				logrus.WithError(err).Warn("Failed to complete backup restore")
			}
		}()
	}()

	if isFullRestore {
		r.log.Infof("Starting a new full restore for backup %v", backupUrl)
		if err := r.backupRestore(backupUrl, newRestore.LvolName, concurrentLimit); err != nil {
			return errors.Wrapf(err, "failed to start full backup restore")
		}
		r.log.Infof("Successfully initiated full restore for %v to %v", backupUrl, newRestore.LvolName)
	} else {
		r.log.Infof("Starting an incremental restore for backup %v", backupUrl)
		if err := r.backupRestoreIncrementally(backupUrl, newRestore.LastRestored, newRestore.LvolName, concurrentLimit); err != nil {
			return errors.Wrapf(err, "failed to start incremental backup restore")
		}
		r.log.Infof("Successfully initiated incremental restore for %v to %v", backupUrl, newRestore.LvolName)
	}

	return nil

}

func (r *Replica) backupRestoreIncrementally(backupURL, lastRestored, snapshotLvolName string, concurrentLimit int32) error {
	backupURL = butil.UnescapeURL(backupURL)

	logrus.WithFields(logrus.Fields{
		"backupURL":        backupURL,
		"lastRestored":     lastRestored,
		"snapshotLvolName": snapshotLvolName,
		"concurrentLimit":  concurrentLimit,
	}).Info("Start restoring backup incrementally")

	return backupstore.RestoreDeltaBlockBackupIncrementally(r.ctx, &backupstore.DeltaRestoreConfig{
		BackupURL:       backupURL,
		DeltaOps:        r.restore,
		LastBackupName:  lastRestored,
		Filename:        snapshotLvolName,
		ConcurrentLimit: int32(concurrentLimit),
	})
}

func (r *Replica) backupRestore(backupURL, snapshotLvolName string, concurrentLimit int32) error {
	backupURL = butil.UnescapeURL(backupURL)

	logrus.WithFields(logrus.Fields{
		"backupURL":        backupURL,
		"snapshotLvolName": snapshotLvolName,
		"concurrentLimit":  concurrentLimit,
	}).Info("Start restoring backup")

	return backupstore.RestoreDeltaBlockBackup(r.ctx, &backupstore.DeltaRestoreConfig{
		BackupURL:       backupURL,
		DeltaOps:        r.restore,
		Filename:        snapshotLvolName,
		ConcurrentLimit: int32(concurrentLimit),
	})
}

func (r *Replica) canDoIncrementalRestore(restore *Restore, backupURL, requestedBackupName string) bool {
	if restore.LastRestored == "" {
		logrus.Warnf("There is a restore record in the server but last restored backup is empty with restore state is %v, will do full restore instead", restore.State)
		return false
	}
	if _, err := backupstore.InspectBackup(strings.Replace(backupURL, requestedBackupName, restore.LastRestored, 1)); err != nil {
		logrus.WithError(err).Warnf("The last restored backup %v becomes invalid for incremental restore, will do full restore instead", restore.LastRestored)
		return false
	}
	return true
}

func (r *Replica) completeBackupRestore(spdkClient *spdkclient.Client, isFullRestore bool) (err error) {
	defer func() {
		if extraErr := r.finishRestore(err); extraErr != nil {
			r.log.WithError(extraErr).Error("Failed to finish backup restore")
		}
	}()

	if err := r.waitForRestoreComplete(); err != nil {
		return errors.Wrapf(err, "failed to wait for restore complete")
	}

	r.RLock()
	restore := r.restore.DeepCopy()
	r.RUnlock()

	if isFullRestore {
		return r.postFullRestoreOperations(spdkClient, restore)
	}

	return r.postIncrementalRestoreOperations(spdkClient, restore)
}

func (r *Replica) waitForRestoreComplete() error {
	periodicChecker := time.NewTicker(time.Duration(restorePeriodicRefreshInterval.Seconds()) * time.Second)
	defer periodicChecker.Stop()

	for range periodicChecker.C {
		r.restore.RLock()
		restoreProgress := r.restore.Progress
		restoreError := r.restore.Error
		restoreState := r.restore.State
		r.restore.RUnlock()

		if restoreProgress == 100 {
			r.log.Info("Backup restoration completed successfully")
			return nil
		}
		if restoreState == btypes.ProgressStateCanceled {
			r.log.Info("Backup restoration is cancelled")
			return nil
		}
		if restoreError != "" {
			err := fmt.Errorf("%v", restoreError)
			r.log.WithError(err).Errorf("Found backup restoration error")
			return err
		}
	}
	return nil
}

func (r *Replica) postIncrementalRestoreOperations(spdkClient *spdkclient.Client, restore *Restore) error {
	r.log.Infof("Replacing snapshot %v of the restored volume", restore.SnapshotName)

	if r.restore.State == btypes.ProgressStateCanceled {
		r.log.Info("Doing nothing for canceled backup restoration")
		return nil
	}

	// Delete snapshot; SPDK will coalesce the content into the current head lvol.
	r.log.Infof("Deleting snapshot %v for snapshot replacement of the restored volume", restore.SnapshotName)
	_, err := r.SnapshotDelete(spdkClient, restore.SnapshotName)
	if err != nil {
		r.log.WithError(err).Error("Failed to delete snapshot of the restored volume")
		return errors.Wrapf(err, "failed to delete snapshot of the restored volume")
	}

	r.log.Infof("Creating snapshot %v for snapshot replacement of the restored volume", restore.SnapshotName)
	opts := &api.SnapshotOptions{
		UserCreated: false,
		Timestamp:   util.Now(),
	}
	_, err = r.SnapshotCreate(spdkClient, restore.SnapshotName, opts)
	if err != nil {
		r.log.WithError(err).Error("Failed to take snapshot of the restored volume")
		return errors.Wrapf(err, "failed to take snapshot of the restored volume")
	}

	r.log.Infof("Done running incremental restore %v to lvol %v", restore.BackupURL, restore.LvolName)
	return nil
}

func (r *Replica) postFullRestoreOperations(spdkClient *spdkclient.Client, restore *Restore) error {
	if r.restore.State == btypes.ProgressStateCanceled {
		r.log.Info("Doing nothing for canceled backup restoration")
		return nil
	}

	snapLvolName := GetReplicaSnapshotLvolName(r.Name, restore.SnapshotName)
	if _, exists := r.SnapshotLvolMap[snapLvolName]; exists {
		r.log.Infof("Deleting existing snapshot %v of the restored volume", snapLvolName)
		_, err := r.SnapshotDelete(spdkClient, restore.SnapshotName)
		if err != nil {
			r.log.WithError(err).Errorf("Failed to delete existing snapshot %v of the restored volume", snapLvolName)
			return errors.Wrapf(err, "failed to delete snapshot %v of the restored volume", snapLvolName)
		}
	}

	r.log.Infof("Taking snapshot %v of the restored volume", restore.SnapshotName)
	opts := &api.SnapshotOptions{
		UserCreated: false,
		Timestamp:   util.Now(),
	}
	_, err := r.SnapshotCreate(spdkClient, restore.SnapshotName, opts)
	if err != nil {
		r.log.WithError(err).Error("Failed to take snapshot of the restored volume")
		return errors.Wrapf(err, "failed to take snapshot of the restored volume")
	}

	r.log.Infof("Done running full restore %v to lvol %v (snapshot %v)", restore.BackupURL, restore.LvolName, restore.SnapshotName)
	return nil
}

func (r *Replica) finishRestore(restoreErr error) error {
	r.Lock()
	defer r.Unlock()

	defer func() {
		if r.restore == nil {
			return
		}
		if restoreErr != nil {
			r.restore.UpdateRestoreStatus(r.restore.LvolName, 0, restoreErr)
			return
		}
		r.restore.FinishRestore()
	}()

	if !r.isRestoring {
		err := fmt.Errorf("BUG: volume is not being restored")
		if restoreErr != nil {
			restoreErr = util.CombineErrors(err, restoreErr)
		} else {
			restoreErr = err
		}
		return err
	}

	r.log.Infof("Unflagging isRestoring")
	r.isRestoring = false

	return nil
}

func (r *Replica) SetErrorState() {
	needUpdate := false

	r.Lock()
	defer func() {
		r.Unlock()

		if needUpdate {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateStopped && r.State != types.InstanceStateError {
		r.State = types.InstanceStateError
		needUpdate = true
	}
}

// CleanupLvolTree retrieves the lvol tree with BFS. Then try its best effort to do cleanup bottom up.
func (r *Replica) CleanupLvolTree(spdkClient *spdkclient.Client, rootLvolName string, bdevLvolMap map[string]*spdktypes.BdevInfo) {
	var queue []*spdktypes.BdevInfo
	if bdevLvolMap[rootLvolName] != nil {
		queue = []*spdktypes.BdevInfo{bdevLvolMap[rootLvolName]}
	}
	for idx := 0; idx < len(queue); idx++ {
		for _, childLvolName := range queue[idx].DriverSpecific.Lvol.Clones {
			if bdevLvolMap[childLvolName] != nil {
				queue = append(queue, bdevLvolMap[childLvolName])
			}
		}
	}
	for idx := len(queue) - 1; idx >= 0; idx-- {
		// This may fail since there may be a rebuilding failed replicas on the same host that leaves an orphan rebuilding lvol as a child of a snapshot lvol.
		// Then this snapshot lvol would have multiple children then cannot be deleted.
		if _, err := spdkClient.BdevLvolDelete(queue[idx].UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			r.log.WithError(err).Errorf("Failed to delete lvol %v(%v) from the lvol tree with root %v(%s), this lvol may accidentally have some leftover orphans children %+v, will continue", queue[idx].Aliases[0], queue[idx].UUID, bdevLvolMap[rootLvolName].Aliases[0], bdevLvolMap[rootLvolName].UUID, queue[idx].DriverSpecific.Lvol.Clones)
		}
	}
}
