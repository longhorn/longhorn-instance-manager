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
	btypes "github.com/longhorn/backupstore/types"
	butil "github.com/longhorn/backupstore/util"
	commonbitmap "github.com/longhorn/go-common-libs/bitmap"
	commonnet "github.com/longhorn/go-common-libs/net"
	commonutils "github.com/longhorn/go-common-libs/utils"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
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

	IsExposed bool

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

	log logrus.FieldLogger

	// TODO: Record error message
}

type RebuildingDstCache struct {
	rebuildingLvol *Lvol
	rebuildingPort int32

	srcReplicaName           string
	srcReplicaAddress        string
	externalSnapshotName     string
	externalSnapshotBdevName string

	rebuildingSnapshotMap map[string]*api.Lvol
	rebuildingSize        uint64
	rebuildingError       string
	rebuildingState       string

	processedSnapshotList  []string
	processedSnapshotsSize uint64

	processingSnapshotName string
	processingOpID         uint32
	processingState        string
	processingSize         uint64
}

type RebuildingSrcCache struct {
	dstReplicaName string
	// dstRebuildingBdev is the result of attaching the rebuilding lvol exposed by the dst replica
	dstRebuildingBdevName string

	exposedSnapshotAlias string
	exposedSnapshotPort  int32
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

		rebuildingDstCache: RebuildingDstCache{
			rebuildingSnapshotMap: map[string]*api.Lvol{},
			processedSnapshotList: []string{},
		},
		rebuildingSrcCache: RebuildingSrcCache{},

		restore: &Restore{},

		UpdateCh: updateCh,

		log: log,
	}
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
	return IsReplicaLvol(r.Name, lvolName) || (len(r.ActiveChain) > 0 && r.ActiveChain[0] != nil && r.ActiveChain[0].Name == lvolName)
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

	return nil
}

func getExposedPort(subsystem *spdktypes.NvmfSubsystem) (exposedPort int32, err error) {
	if subsystem == nil || len(subsystem.ListenAddresses) == 0 {
		return 0, fmt.Errorf("cannot find the Nvmf subsystem")
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

	return 0, fmt.Errorf("cannot find a exposed port in the Nvmf subsystem")
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

func (r *Replica) IsHeadAvailable(spdkClient *spdkclient.Client) (isAvailable bool, err error) {
	defer func() {
		if err != nil || isAvailable {
			return
		}
		r.Head = nil
		if r.ActiveChain[len(r.ActiveChain)-1] != nil &&
			r.ActiveChain[len(r.ActiveChain)-1].Name == r.Name {
			r.ActiveChain = r.ActiveChain[:len(r.ActiveChain)-1]
		}
	}()

	if len(r.ActiveChain) < 2 {
		return false, nil
	}
	if r.Head == nil {
		return false, nil
	}

	bdevLvolList, err := spdkClient.BdevLvolGet(r.Alias, 0)
	if err != nil {
		return false, err
	}
	if len(bdevLvolList) < 1 {
		return false, nil
	}

	return true, nil
}

func (r *Replica) updateHeadCache(spdkClient *spdkclient.Client) (err error) {
	bdevLvolList, err := spdkClient.BdevLvolGet(r.Alias, 0)
	if err != nil {
		return err
	}
	if len(bdevLvolList) < 1 {
		return fmt.Errorf("cannot find head lvol %v for the cache update", r.Alias)
	}

	r.Head = BdevLvolInfoToServiceLvol(&bdevLvolList[0])

	if len(r.ActiveChain) == 1 || (r.ActiveChain[len(r.ActiveChain)-1] != nil && r.ActiveChain[len(r.ActiveChain)-1].Name != r.Name) {
		r.ActiveChain = append(r.ActiveChain, r.Head)
	} else {
		r.ActiveChain[len(r.ActiveChain)-1] = r.Head
	}
	if r.ActiveChain[len(r.ActiveChain)-2] != nil {
		if r.ActiveChain[len(r.ActiveChain)-2].Name != r.Head.Parent {
			return fmt.Errorf("found the last entry of the active chain %v is not the head parent %v", r.ActiveChain[len(r.ActiveChain)-2].Name, r.Head.Parent)
		}
		r.ActiveChain[len(r.ActiveChain)-2].Children[r.Head.Name] = r.Head
	}

	return nil
}

func (r *Replica) prepareHead(spdkClient *spdkclient.Client) (err error) {
	isHeadAvailable, err := r.IsHeadAvailable(spdkClient)
	if err != nil {
		return err
	}

	if !isHeadAvailable {
		r.log.Info("Creating a lvol bdev as replica Head")
		if r.ActiveChain[len(r.ActiveChain)-1] != nil { // The replica has a backing image or somehow there are already snapshots in the chain
			if _, err := spdkClient.BdevLvolClone(r.ActiveChain[len(r.ActiveChain)-1].UUID, r.Name); err != nil {
				return err
			}
			if r.ActiveChain[len(r.ActiveChain)-1].SpecSize != r.SpecSize {
				if _, err := spdkClient.BdevLvolResize(r.Alias, r.SpecSize); err != nil {
					return err
				}
			}
		} else {
			if _, err := spdkClient.BdevLvolCreate("", r.LvsUUID, r.Name, util.BytesToMiB(r.SpecSize), "", true); err != nil {
				return err
			}
		}
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
	headParentSnapshotName := headBdevLvol.DriverSpecific.Lvol.BaseSnapshot
	if IsReplicaSnapshotLvol(replicaName, headParentSnapshotName) {
		headParentSnapSvcLvol := snapshotLvolMap[headParentSnapshotName]
		if headParentSnapSvcLvol == nil {
			return nil, fmt.Errorf("cannot find the parent snapshot %s of the head for replica %s", headParentSnapshotName, replicaName)
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
func (r *Replica) Create(spdkClient *spdkclient.Client, portCount int32, superiorPortAllocator *commonbitmap.Bitmap) (ret *spdkrpc.Replica, err error) {
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
	if err := r.prepareHead(spdkClient); err != nil {
		return nil, err
	}
	r.State = types.InstanceStateStopped

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
		r.log.Info("Unexposing lvol bdev for replica deletion")
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.Name)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		r.IsExposed = false
		updateRequired = true
	}

	// Clean up the rebuilding cached info first
	r.doCleanupForRebuildingSrc(spdkClient)
	_ = r.doCleanupForRebuildingDst(spdkClient, false)
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

	// Clean up the valid snapshot tree
	if len(r.ActiveChain) > 1 {
		bdevLvolMap, err := GetBdevLvolMapWithFilter(spdkClient, r.replicaLvolFilter)
		if err != nil {
			return err
		}
		// Notice r.ActiveChain[0] may be the backing image lvol, which should not be cleaned up by replicas
		CleanupLvolTree(spdkClient, r.ActiveChain[1].Name, bdevLvolMap, r.log)
	}
	// Clean up the possible rebuilding leftovers
	_ = r.doCleanupForRebuildingDst(spdkClient, true)

	r.log.Info("Deleted replica")

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

	bdevLvolList, err := spdkClient.BdevLvolGet(snapUUID, 0)
	if err != nil {
		return nil, err
	}
	if len(bdevLvolList) != 1 {
		return nil, fmt.Errorf("zero or multiple snap lvols with UUID %s found after lvol snapshot create", snapUUID)
	}
	snapSvcLvol := BdevLvolInfoToServiceLvol(&bdevLvolList[0])

	bdevLvolList, err = spdkClient.BdevLvolGet(r.Head.Alias, 0)
	if err != nil {
		return nil, err
	}
	if len(bdevLvolList) != 1 {
		return nil, fmt.Errorf("zero or multiple head lvols with UUID %s found after lvol snapshot create", snapUUID)
	}
	r.Head = BdevLvolInfoToServiceLvol(&bdevLvolList[0])
	snapSvcLvol.Children[r.Head.Name] = r.Head

	// Already contain a valid snapshot lvol or backing image lvol before this snapshot creation
	if len(r.ActiveChain) > 1 && r.ActiveChain[len(r.ActiveChain)-2] != nil {
		prevSvcLvol := r.ActiveChain[len(r.ActiveChain)-2]
		delete(prevSvcLvol.Children, r.Head.Name)
		prevSvcLvol.Children[snapSvcLvol.Name] = snapSvcLvol
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
		bdevLvol, err := spdkClient.BdevLvolGet(childSvcLvol.UUID, 0)
		if err != nil {
			return nil, err
		}
		if len(bdevLvol) != 1 {
			return nil, fmt.Errorf("failed to get the bdev of the only child lvol %s after snapshot %s delete", childSvcLvol.Name, snapshotName)
		}
		childSvcLvol.ActualSize = bdevLvol[0].DriverSpecific.Lvol.NumAllocatedClusters * defaultClusterSize
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
			bdevLvol, err := spdkClient.BdevLvolGet(childSvcLvol.UUID, 0)
			if err != nil {
				return err
			}
			if len(bdevLvol) != 1 {
				return fmt.Errorf("failed to get the bdev of the only child lvol %s after purging system snapshot %s", childSvcLvol.Name, snapshotLvolName)
			}
			childSvcLvol.ActualSize = bdevLvol[0].DriverSpecific.Lvol.NumAllocatedClusters * defaultClusterSize
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
		return "", fmt.Errorf("cannot find snapshot %s for for replica %s rebuilding src start", exposedSnapshotName, r.Name)
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

// RebuildingSrcAttach blindly attaches the rebuilding lvol of the dst replica as NVMf controller no matter if src and dst are on different nodes
func (r *Replica) RebuildingSrcAttach(spdkClient *spdkclient.Client, dstReplicaName, dstRebuildingLvolAddress string) (err error) {
	dstRebuildingLvolName := GetReplicaRebuildingLvolName(dstReplicaName)
	if r.rebuildingSrcCache.dstRebuildingBdevName != "" {
		controllerName := helperutil.GetNvmeControllerNameFromNamespaceName(r.rebuildingSrcCache.dstRebuildingBdevName)
		if dstRebuildingLvolName != controllerName {
			return fmt.Errorf("found mismatching between the required dst bdev nvme controller name %s and the expected dst controller name %s for replica %s rebuilding src attach", dstRebuildingLvolName, controllerName, r.Name)
		}
		return nil
	}

	r.rebuildingSrcCache.dstRebuildingBdevName, err = connectNVMfBdev(spdkClient, dstRebuildingLvolName, dstRebuildingLvolAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to connect rebuilding lvol %s with address %s as a NVMe bdev for replica %s rebuilding src attach", dstRebuildingLvolName, dstRebuildingLvolAddress, r.Name)
	}

	return nil
}

// RebuildingSrcDetach detaches the rebuilding lvol of the dst replica as NVMf controller if src and dst are on different nodes
func (r *Replica) RebuildingSrcDetach(spdkClient *spdkclient.Client, dstReplicaName string) (err error) {
	if r.rebuildingSrcCache.dstRebuildingBdevName == "" {
		return nil
	}
	if err := disconnectNVMfBdev(spdkClient, r.rebuildingSrcCache.dstRebuildingBdevName); err != nil {
		return err
	}
	r.rebuildingSrcCache.dstRebuildingBdevName = ""

	return nil
}

// RebuildingSrcShallowCopyStart asks the src replica to start a shallow copy from its snapshot lvol to the dst rebuilding lvol.
// It returns the shallow copy op ID, which is for retrieving the shallow copy progress and status. The caller is responsible for storing the op ID.
func (r *Replica) RebuildingSrcShallowCopyStart(spdkClient *spdkclient.Client, snapshotName string) (shallowCopyOpID uint32, err error) {
	r.RLock()
	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	srcSnapLvol := r.SnapshotLvolMap[snapLvolName]
	dstBdevName := r.rebuildingSrcCache.dstRebuildingBdevName
	r.RUnlock()

	if dstBdevName == "" {
		return 0, fmt.Errorf("no destination bdev for replica %s shallow copy start", r.Name)
	}
	if srcSnapLvol == nil {
		return 0, fmt.Errorf("cannot find snapshot %s for replica %s shallow copy start", snapshotName, r.Name)
	}

	return spdkClient.BdevLvolStartShallowCopy(srcSnapLvol.UUID, dstBdevName)
}

// RebuildingSrcShallowCopyCheck asks the src replica to check the shallow copy progress and status via the shallow copy op ID returned by RebuildingSrcShallowCopyStart.
func (r *Replica) RebuildingSrcShallowCopyCheck(spdkClient *spdkclient.Client, shallowCopyOpID uint32) (status *spdktypes.ShallowCopyStatus, err error) {
	status, err = spdkClient.BdevLvolCheckShallowCopy(shallowCopyOpID)
	if err != nil {
		return nil, err
	}
	if status.State == types.SPDKShallowCopyStateInProgress {
		status.State = types.ProgressStateInProgress
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

	if len(r.ActiveChain) != 2 {
		return "", fmt.Errorf("invalid chain length %d for dst replica %v rebuilding start", len(r.ActiveChain), r.Name)
	}

	// Replica.Delete and Replica.Create do not guarantee that the previous rebuilding src replica info is cleaned up
	if r.rebuildingDstCache.srcReplicaName != "" || r.rebuildingDstCache.srcReplicaAddress != "" || r.rebuildingDstCache.externalSnapshotName != "" || r.rebuildingDstCache.externalSnapshotBdevName != "" {
		if err := r.doCleanupForRebuildingDst(spdkClient, false); err != nil {
			return "", fmt.Errorf("failed to clean up the previous src replica info for dst replica rebuilding start, src replica name %s, address %s, external snapshot name %s, or external snapshot bdev name %s", r.rebuildingDstCache.srcReplicaName, r.rebuildingDstCache.srcReplicaAddress, r.rebuildingDstCache.externalSnapshotName, r.rebuildingDstCache.externalSnapshotBdevName)
		}
	}
	r.rebuildingDstCache.srcReplicaName = srcReplicaName
	r.rebuildingDstCache.srcReplicaAddress = srcReplicaAddress

	externalSnapshotLvolName := GetReplicaSnapshotLvolName(srcReplicaName, externalSnapshotName)
	externalSnapshotBdevName, err := connectNVMfBdev(spdkClient, externalSnapshotLvolName, externalSnapshotAddress)
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

	// Create a new head lvol based on the external src snapshot lvol
	if r.IsExposed {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.Name)); err != nil {
			return "", err
		}
		r.IsExposed = false
	}
	if _, err := spdkClient.BdevLvolDelete(r.Alias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return "", err
	}
	headLvolUUID, err := spdkClient.BdevLvolCloneBdev(r.rebuildingDstCache.externalSnapshotBdevName, r.LvsName, r.Name)
	if err != nil {
		return "", err
	}

	bdevLvolList, err := spdkClient.BdevLvolGet(headLvolUUID, 0)
	if err != nil {
		return "", err
	}
	if len(bdevLvolList) != 1 {
		return "", fmt.Errorf("zero or multiple head lvols with UUID %s found after rebuilding dst head %s creation", headLvolUUID, r.Name)
	}
	r.Head = BdevLvolInfoToServiceLvol(&bdevLvolList[0])
	r.ActiveChain[1] = r.Head

	nguid := commonutils.RandomID(nvmeNguidLength)
	if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(r.Name), r.Head.UUID, nguid, r.IP, strconv.Itoa(int(r.PortStart))); err != nil {
		return "", err
	}
	r.IsExposed = true
	dstHeadLvolAddress := net.JoinHostPort(r.IP, strconv.Itoa(int(r.PortStart)))

	for _, apiLvol := range rebuildingSnapshotList {
		r.rebuildingDstCache.rebuildingSnapshotMap[apiLvol.Name] = apiLvol
		r.rebuildingDstCache.rebuildingSize += apiLvol.ActualSize
	}
	r.rebuildingDstCache.rebuildingError = ""
	r.rebuildingDstCache.rebuildingState = types.ProgressStateInProgress

	r.rebuildingDstCache.processingSnapshotName = ""
	r.rebuildingDstCache.processingOpID = 0
	r.rebuildingDstCache.processingState = types.ProgressStateStarting
	r.rebuildingDstCache.processingSize = 0
	r.rebuildingDstCache.processedSnapshotList = make([]string, 0, len(rebuildingSnapshotList))
	r.rebuildingDstCache.processedSnapshotsSize = 0

	r.isRebuilding = true

	r.log.Infof("Replica created a new head %s(%s) based on the external snapshot %s(%s) from healthy replica %s for rebuilding start", r.Head.Alias, dstHeadLvolAddress, externalSnapshotName, externalSnapshotAddress, srcReplicaName)

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

	_ = r.doCleanupForRebuildingDst(spdkClient, r.rebuildingDstCache.rebuildingState == types.ProgressStateError)

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

func (r *Replica) doCleanupForRebuildingDst(spdkClient *spdkclient.Client, cleanupRebuildingLvolTree bool) error {
	aggregatedErrors := []error{}
	if r.rebuildingDstCache.srcReplicaAddress != "" {
		if err := disconnectNVMfBdev(spdkClient, r.rebuildingDstCache.externalSnapshotBdevName); err != nil {
			r.log.WithError(err).Errorf("Failed to disconnect the external src snapshot bdev %s for rebuilding dst cleanup, will continue", r.rebuildingDstCache.externalSnapshotBdevName)
			aggregatedErrors = append(aggregatedErrors, err)
		} else {
			r.rebuildingDstCache.srcReplicaAddress = ""
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
		}
	}
	if _, err := spdkClient.BdevLvolDelete(spdktypes.GetLvolAlias(r.LvsName, rebuildingLvolName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		r.log.WithError(err).Errorf("Failed to delete the rebuilding lvol %s for rebuilding dst cleanup, will continue", rebuildingLvolName)
		aggregatedErrors = append(aggregatedErrors, err)
	} else {
		r.rebuildingDstCache.rebuildingLvol = nil
	}

	// Mainly for rebuilding failed case
	if cleanupRebuildingLvolTree && len(r.rebuildingDstCache.processedSnapshotList) > 0 {
		allLvolsCleaned := true
		// Do cleanup in a reverse order to avoid trying to delete a snapshot lvol with multiple children
		for idx := len(r.rebuildingDstCache.processedSnapshotList) - 1; idx >= 0; idx-- {
			snapLvolAlias := spdktypes.GetLvolAlias(r.LvsName, GetReplicaSnapshotLvolName(r.Name, r.rebuildingDstCache.processedSnapshotList[idx]))
			if _, err := spdkClient.BdevLvolDelete(snapLvolAlias); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
				allLvolsCleaned = false
				r.log.WithError(err).Errorf("failed to delete rebuilt snapshot lvol %v for rebuilding dst cleanup, will continue", snapLvolAlias)
				aggregatedErrors = append(aggregatedErrors, err)
			}
		}
		if allLvolsCleaned {
			r.rebuildingDstCache.processedSnapshotList = make([]string, 0)
		}
	}

	r.rebuildingDstCache.rebuildingSnapshotMap = map[string]*api.Lvol{}

	return util.CombineErrors(aggregatedErrors...)
}

// RebuildingDstShallowCopyStart let the dst replica ask the src replica to start a shallow copy from a snapshot to the rebuilding lvol.
// This dst replica is responsible for storing the shallow copy op ID so that it can help check the progress and status later on.
func (r *Replica) RebuildingDstShallowCopyStart(spdkClient *spdkclient.Client, snapshotName string) (err error) {
	r.Lock()
	defer r.Unlock()

	defer func() {
		if err != nil {
			r.rebuildingDstCache.rebuildingError = err.Error()
			r.rebuildingDstCache.rebuildingState = types.ProgressStateError
			r.rebuildingDstCache.processingState = types.ProgressStateError
		} else {
			r.rebuildingDstCache.processingState = types.ProgressStateInProgress
		}
	}()
	if r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName] == nil {
		return fmt.Errorf("cannot find snapshot %s in the rebuilding snapshot list for replica %s shallow copy start", snapshotName, r.Name)
	}
	r.rebuildingDstCache.processingSnapshotName = snapshotName
	r.rebuildingDstCache.processingSize = 0

	srcReplicaServiceCli, err := GetServiceClient(r.rebuildingDstCache.srcReplicaAddress)
	if err != nil {
		return err
	}
	defer func() {
		if errClose := srcReplicaServiceCli.Close(); errClose != nil {
			r.log.WithError(errClose).Errorf("Failed to close replica %s client with address %s during start rebuilding dst shallow copy", r.rebuildingDstCache.srcReplicaName, r.rebuildingDstCache.srcReplicaAddress)
		}
	}()

	r.rebuildingDstCache.processingOpID, err = srcReplicaServiceCli.ReplicaRebuildingSrcShallowCopyStart(r.rebuildingDstCache.srcReplicaName, snapshotName)
	return err
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

		state, copiedClusters, totalClusters, errorMsg, err := srcReplicaServiceCli.ReplicaRebuildingSrcShallowCopyCheck(r.rebuildingDstCache.srcReplicaName, r.Name, r.rebuildingDstCache.processingSnapshotName, r.rebuildingDstCache.processingOpID)
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

	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	snapUUID, err := spdkClient.BdevLvolSnapshot(r.rebuildingDstCache.rebuildingLvol.UUID, snapLvolName, xattrs)
	if err != nil {
		return err
	}

	bdevLvolList, err := spdkClient.BdevLvolGet(snapUUID, 0)
	if err != nil {
		return err
	}
	if len(bdevLvolList) != 1 {
		return fmt.Errorf("zero or multiple snap lvols with UUID %s found after rebuilding dst snapshot %s creation", snapUUID, snapshotName)
	}
	snapSvcLvol := BdevLvolInfoToServiceLvol(&bdevLvolList[0])

	if r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName] == nil {
		return fmt.Errorf("cannot find snapshot %s in the rebuilding snapshot list for replica %s rebuilding snapshot creation", snapshotName, r.Name)
	}
	if r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName].ActualSize != snapSvcLvol.ActualSize {
		return fmt.Errorf("rebuilding dst newly rebuilt snapshot %s(%s) actual size %d does not match the corresponding rebuilding src snapshot (%s) actual size %d", snapSvcLvol.Name, snapSvcLvol.UUID, snapSvcLvol.ActualSize, r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName].UUID, r.rebuildingDstCache.rebuildingSnapshotMap[snapshotName].ActualSize)
	}

	r.log.Infof("Created a new snapshot %s(%s) for rebuilding dst", snapSvcLvol.Alias, snapSvcLvol.UUID)

	// Do not update r.ActiveChain for the rebuilding snapshots here.
	// The replica will directly reconstruct r.ActiveChain as well as r.SnapshotLvolMap during the rebuilding dst finish.
	r.rebuildingDstCache.rebuildingLvol.Parent = snapSvcLvol.Name

	r.rebuildingDstCache.processedSnapshotList = append(r.rebuildingDstCache.processedSnapshotList, snapshotName)
	r.rebuildingDstCache.processedSnapshotsSize += snapSvcLvol.ActualSize

	r.rebuildingDstCache.processingState = types.ProgressStateStarting
	r.rebuildingDstCache.processingSnapshotName = ""
	r.rebuildingDstCache.processingOpID = 0
	r.rebuildingDstCache.processingSize = 0
	updateRequired = true

	return nil
}

// RebuildingDstSnapshotRevert reverts the rebuilding lvol to another snapshot lvol for the dst replica before rebuilding a new branch of the snapshot tree
// Notice that the snapshot name can be empty, which means the beginning of the snapshot tree rebuilding
func (r *Replica) RebuildingDstSnapshotRevert(spdkClient *spdkclient.Client, snapshotName string) (dstRebuildingLvolAddress string, err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateRunning {
		return "", fmt.Errorf("invalid state %v for dst replica %s rebuilding snapshot %s revert", r.State, r.Name, snapshotName)
	}
	if !r.isRebuilding {
		return "", fmt.Errorf("rebuilding dst replica %s is not in rebuilding", r.Name)
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

	// TODO: Should we blindly expose it?
	rebuildingLvolName := GetReplicaRebuildingLvolName(r.Name)
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

	rebuildingLvolUUID := ""
	if snapshotName != "" {
		snapLvolAlias := spdktypes.GetLvolAlias(r.LvsName, GetReplicaSnapshotLvolName(r.Name, snapshotName))

		if rebuildingLvolUUID, err = spdkClient.BdevLvolClone(snapLvolAlias, rebuildingLvolName); err != nil {
			return "", err
		}
	} else {
		if rebuildingLvolUUID, err = spdkClient.BdevLvolCreate("", r.LvsUUID, rebuildingLvolName, util.BytesToMiB(r.SpecSize), "", true); err != nil {
			return "", err
		}
	}
	bdevLvolList, err := spdkClient.BdevLvolGet(rebuildingLvolUUID, 0)
	if err != nil {
		return "", err
	}
	if len(bdevLvolList) != 1 {
		return "", fmt.Errorf("zero or multiple lvols with UUID %s found after rebuilding dst snapshot %s revert", rebuildingLvolUUID, snapshotName)
	}
	r.rebuildingDstCache.rebuildingLvol = BdevLvolInfoToServiceLvol(&bdevLvolList[0])

	dstRebuildingLvolAddress = r.rebuildingDstCache.rebuildingLvol.Alias
	if r.rebuildingDstCache.rebuildingPort != 0 {
		nguid := commonutils.RandomID(nvmeNguidLength)
		if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(r.rebuildingDstCache.rebuildingLvol.Name), r.rebuildingDstCache.rebuildingLvol.UUID, nguid, r.IP, strconv.Itoa(int(r.rebuildingDstCache.rebuildingPort))); err != nil {
			return "", err
		}
		dstRebuildingLvolAddress = net.JoinHostPort(r.IP, strconv.Itoa(int(r.rebuildingDstCache.rebuildingPort)))
	}

	updateRequired = true

	r.log.Infof("Rebuilding destination replica reverted its rebuilding lvol %s(%s) to snapshot %s and expose it to %s", r.rebuildingDstCache.rebuildingLvol.Alias, rebuildingLvolUUID, snapshotName, dstRebuildingLvolAddress)

	return dstRebuildingLvolAddress, nil
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
