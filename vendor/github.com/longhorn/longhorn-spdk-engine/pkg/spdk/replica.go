package spdk

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"

	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
)

type Replica struct {
	sync.RWMutex

	ctx context.Context

	ActiveChain []*Lvol
	ChainLength int
	// SnapshotMap map[<snapshot name>]. <snapshot name> is a caller provided name and does not contain prefix `<replica name>-snap-`
	SnapshotMap map[string]*Lvol

	Name      string
	UUID      string
	Alias     string
	LvsName   string
	LvsUUID   string
	SpecSize  uint64
	IP        string
	PortStart int32
	PortEnd   int32

	State     types.InstanceState
	IsExposed bool

	isRebuilding   bool
	rebuildingLvol *Lvol
	rebuildingPort int32

	rebuildingDstReplicaName string
	rebuildingDstBdevName    string
	rebuildingDstBdevType    spdktypes.BdevType

	portAllocator *util.Bitmap
	// UpdateCh should not be protected by the replica lock
	UpdateCh chan interface{}

	log logrus.FieldLogger

	// TODO: Record error message
}

type Lvol struct {
	Name       string
	UUID       string
	Alias      string
	SpecSize   uint64
	ActualSize uint64
	Parent     string
	// Children is map[<snapshot lvol name>] rather than map[<snapshot name>]. <snapshot lvol name> consists of `<replica name>-snap-<snapshot name>`
	Children map[string]*Lvol
}

func ServiceReplicaToProtoReplica(r *Replica) *spdkrpc.Replica {
	res := &spdkrpc.Replica{
		Name:      r.Name,
		Uuid:      r.UUID,
		LvsName:   r.LvsName,
		LvsUuid:   r.LvsUUID,
		SpecSize:  r.SpecSize,
		Snapshots: map[string]*spdkrpc.Lvol{},
		Ip:        r.IP,
		PortStart: r.PortStart,
		PortEnd:   r.PortEnd,
		State:     string(r.State),
	}
	// spdkrpc.Replica.Snapshots is map[<snapshot name>] rather than map[<snapshot lvol name>]
	for name, lvol := range r.SnapshotMap {
		res.Snapshots[name] = ServiceLvolToProtoLvol(lvol)
	}

	return res
}

func ServiceLvolToProtoLvol(lvol *Lvol) *spdkrpc.Lvol {
	res := &spdkrpc.Lvol{
		Name:       lvol.Name,
		Uuid:       lvol.UUID,
		SpecSize:   lvol.SpecSize,
		ActualSize: lvol.ActualSize,
		Parent:     lvol.Parent,
		Children:   map[string]bool{},
	}
	for _, childSvcLvol := range lvol.Children {
		// For a snapshot lvol, the child name means the child lvol name, which consists of <replica-name>-snap-<snapshot-name>
		res.Children[childSvcLvol.Name] = true
	}

	return res
}

func BdevLvolInfoToServiceLvol(bdev *spdktypes.BdevInfo) *Lvol {
	return &Lvol{
		Name:     spdktypes.GetLvolNameFromAlias(bdev.Aliases[0]),
		Alias:    bdev.Aliases[0],
		UUID:     bdev.UUID,
		SpecSize: bdev.NumBlocks * uint64(bdev.BlockSize),
		Parent:   bdev.DriverSpecific.Lvol.BaseSnapshot,
		// Need to update this separately
		Children: map[string]*Lvol{},
	}
}

func NewReplica(ctx context.Context, replicaName, lvsName, lvsUUID string, specSize uint64, updateCh chan interface{}) *Replica {
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

		ActiveChain: []*Lvol{
			{
				Name:     replicaName,
				Alias:    spdktypes.GetLvolAlias(lvsName, replicaName),
				SpecSize: roundedSpecSize,
				Children: map[string]*Lvol{},
			},
		},
		ChainLength: 1,
		SnapshotMap: map[string]*Lvol{},
		Name:        replicaName,
		Alias:       spdktypes.GetLvolAlias(lvsName, replicaName),
		LvsName:     lvsName,
		LvsUUID:     lvsUUID,
		SpecSize:    roundedSpecSize,
		State:       types.InstanceStatePending,

		UpdateCh: updateCh,

		log: log,
	}
}

func (r *Replica) Sync(spdkClient *spdkclient.Client) (err error) {
	r.Lock()
	defer r.Unlock()
	// It's better to let the server send the update signal

	// This lvol and nvmf subsystem fetch should be protected by replica lock, in case of snapshot operations happened during the sync-up.
	bdevLvolMap, err := GetBdevLvolMap(spdkClient)
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

// construct build Replica with the SnapshotMap and SnapshotChain from the bdev lvol list.
// This function is typically invoked for the existing lvols after node/service restart and device add.
func (r *Replica) construct(bdevLvolMap map[string]*spdktypes.BdevInfo) (err error) {
	defer func() {
		if err != nil {
			r.State = types.InstanceStateError
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
		return fmt.Errorf("invalid state %s for replica %s construct", r.Name, r.State)
	}

	if err := r.validateReplicaInfo(bdevLvolMap[r.Name]); err != nil {
		return err
	}
	newChain, err := constructActiveChain(r.Name, bdevLvolMap)
	if err != nil {
		return err
	}
	newSnapshotMap, err := constructSnapshotMap(r.Name, newChain[0], bdevLvolMap)
	if err != nil {
		return err
	}

	r.UUID = bdevLvolMap[r.Name].UUID
	r.ActiveChain = newChain
	r.ChainLength = len(r.ActiveChain)
	r.SnapshotMap = newSnapshotMap
	if r.State == types.InstanceStatePending {
		r.State = types.InstanceStateStopped
	}
	r.log.WithField("uuid", r.UUID)

	return nil
}

func (r *Replica) validateAndUpdate(bdevLvolMap map[string]*spdktypes.BdevInfo, subsystemMap map[string]*spdktypes.NvmfSubsystem) (err error) {
	defer func() {
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
			r.log.Errorf("Found error during validation and update: %v", err)
		}
	}()

	// Stop syncing with the SPDK TGT server if the replica does not contain any valid SPDK components.
	if r.State != types.InstanceStateRunning && r.State != types.InstanceStateStopped {
		return nil
	}

	// Should not sync a rebuilding replica since the snapshot map as well as the active chain is not ready.
	if r.rebuildingLvol != nil {
		return nil
	}

	if err := r.validateReplicaInfo(bdevLvolMap[r.Name]); err != nil {
		return err
	}

	newChain, err := constructActiveChain(r.Name, bdevLvolMap)
	if err != nil {
		return err
	}
	if len(r.ActiveChain) != len(newChain) {
		return fmt.Errorf("replica current active chain length %d is not the same as the latest chain length %d", len(r.ActiveChain), len(newChain))
	}
	for idx, svcLvol := range r.ActiveChain {
		newSvcLvol := newChain[idx]
		if err := compareSvcLvols(svcLvol, newChain[idx], false, svcLvol.Name != r.Name); err != nil {
			return err
		}
		// Then update the actual size for the head lvol
		if svcLvol.Name == r.Name && svcLvol.ActualSize != newSvcLvol.ActualSize {
			svcLvol.ActualSize = newSvcLvol.ActualSize
		}
	}

	newSnapshotMap, err := constructSnapshotMap(r.Name, newChain[0], bdevLvolMap)
	if err != nil {
		return err
	}
	if len(r.SnapshotMap) != len(newSnapshotMap) {
		return fmt.Errorf("replica current active snapshot map length %d is not the same as the latest snapshot map length %d", len(r.SnapshotMap), len(newSnapshotMap))
	}
	for snapshotName := range r.SnapshotMap {
		if err := compareSvcLvols(r.SnapshotMap[snapshotName], newSnapshotMap[snapshotName], true, true); err != nil {
			return err
		}
	}

	if r.State == types.InstanceStateRunning {
		if r.IP == "" {
			return fmt.Errorf("found invalid IP %s for replica %s", r.IP, r.Name)
		}
		if r.PortStart == 0 || r.PortEnd == 0 || r.PortStart > r.PortEnd {
			return fmt.Errorf("found invalid Ports [%d, %d] for the running replica %s", r.PortStart, r.PortEnd, r.Name)
		}
	}

	// In case of a stopped replica being wrongly exposed, this function will check the exposing state anyway.
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
	if prev.Name != cur.Name || prev.UUID != cur.UUID || prev.SpecSize != cur.SpecSize || prev.Parent != cur.Parent || len(prev.Children) != len(cur.Children) {
		return fmt.Errorf("found mismatching lvol %+v with recorded prev lvol %+v when validating the active chain", cur, prev)
	}
	if checkChildren {
		for childName := range prev.Children {
			if cur.Children[childName] == nil {
				return fmt.Errorf("found mismatching lvol children %+v with recorded prev lvol children %+v when validating the active chain lvol %s", cur.Children, prev.Children, prev.Name)
			}
		}
	}
	if checkActualSize && prev.ActualSize != cur.ActualSize {
		return fmt.Errorf("found mismatching lvol actual size %v with recorded prev lvol actual size %v when validating the active chain lvol %s", cur.ActualSize, prev.ActualSize, prev.Name)
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

func (r *Replica) validateReplicaInfo(headBdevLvol *spdktypes.BdevInfo) (err error) {
	if headBdevLvol == nil {
		return fmt.Errorf("found nil head bdev lvol for replica %s", r.Name)
	}
	if headBdevLvol.DriverSpecific.Lvol.Snapshot {
		return fmt.Errorf("found the head bdev lvol is a snapshot lvol for replica %s", r.Name)
	}
	if r.LvsUUID != headBdevLvol.DriverSpecific.Lvol.LvolStoreUUID {
		return fmt.Errorf("found mismatching lvol LvsUUID %v with recorded LvsUUID %v for replica %s", headBdevLvol.DriverSpecific.Lvol.LvolStoreUUID, r.LvsUUID, r.Name)
	}
	if r.UUID != "" && headBdevLvol.UUID != r.UUID {
		return fmt.Errorf("found mismatching lvol UUID %v with recorded UUID %v for replica %s", headBdevLvol.UUID, r.UUID, r.Name)
	}
	bdevLvolSpecSize := headBdevLvol.NumBlocks * uint64(headBdevLvol.BlockSize)
	if r.SpecSize != 0 && r.SpecSize != bdevLvolSpecSize {
		return fmt.Errorf("found mismatching lvol spec size %v with recorded spec size %v for replica %s", bdevLvolSpecSize, r.SpecSize, r.Name)
	}

	return nil
}

func constructActiveChain(replicaName string, bdevLvolMap map[string]*spdktypes.BdevInfo) (res []*Lvol, err error) {
	newChain := []*Lvol{}

	headBdevLvol := bdevLvolMap[replicaName]
	if headBdevLvol == nil {
		return nil, fmt.Errorf("found nil head bdev lvol for replica %s", replicaName)
	}
	headSvcLvol := BdevLvolInfoToServiceLvol(headBdevLvol)
	// TODO: Considering the clone, this function or `constructSnapshotMap` may need to construct the children map for the head

	for childSvcLvol, curBdevLvol := headSvcLvol, bdevLvolMap[headBdevLvol.DriverSpecific.Lvol.BaseSnapshot]; curBdevLvol != nil; {
		curSvcLvol := BdevLvolInfoToServiceLvol(curBdevLvol)
		curSvcLvol.Children[childSvcLvol.Name] = childSvcLvol
		childSvcLvol.Parent = curSvcLvol.Name
		newChain = append(newChain, curSvcLvol)

		childSvcLvol = curSvcLvol
		curBdevLvol = bdevLvolMap[curBdevLvol.DriverSpecific.Lvol.BaseSnapshot]
	}

	// Need to flip r.ActiveSnapshotChain since the oldest should be the first entry
	for head, tail := 0, len(newChain)-1; head < tail; head, tail = head+1, tail-1 {
		newChain[head], newChain[tail] = newChain[tail], newChain[head]
	}
	newChain = append(newChain, headSvcLvol)

	return newChain, nil
}

func constructSnapshotMap(replicaName string, rootSvcLvol *Lvol, bdevLvolMap map[string]*spdktypes.BdevInfo) (res map[string]*Lvol, err error) {
	res = map[string]*Lvol{}

	queue := []*Lvol{rootSvcLvol}
	for ; len(queue) > 0; queue = queue[1:] {
		curSvcLvol := queue[0]
		if curSvcLvol == nil || curSvcLvol.Name == replicaName {
			continue
		}
		snapshotName := GetSnapshotNameFromReplicaSnapshotLvolName(replicaName, curSvcLvol.Name)
		if snapshotName == "" {
			continue
		}
		res[snapshotName] = curSvcLvol

		if bdevLvolMap[curSvcLvol.Name].DriverSpecific.Lvol.Clones == nil {
			continue
		}
		for _, childName := range bdevLvolMap[curSvcLvol.Name].DriverSpecific.Lvol.Clones {
			if bdevLvolMap[childName] == nil {
				return nil, fmt.Errorf("cannot find child lvol %v for lvol %v during the snapshot map construction", childName, curSvcLvol.Name)
			}
			tmpSvcLvol := curSvcLvol.Children[childName]
			if tmpSvcLvol == nil {
				tmpSvcLvol = BdevLvolInfoToServiceLvol(bdevLvolMap[childName])
			}
			curSvcLvol.Children[childName] = tmpSvcLvol
			queue = append(queue, tmpSvcLvol)
		}
	}

	return res, nil
}

func (r *Replica) Create(spdkClient *spdkclient.Client, exposeRequired bool, portCount int32, superiorPortAllocator *util.Bitmap) (ret *spdkrpc.Replica, err error) {
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
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
		}
	}()

	if r.ChainLength < 1 {
		return nil, fmt.Errorf("invalid chain length %d for replica creation", r.ChainLength)
	}
	headSvcLvol := r.ActiveChain[r.ChainLength-1]

	// Create bdev lvol if the replica is the new one
	if r.State == types.InstanceStatePending {
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

		r.log.Infof("Creating a lvol bdev for the new replica")
		if _, err := spdkClient.BdevLvolCreate("", r.LvsUUID, r.Name, util.BytesToMiB(r.SpecSize), "", true); err != nil {
			return nil, err
		}
		bdevLvolList, err := spdkClient.BdevLvolGet(r.Alias, 0)
		if err != nil {
			return nil, err
		}
		if len(bdevLvolList) < 1 {
			return nil, fmt.Errorf("cannot find lvol %v after creation", r.Alias)
		}
		headSvcLvol.UUID = bdevLvolList[0].UUID
		r.UUID = bdevLvolList[0].UUID
		r.State = types.InstanceStateStopped
		r.log.WithField("uuid", r.UUID)
	}

	podIP, err := util.GetIPForPod()
	if err != nil {
		return nil, err
	}
	r.IP = podIP

	r.PortStart, r.PortEnd, err = superiorPortAllocator.AllocateRange(portCount)
	if err != nil {
		return nil, err
	}
	// Always reserved the 1st port for replica expose and the rest for rebuilding
	r.portAllocator = util.NewBitmap(r.PortStart+1, r.PortEnd)

	if exposeRequired {
		if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(r.Name), r.UUID, podIP, strconv.Itoa(int(r.PortStart))); err != nil {
			return nil, err
		}
		r.IsExposed = true
	}
	r.State = types.InstanceStateRunning

	return ServiceReplicaToProtoReplica(r), nil
}

func (r *Replica) Delete(spdkClient *spdkclient.Client, cleanupRequired bool, superiorPortAllocator *util.Bitmap) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		if err != nil {
			r.State = types.InstanceStateError
		}
		// The port can be released once the rebuilding and expose are stopped
		if r.PortStart != 0 {
			if releaseErr := superiorPortAllocator.ReleaseRange(r.PortStart, r.PortEnd); releaseErr != nil {
				r.log.Errorf("Failed to release port %d to %d at the end of replica deletion: %v", r.PortStart, r.PortEnd, releaseErr)
				return
			}
			r.portAllocator = nil
			r.PortStart, r.PortEnd = 0, 0
			if r.State == types.InstanceStateRunning {
				r.State = types.InstanceStateStopped
			}
			updateRequired = true
		}
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	// TODO: Need to stop all in-progress rebuilding first
	// if err := r.rebuildingDstCleanup(spdkClient); err != nil {
	// 	return err
	// }

	if r.IsExposed {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.Name)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		r.IsExposed = false
		r.rebuildingPort = 0
		updateRequired = true
	}
	r.rebuildingLvol = nil
	r.isRebuilding = false

	if !cleanupRequired {
		return nil
	}

	if _, err := spdkClient.BdevLvolDelete(r.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}
	updateRequired = true
	for snapshotName, snapSvcLvol := range r.SnapshotMap {
		if _, err := spdkClient.BdevLvolDelete(snapSvcLvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		delete(r.SnapshotMap, snapshotName)
	}

	return nil
}

func (r *Replica) Get() (pReplica *spdkrpc.Replica) {
	r.RLock()
	defer r.RUnlock()
	return ServiceReplicaToProtoReplica(r)
}

func (r *Replica) SnapshotCreate(spdkClient *spdkclient.Client, snapshotName string) (pReplica *spdkrpc.Replica, err error) {
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
	if _, exists := r.SnapshotMap[snapshotName]; exists {
		return nil, fmt.Errorf("snapshot %s(%s) already exists in replica %s", snapshotName, snapLvolName, r.Name)
	}

	defer func() {
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
			updateRequired = true
		}
	}()

	if r.ChainLength < 1 {
		return nil, fmt.Errorf("invalid chain length %d for replica snapshot creation", r.ChainLength)
	}
	headSvcLvol := r.ActiveChain[r.ChainLength-1]

	snapUUID, err := spdkClient.BdevLvolSnapshot(headSvcLvol.UUID, snapLvolName)
	if err != nil {
		return nil, err
	}

	bdevLvolList, err := spdkClient.BdevLvolGet(snapUUID, 0)
	if err != nil {
		return nil, err
	}
	if len(bdevLvolList) != 1 {
		return nil, fmt.Errorf("zero or multiple snap lvols with UUID %s found after lvol snapshot", snapUUID)
	}
	snapSvcLvol := BdevLvolInfoToServiceLvol(&bdevLvolList[0])
	snapSvcLvol.Children[headSvcLvol.Name] = headSvcLvol

	// Already contain active snapshots before this snapshot creation
	if r.ChainLength > 1 {
		prevSvcLvol := r.ActiveChain[r.ChainLength-2]
		delete(prevSvcLvol.Children, headSvcLvol.Name)
		prevSvcLvol.Children[snapSvcLvol.Name] = snapSvcLvol
	}
	r.ActiveChain[r.ChainLength-1] = snapSvcLvol
	r.ActiveChain = append(r.ActiveChain, headSvcLvol)
	r.ChainLength++
	r.SnapshotMap[snapshotName] = snapSvcLvol
	headSvcLvol.Parent = snapSvcLvol.Name
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
	snapSvcLvol := r.SnapshotMap[snapshotName]
	if snapSvcLvol == nil {
		return ServiceReplicaToProtoReplica(r), nil
	}
	if len(snapSvcLvol.Children) > 1 {
		return nil, fmt.Errorf("cannot delete snapshot %s(%s) since it has %d children", snapshotName, snapLvolName, len(snapSvcLvol.Children))
	}

	defer func() {
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
			updateRequired = true
		}
	}()

	if r.ChainLength < 1 {
		return nil, fmt.Errorf("invalid chain length %d for replica snapshot delete", r.ChainLength)
	}

	if _, err := spdkClient.BdevLvolDelete(snapSvcLvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, err
	}
	r.removeLvolFromSnapshotMapWithoutLock(snapshotName)
	r.removeLvolFromActiveChainWithoutLock(snapLvolName)

	updateRequired = true

	r.log.Infof("Replica deleted snapshot %s(%s)", snapshotName, snapSvcLvol.Alias)

	return ServiceReplicaToProtoReplica(r), nil
}

func (r *Replica) removeLvolFromSnapshotMapWithoutLock(snapshotName string) {
	var deletingSvcLvol, parentSvcLvol, childSvcLvol *Lvol

	deletingSvcLvol = r.SnapshotMap[snapshotName]
	parentSvcLvol = r.SnapshotMap[GetSnapshotNameFromReplicaSnapshotLvolName(r.Name, deletingSvcLvol.Parent)]
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

	delete(r.SnapshotMap, snapshotName)
}

func (r *Replica) removeLvolFromActiveChainWithoutLock(snapLvolName string) int {
	pos := -1
	for idx, lvol := range r.ActiveChain {
		if lvol.Name == snapLvolName {
			pos = idx
			break
		}
	}

	prevChain := r.ActiveChain
	if pos >= 0 && pos < r.ChainLength-1 {
		r.ActiveChain = append([]*Lvol{}, prevChain[:pos]...)
		r.ActiveChain = append(r.ActiveChain, prevChain[pos+1:]...)
	}
	r.ChainLength = len(r.ActiveChain)

	return pos
}

func (r *Replica) RebuildingSrcStart(spdkClient *spdkclient.Client, localReplicaLvsNameMap map[string]string, dstReplicaName, dstRebuildingLvolAddress string) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateRunning {
		return fmt.Errorf("invalid state %v for replica %s rebuilding src start", r.State, r.Name)
	}
	if r.rebuildingLvol != nil || r.rebuildingPort != 0 {
		return fmt.Errorf("replica %s is being rebuilding hence it cannot be the source of rebuilding replica %s", r.Name, dstReplicaName)
	}

	dstRebuildingLvolIP, dstRebuildingLvolPort, err := net.SplitHostPort(dstRebuildingLvolAddress)
	if err != nil {
		return err
	}
	// TODO: After launching online rebuilding, the destination lvol name would be GetReplicaRebuildingLvolName(dstReplicaName)
	dstRebuildingLvolName := dstReplicaName

	if dstRebuildingLvolIP == r.IP {
		dstReplicaLvsName := localReplicaLvsNameMap[dstReplicaName]
		if dstReplicaLvsName == "" {
			return fmt.Errorf("cannot find dst replica %s from the local replica map for replica %s rebuilding src start", dstReplicaName, r.Name)
		}
		r.rebuildingDstBdevName = spdktypes.GetLvolAlias(dstReplicaLvsName, dstRebuildingLvolName)
		r.rebuildingDstBdevType = spdktypes.BdevTypeLvol
	} else {
		nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(dstRebuildingLvolName, helpertypes.GetNQN(dstRebuildingLvolName), dstRebuildingLvolIP, dstRebuildingLvolPort, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4,
			helpertypes.DefaultCtrlrLossTimeoutSec, helpertypes.DefaultReconnectDelaySec, helpertypes.DefaultFastIOFailTimeoutSec)
		if err != nil {
			return err
		}
		if len(nvmeBdevNameList) != 1 {
			return fmt.Errorf("got zero or multiple results when attaching rebuilding dst lvol %s with address %s as a NVMe bdev: %+v", dstRebuildingLvolName, dstRebuildingLvolAddress, nvmeBdevNameList)
		}
		r.rebuildingDstBdevName = nvmeBdevNameList[0]
		r.rebuildingDstBdevType = spdktypes.BdevTypeNvme
	}
	r.rebuildingDstReplicaName = dstReplicaName
	updateRequired = true

	return nil
}

func (r *Replica) RebuildingSrcFinish(spdkClient *spdkclient.Client, dstReplicaName string) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.rebuildingDstReplicaName == "" && r.rebuildingDstBdevName == "" && r.rebuildingDstBdevType == "" {
		return nil
	}

	if r.rebuildingDstReplicaName != "" && r.rebuildingDstReplicaName != dstReplicaName {
		return fmt.Errorf("found mismatching between the required dst replica name %s and the recorded dst replica name %s for replica %s rebuilding src finish", dstReplicaName, r.rebuildingDstReplicaName, r.Name)
	}

	// TODO: After launching online rebuilding, the destination lvol name would be GetReplicaRebuildingLvolName(dstReplicaName)
	dstRebuildingLvolName := dstReplicaName
	switch r.rebuildingDstBdevType {
	case spdktypes.BdevTypeLvol:
		lvolName := spdktypes.GetLvolNameFromAlias(r.rebuildingDstBdevName)
		if dstRebuildingLvolName != lvolName {
			r.log.Errorf("Found mismatching between the required dst bdev lvol name %d and the expected dst lvol name %d for replica %s rebuilding src finish, will do nothing but just clean up rebuilding dst info", dstRebuildingLvolName, lvolName, r.Name)
		}
	case spdktypes.BdevTypeNvme:
		controllerName := helperutil.GetNvmeControllerNameFromNamespaceName(r.rebuildingDstBdevName)
		if dstRebuildingLvolName != controllerName {
			r.log.Errorf("Found mismatching between the required dst bdev nvme controller name %d and the expected dst controller name %d for replica %s rebuilding src finish, will do nothing but just clean up rebuilding dst info", dstRebuildingLvolName, controllerName, r.Name)
			break
		}
		if _, err := spdkClient.BdevNvmeDetachController(helperutil.GetNvmeControllerNameFromNamespaceName(r.rebuildingDstBdevName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
	default:
		r.log.Errorf("Found unknown rebuilding dst bdev type %s with name %s for replica %s rebuilding src finish, will do nothing but just clean up rebuilding dst info", r.rebuildingDstBdevType, r.rebuildingDstBdevName, r.Name)
	}

	r.rebuildingDstReplicaName = ""
	r.rebuildingDstBdevName = ""
	r.rebuildingDstBdevType = ""
	updateRequired = true

	return nil
}

func (r *Replica) SnapshotShallowCopy(spdkClient *spdkclient.Client, snapshotName string) (err error) {
	r.RLock()
	srcSnapLvol := r.SnapshotMap[snapshotName]
	dstBdevName := r.rebuildingDstBdevName
	r.RUnlock()

	if dstBdevName == "" {
		return fmt.Errorf("no destination bdev for replica %s shallow copy", r.Name)
	}
	if srcSnapLvol == nil {
		return fmt.Errorf("cannot find snapshot %s for replica %s shallow copy", snapshotName, r.Name)
	}

	_, err = spdkClient.BdevLvolShallowCopy(srcSnapLvol.UUID, dstBdevName)
	return err
}

func (r *Replica) RebuildingDstStart(spdkClient *spdkclient.Client, exposeRequired bool) (address string, err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateRunning {
		return "", fmt.Errorf("invalid state %v for replica %s rebuilding start", r.State, r.Name)
	}
	if r.isRebuilding || r.rebuildingLvol != nil {
		return "", fmt.Errorf("replica %s rebuilding is in process", r.Name)
	}

	defer func() {
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
			updateRequired = true
		}
	}()

	// TODO: When online rebuilding related APIs are ready, we need to create a separate and temporary lvol for snapshot lvol rebuilding
	// lvolName := GetReplicaRebuildingLvolName(r.Name)
	// if _, err := spdkClient.BdevLvolCreate("", r.LvsUUID, lvolName, util.BytesToMiB(r.SpecSize), "", true); err != nil {
	// 	return "", err
	// }
	// bdevLvolList, err := spdkClient.BdevLvolGet(r.Alias, 0)
	// if err != nil {
	// 	return "", err
	// }
	// if len(bdevLvolList) < 1 {
	// 	return "", fmt.Errorf("cannot find lvol %v after rebuilding lvol creation", spdktypes.GetLvolAlias(r.LvsName, lvolName))
	// }
	// r.rebuildingLvol = &Lvol{
	// 	Name:     lvolName,
	// 	UUID:     bdevLvolList[0].UUID,
	// 	Alias:    bdevLvolList[0].Aliases[0],
	// 	SpecSize: r.SpecSize,
	// 	Children: map[string]*Lvol{},
	// }
	// if exposeRequired {
	// 	r.rebuildingPort, _, err = r.portAllocator.AllocateRange(1)
	// 	if err != nil {
	// 		return "", err
	// 	}
	// 	if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(lvolName), r.rebuildingLvol.UUID, r.IP, strconv.Itoa(int(r.rebuildingPort))); err != nil {
	// 		return "", err
	// 	}
	// }

	r.isRebuilding = true
	// TODO: Currently, the online rebuilding related APIs are not ready, we use the exposed head lvol for snapshot lvol rebuilding
	// TODO: Will use r.portAllocator rather than the reserved first port for rebuilding in the future version
	r.rebuildingLvol = r.ActiveChain[r.ChainLength-1]
	if exposeRequired {
		if !r.IsExposed {
			if err := spdkClient.StartExposeBdev(helpertypes.GetNQN(r.rebuildingLvol.Name), r.rebuildingLvol.UUID, r.IP, strconv.Itoa(int(r.PortStart))); err != nil {
				return "", err
			}
			r.IsExposed = true
		}
		r.rebuildingPort = r.PortStart
	}

	return net.JoinHostPort(r.IP, strconv.Itoa(int(r.rebuildingPort))), nil
}

func (r *Replica) RebuildingDstFinish(spdkClient *spdkclient.Client, unexposeRequired bool) (err error) {
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
	if r.rebuildingLvol == nil {
		return fmt.Errorf("cannot find rebuilding lvol for replica %s rebuilding finish", r.Name)
	}

	defer func() {
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
			updateRequired = true
		}
		r.isRebuilding = false
	}()

	// TODO: For the online rebuilding, Remove the temporary rebuilding lvol and release ports for r.portAllocator
	// if err := r.rebuildingDstCleanup(spdkClient); err != nil {
	// 	return err
	// }

	// For the current offline rebuilding, the temporary rebuilding lvol is actually the head lvol hence there is no need to remove it
	if unexposeRequired {
		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.rebuildingLvol.Name)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			return err
		}
		r.IsExposed = false
	}
	r.rebuildingPort = 0
	r.rebuildingLvol = nil

	// TODO: For online rebuilding, connect the snapshot tree/chain with the head lvol then mark the head lvol as mode RW

	bdevLvolList, err := spdkClient.BdevLvolGet("", 0)
	if err != nil {
		return err
	}
	bdevLvolMap := map[string]*spdktypes.BdevInfo{}
	for idx := range bdevLvolList {
		bdevLvol := &bdevLvolList[idx]
		lvolName := spdktypes.GetLvolNameFromAlias(bdevLvol.Aliases[0])
		bdevLvolMap[lvolName] = bdevLvol
	}

	return r.construct(bdevLvolMap)
}

// func (r *Replica) rebuildingDstCleanup(spdkClient *spdkclient.Client) error {
// 	if r.rebuildingPort != 0 {
// 		if err := spdkClient.StopExposeBdev(helpertypes.GetNQN(r.rebuildingLvol.Name)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
// 			return err
// 		}
// 		if err := r.portAllocator.ReleaseRange(r.rebuildingPort, r.rebuildingPort); err != nil {
// 			return err
// 		}
// 		r.rebuildingPort = 0
// 	}
// 	if r.rebuildingLvol != nil {
// 		if _, err := spdkClient.BdevLvolDelete(r.rebuildingLvol.UUID); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
// 			return err
// 		}
// 		r.rebuildingLvol = nil
// 	}
// 	return nil
// }

func (r *Replica) RebuildingDstSnapshotCreate(spdkClient *spdkclient.Client, snapshotName string) (err error) {
	updateRequired := false

	r.Lock()
	defer func() {
		r.Unlock()

		if updateRequired {
			r.UpdateCh <- nil
		}
	}()

	if r.State != types.InstanceStateRunning {
		return fmt.Errorf("invalid state %v for replica %s rebuilding snapshot %s creation", r.State, r.Name, snapshotName)
	}
	if !r.isRebuilding {
		return fmt.Errorf("replica %s is not in rebuilding", r.Name)
	}
	if r.rebuildingLvol == nil || (r.IsExposed && r.rebuildingPort == 0) {
		return fmt.Errorf("rebuilding lvol is not existed, or exposed without rebuilding port for replica %s rebuilding snapshot %s creation", r.Name, snapshotName)
	}

	defer func() {
		if err != nil && r.State != types.InstanceStateError {
			r.State = types.InstanceStateError
			updateRequired = true
		}
	}()

	snapLvolName := GetReplicaSnapshotLvolName(r.Name, snapshotName)
	snapUUID, err := spdkClient.BdevLvolSnapshot(r.rebuildingLvol.UUID, snapLvolName)
	if err != nil {
		return err
	}

	bdevLvolList, err := spdkClient.BdevLvolGet(snapUUID, 0)
	if err != nil {
		return err
	}
	if len(bdevLvolList) != 1 {
		return fmt.Errorf("zero or multiple snap lvols with UUID %s found after rebuilding snapshot %s creation", snapUUID, snapshotName)
	}
	snapSvcLvol := BdevLvolInfoToServiceLvol(&bdevLvolList[0])

	// Do not update r.ActiveChain since we don't know which branch of the snapshot tree is the active one.
	r.SnapshotMap[snapshotName] = snapSvcLvol
	snapSvcLvol.Children[r.rebuildingLvol.Name] = r.rebuildingLvol
	r.rebuildingLvol.Parent = snapSvcLvol.Name
	updateRequired = true

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
