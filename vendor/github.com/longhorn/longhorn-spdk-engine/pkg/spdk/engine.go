package spdk

import (
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

	commonNet "github.com/longhorn/go-common-libs/net"
	commonTypes "github.com/longhorn/go-common-libs/types"
	commonUtils "github.com/longhorn/go-common-libs/utils"
	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/go-spdk-helper/pkg/nvme"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/api"
	"github.com/longhorn/longhorn-spdk-engine/pkg/client"
	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
)

type Engine struct {
	sync.RWMutex

	Name               string
	VolumeName         string
	SpecSize           uint64
	ActualSize         uint64
	ReplicaAddressMap  map[string]string
	ReplicaBdevNameMap map[string]string
	ReplicaModeMap     map[string]types.Mode
	IP                 string
	Port               int32
	TargetIP           string
	TargetPort         int32
	Frontend           string
	Endpoint           string
	Nqn                string
	Nguid              string

	dmDeviceBusy bool

	State    types.InstanceState
	ErrorMsg string

	Head        *api.Lvol
	SnapshotMap map[string]*api.Lvol

	IsRestoring bool

	// UpdateCh should not be protected by the engine lock
	UpdateCh chan interface{}

	log logrus.FieldLogger
}

func NewEngine(engineName, volumeName, frontend string, specSize uint64, engineUpdateCh chan interface{}) *Engine {
	log := logrus.StandardLogger().WithFields(logrus.Fields{
		"engineName": engineName,
		"volumeName": volumeName,
		"frontend":   frontend,
	})

	roundedSpecSize := util.RoundUp(specSize, helpertypes.MiB)
	if roundedSpecSize != specSize {
		log.Infof("Rounded up spec size from %v to %v since the spec size should be multiple of MiB", specSize, roundedSpecSize)
	}
	log.WithField("specSize", roundedSpecSize)

	return &Engine{
		Name:               engineName,
		VolumeName:         volumeName,
		Frontend:           frontend,
		SpecSize:           specSize,
		ReplicaAddressMap:  map[string]string{},
		ReplicaBdevNameMap: map[string]string{},
		ReplicaModeMap:     map[string]types.Mode{},

		State: types.InstanceStatePending,

		SnapshotMap: map[string]*api.Lvol{},

		UpdateCh: engineUpdateCh,

		log: log,
	}
}

func (e *Engine) Create(spdkClient *spdkclient.Client, replicaAddressMap map[string]string, portCount int32, superiorPortAllocator *util.Bitmap, initiatorAddress, targetAddress string, upgradeRequired bool) (ret *spdkrpc.Engine, err error) {
	logrus.WithFields(logrus.Fields{
		"portCount":         portCount,
		"upgradeRequired":   upgradeRequired,
		"replicaAddressMap": replicaAddressMap,
		"initiatorAddress":  initiatorAddress,
		"targetAddress":     targetAddress,
	}).Info("Creating engine")

	requireUpdate := true

	e.Lock()
	defer func() {
		e.Unlock()

		if requireUpdate {
			e.UpdateCh <- nil
		}
	}()

	podIP, err := commonNet.GetIPForPod()
	if err != nil {
		return nil, err
	}

	initiatorIP, _, err := splitHostPort(initiatorAddress)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to split initiator address %v", initiatorAddress)
	}
	targetIP, _, err := splitHostPort(targetAddress)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to split target address %v", targetAddress)
	}

	if e.State != types.InstanceStatePending {
		switchingOverBack := e.State == types.InstanceStateRunning && initiatorIP == targetIP
		if !switchingOverBack {
			requireUpdate = false
			return nil, fmt.Errorf("invalid state %s for engine %s creation", e.State, e.Name)
		}
	}

	defer func() {
		if err != nil {
			e.log.WithError(err).Errorf("Failed to create engine %s", e.Name)
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
			}
			e.ErrorMsg = err.Error()

			ret = e.getWithoutLock()
			err = nil
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	e.Nqn = helpertypes.GetNQN(e.Name)

	replicaBdevList := []string{}

	initiatorCreationRequired := true
	if !upgradeRequired {
		if e.IP == "" {
			if initiatorIP != targetIP {
				// For creating target on another node
				initiatorCreationRequired = false
				e.log.Info("Creating an target engine")
				e.TargetIP = podIP
			} else {
				// For newly creating engine
				e.log.Info("Creating an new engine")
				e.IP = podIP
				e.TargetIP = podIP
			}

			e.log = e.log.WithField("ip", e.IP)
		} else {
			if initiatorIP != targetIP {
				return nil, errors.Errorf("unsupported operation: engine ip=%v, initiator address=%v, target address=%v", e.IP, initiatorAddress, targetAddress)
			}

			// For creating target on attached node
			initiatorCreationRequired = false
		}

		for replicaName, replicaAddr := range replicaAddressMap {
			bdevName, err := e.connectReplica(spdkClient, replicaName, replicaAddr)
			if err != nil {
				e.log.WithError(err).Errorf("Failed to get bdev from replica %s with address %s, will skip it and continue", replicaName, replicaAddr)
				e.ReplicaModeMap[replicaName] = types.ModeERR
				e.ReplicaBdevNameMap[replicaName] = ""
				continue
			}
			// TODO: Check if a replica is really a RW replica rather than a rebuilding failed replica
			e.ReplicaModeMap[replicaName] = types.ModeRW
			e.ReplicaBdevNameMap[replicaName] = bdevName
			replicaBdevList = append(replicaBdevList, bdevName)
		}
		e.ReplicaAddressMap = replicaAddressMap
		e.log = e.log.WithField("replicaAddressMap", replicaAddressMap)

		e.CheckAndUpdateInfoFromReplica()

		e.log.Info("Launching raid during engine creation")
		if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList); err != nil {
			return nil, err
		}
	} else {
		// For reconstructing engine after switching over target to another node
		initiatorCreationRequired = false

		e.IP = targetIP

		// Get ReplicaModeMap and ReplicaBdevNameMap
		targetSPDKClient, err := GetServiceClient(net.JoinHostPort(e.IP, strconv.Itoa(types.SPDKServicePort)))
		if err != nil {
			return nil, err
		}

		var engineWithTarget *api.Engine
		if initiatorIP != targetIP {
			engineWithTarget, err = targetSPDKClient.EngineGet(e.Name)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get engine %v from %v", e.Name, targetAddress)
			}
		} else {
			engineWithTarget = api.ProtoEngineToEngine(e.getWithoutLock())
		}

		for replicaName, replicaAddr := range replicaAddressMap {
			_, ok := engineWithTarget.ReplicaAddressMap[replicaName]
			if !ok {
				e.log.WithError(err).Errorf("Failed to get bdev from replica %s with address %s, will skip it and continue", replicaName, replicaAddr)
				e.ReplicaModeMap[replicaName] = types.ModeERR
				e.ReplicaBdevNameMap[replicaName] = ""
				continue
			}

			e.ReplicaModeMap[replicaName] = types.ModeRW
			e.ReplicaBdevNameMap[replicaName] = replicaName
			replicaBdevList = append(replicaBdevList, replicaName)
		}

		e.ReplicaAddressMap = replicaAddressMap
		e.log = e.log.WithField("replicaAddressMap", replicaAddressMap)
	}

	e.log.Info("Launching frontend during engine creation")
	if err := e.handleFrontend(spdkClient, portCount, superiorPortAllocator, initiatorCreationRequired, upgradeRequired, initiatorAddress, targetAddress); err != nil {
		return nil, err
	}

	e.State = types.InstanceStateRunning

	e.log.Info("Created engine")

	return e.getWithoutLock(), nil
}

func (e *Engine) connectReplica(spdkClient *spdkclient.Client, replicaName, replicaAddress string) (bdevName string, err error) {
	replicaIP, replicaPort, err := net.SplitHostPort(replicaAddress)
	if err != nil {
		return "", err
	}

	nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(replicaName, helpertypes.GetNQN(replicaName),
		replicaIP, replicaPort, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4,
		helpertypes.DefaultCtrlrLossTimeoutSec, helpertypes.DefaultReconnectDelaySec, helpertypes.DefaultFastIOFailTimeoutSec,
		helpertypes.DefaultMultipath)
	if err != nil {
		return "", err
	}

	if len(nvmeBdevNameList) != 1 {
		return "", fmt.Errorf("got zero or multiple results when attaching replica %s with address %s as a NVMe bdev: %+v", replicaName, replicaAddress, nvmeBdevNameList)
	}

	return nvmeBdevNameList[0], nil
}

func (e *Engine) handleFrontend(spdkClient *spdkclient.Client, portCount int32, superiorPortAllocator *util.Bitmap, initiatorCreationRequired, upgradeRequired bool, initiatorAddress, targetAddress string) (err error) {
	if !types.IsFrontendSupported(e.Frontend) {
		return fmt.Errorf("unknown frontend type %s", e.Frontend)
	}

	if e.Frontend == types.FrontendEmpty {
		e.log.Infof("No frontend specified, will not expose the volume %s", e.VolumeName)
		return nil
	}

	initiatorIP, _, err := splitHostPort(initiatorAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to split initiator address %v", initiatorAddress)
	}

	targetIP, targetPort, err := splitHostPort(targetAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to split target address %v", targetAddress)
	}

	e.Nqn = helpertypes.GetNQN(e.Name)

	var port int32
	if !upgradeRequired {
		e.Nguid = commonUtils.RandomID(nvmeNguidLength)

		e.log.Info("Blindly stopping expose bdev for engine")
		if err := spdkClient.StopExposeBdev(e.Nqn); err != nil {
			return errors.Wrapf(err, "failed to blindly stop expose bdev for engine %v", e.Name)
		}

		port, _, err = superiorPortAllocator.AllocateRange(portCount)
		if err != nil {
			return err
		}

		e.log.Infof("Allocated port %v", port)
		if err := spdkClient.StartExposeBdev(e.Nqn, e.Name, e.Nguid, targetIP, strconv.Itoa(int(port))); err != nil {
			return err
		}

		if initiatorCreationRequired {
			e.Port = port
			e.TargetPort = port
		} else {
			e.TargetPort = port
		}
	} else {
		e.Port = targetPort
	}

	if e.Frontend == types.FrontendSPDKTCPNvmf {
		e.Endpoint = GetNvmfEndpoint(e.Nqn, targetIP, port)
		return nil
	}

	if initiatorIP != targetIP && !upgradeRequired {
		e.log.Infof("Initiator IP %v is different from target IP %s, will not start initiator for engine", initiatorIP, targetIP)
		return nil
	}

	initiator, err := nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %v", e.Name)
	}

	dmDeviceBusy := false
	if initiatorCreationRequired {
		e.log.Info("Starting initiator for engine")
		dmDeviceBusy, err = initiator.Start(targetIP, strconv.Itoa(int(port)), true)
		if err != nil {
			return errors.Wrapf(err, "failed to start initiator for engine %v", e.Name)
		}
	} else {
		e.log.Info("Loading NVMe device info for engine")
		err = initiator.LoadNVMeDeviceInfo(initiator.TransportAddress, initiator.TransportServiceID, initiator.SubsystemNQN)
		if err != nil {
			if nvme.IsValidNvmeDeviceNotFound(err) {
				dmDeviceBusy, err = initiator.Start(targetIP, strconv.Itoa(int(targetPort)), true)
				if err != nil {
					return errors.Wrapf(err, "failed to start initiator for engine %v", e.Name)
				}
			} else {
				return errors.Wrapf(err, "failed to load NVMe device info for engine %v", e.Name)
			}
		}
		err = initiator.LoadEndpoint(false)
		if err != nil {
			return errors.Wrapf(err, "failed to load endpoint for engine %v", e.Name)
		}
		//dmDeviceBusy = true
	}

	e.dmDeviceBusy = dmDeviceBusy
	e.Endpoint = initiator.GetEndpoint()

	e.log = e.log.WithFields(logrus.Fields{
		"endpoint": e.Endpoint,
		"port":     port,
	})

	return nil
}

func (e *Engine) Delete(spdkClient *spdkclient.Client, superiorPortAllocator *util.Bitmap) (err error) {
	e.log.Info("Deleting engine")

	requireUpdate := false

	e.Lock()
	defer func() {
		// Considering that there may be still pending validations, it's better to update the state after the deletion.
		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				e.ErrorMsg = err.Error()
				e.log.WithError(err).Error("Failed to delete engine")
				requireUpdate = true
			}
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
		if e.State == types.InstanceStateRunning {
			e.State = types.InstanceStateTerminating
			requireUpdate = true
		}

		e.Unlock()

		if requireUpdate {
			e.UpdateCh <- nil
		}
	}()

	if e.Endpoint != "" {
		nqn := helpertypes.GetNQN(e.Name)

		initiator, err := nvme.NewInitiator(e.VolumeName, nqn, nvme.HostProc)
		if err != nil {
			return err
		}
		if _, err := initiator.Stop(true, true, true); err != nil {
			return err
		}

		if err := spdkClient.StopExposeBdev(nqn); err != nil {
			return err
		}

		e.Endpoint = ""
		requireUpdate = true
	}

	if e.TargetPort != 0 || e.Port != 0 {
		port := e.TargetPort
		if port == 0 {
			port = e.Port
		}
		if err := superiorPortAllocator.ReleaseRange(port, port); err != nil {
			return err
		}
		e.TargetPort = 0
		e.Port = 0
		requireUpdate = true
	}

	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	for replicaName := range e.ReplicaAddressMap {
		if err := e.disconnectReplica(spdkClient, replicaName); err != nil {
			if e.ReplicaModeMap[replicaName] != types.ModeERR {
				e.ReplicaModeMap[replicaName] = types.ModeERR
				requireUpdate = true
			}
			return err
		}

		delete(e.ReplicaAddressMap, replicaName)
		delete(e.ReplicaBdevNameMap, replicaName)
		delete(e.ReplicaModeMap, replicaName)
		requireUpdate = true
	}

	e.log.Info("Deleted engine")

	return nil
}

func (e *Engine) disconnectReplica(spdkClient *spdkclient.Client, replicaName string) (err error) {
	bdevName := e.ReplicaBdevNameMap[replicaName]
	if bdevName == "" {
		return nil
	}
	if _, err := spdkClient.BdevNvmeDetachController(helperutil.GetNvmeControllerNameFromNamespaceName(bdevName)); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	return nil
}

func (e *Engine) Get() (res *spdkrpc.Engine) {
	e.RLock()
	defer e.RUnlock()

	return e.getWithoutLock()
}

func (e *Engine) getWithoutLock() (res *spdkrpc.Engine) {
	res = &spdkrpc.Engine{
		Name:              e.Name,
		SpecSize:          e.SpecSize,
		ActualSize:        e.ActualSize,
		ReplicaAddressMap: e.ReplicaAddressMap,
		ReplicaModeMap:    map[string]spdkrpc.ReplicaMode{},
		Ip:                e.IP,
		Port:              e.Port,
		TargetIp:          e.TargetIP,
		TargetPort:        e.TargetPort,
		Snapshots:         map[string]*spdkrpc.Lvol{},
		Frontend:          e.Frontend,
		Endpoint:          e.Endpoint,
		State:             string(e.State),
		ErrorMsg:          e.ErrorMsg,
	}

	for replicaName, replicaMode := range e.ReplicaModeMap {
		res.ReplicaModeMap[replicaName] = types.ReplicaModeToGRPCReplicaMode(replicaMode)
	}
	res.Head = api.LvolToProtoLvol(e.Head)
	for snapshotName, snapApiLvol := range e.SnapshotMap {
		res.Snapshots[snapshotName] = api.LvolToProtoLvol(snapApiLvol)
	}

	return res
}

func (e *Engine) ValidateAndUpdate(spdkClient *spdkclient.Client) (err error) {
	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	if e.IsRestoring {
		e.log.Info("Engine is restoring, will skip the validation and update")
		return nil
	}

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		return nil
	}

	subsystemMap, err := GetNvmfSubsystemMap(spdkClient)
	if err != nil {
		return err
	}
	bdevMap, err := GetBdevMap(spdkClient)
	if err != nil {
		return err
	}

	defer func() {
		// TODO: we may not need to mark the engine as ERR for each error
		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				e.log.WithError(err).Error("Found error during engine validation and update")
				updateRequired = true
			}
			e.ErrorMsg = err.Error()
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	podIP, err := commonNet.GetIPForPod()
	if err != nil {
		return err
	}
	if e.IP != podIP {
		// Skip the validation if the engine is being upgraded
		if engineOnlyContainsInitiator(e) || engineOnlyContainsTarget(e) {
			return nil
		}
		return fmt.Errorf("found mismatching between engine IP %s and pod IP %s for engine %v", e.IP, podIP, e.Name)
	}

	if err := e.validateAndUpdateFrontend(subsystemMap); err != nil {
		return err
	}

	bdevRaid := bdevMap[e.Name]
	if spdktypes.GetBdevType(bdevRaid) != spdktypes.BdevTypeRaid {
		return fmt.Errorf("cannot find a raid bdev for engine %v", e.Name)
	}
	bdevRaidSize := bdevRaid.NumBlocks * uint64(bdevRaid.BlockSize)
	if e.SpecSize != bdevRaidSize {
		return fmt.Errorf("found mismatching between engine spec size %d and actual raid bdev size %d for engine %s", e.SpecSize, bdevRaidSize, e.Name)
	}

	// Verify engine map consistency
	for replicaName := range e.ReplicaAddressMap {
		if _, exists := e.ReplicaBdevNameMap[replicaName]; !exists {
			e.ReplicaBdevNameMap[replicaName] = ""
			e.ReplicaModeMap[replicaName] = types.ModeERR
			e.log.Errorf("Mark replica %s as mode ERR since it is not found in engine %s bdev name map", replicaName, e.Name)
		}
		if _, exists := e.ReplicaModeMap[replicaName]; !exists {
			e.ReplicaModeMap[replicaName] = types.ModeERR
			e.log.Errorf("Mark replica %s as mode ERR since it is not found in engine %s mode map", replicaName, e.Name)
		}
	}
	for replicaName := range e.ReplicaBdevNameMap {
		if _, exists := e.ReplicaAddressMap[replicaName]; !exists {
			e.ReplicaAddressMap[replicaName] = ""
			e.ReplicaModeMap[replicaName] = types.ModeERR
			e.log.Errorf("Mark replica %s as mode ERR since it is not found in engine %s address map", replicaName, e.Name)
		}
		if _, exists := e.ReplicaModeMap[replicaName]; !exists {
			e.ReplicaModeMap[replicaName] = types.ModeERR
			e.log.Errorf("Mark replica %s as mode ERR since it is not found in engine %s mode map", replicaName, e.Name)
		}
	}
	// Now e.ReplicaAddressMap and e.ReplicaBdevNameMap should have the same key set
	for replicaName := range e.ReplicaModeMap {
		if _, exists := e.ReplicaAddressMap[replicaName]; !exists {
			delete(e.ReplicaModeMap, replicaName)
			e.log.Errorf("Remove replica %s for the mode map since it is not found in engine %s address map", replicaName, e.Name)
		}
		if _, exists := e.ReplicaBdevNameMap[replicaName]; !exists {
			delete(e.ReplicaBdevNameMap, replicaName)
			e.log.Errorf("Remove replica %s for the mode map since it is not found in engine %s bdev name map", replicaName, e.Name)
		}
	}
	e.log = e.log.WithField("replicaAddressMap", e.ReplicaAddressMap)

	containValidReplica := false
	for replicaName, bdevName := range e.ReplicaBdevNameMap {
		if e.ReplicaModeMap[replicaName] == types.ModeERR || e.ReplicaModeMap[replicaName] == types.ModeWO {
			continue
		}
		mode, err := e.validateAndUpdateReplicaMode(replicaName, bdevMap[bdevName])
		if err != nil {
			if e.ReplicaModeMap[replicaName] != types.ModeERR {
				e.log.WithError(err).Errorf("Replica %v is invalid, will update the mode from %s to %s", replicaName, e.ReplicaModeMap[replicaName], types.ModeERR)
				e.ReplicaModeMap[replicaName] = types.ModeERR
				updateRequired = true
			}
			continue
		}
		if e.ReplicaModeMap[replicaName] != mode {
			e.log.Debugf("Replica %s mode is updated from %s to %s", replicaName, e.ReplicaModeMap[replicaName], mode)
			e.ReplicaModeMap[replicaName] = mode
			updateRequired = true
		}
		if e.ReplicaModeMap[replicaName] == types.ModeRW {
			containValidReplica = true
		}
	}
	if !containValidReplica {
		e.State = types.InstanceStateError
		updateRequired = true
		// TODO: should we delete the engine automatically here?
	}

	e.CheckAndUpdateInfoFromReplica()

	return nil
}

func (e *Engine) CheckAndUpdateInfoFromReplica() {
	replicaMap := map[string]*api.Replica{}
	replicaAncestorMap := map[string]*api.Lvol{}
	// hasBackingImage := false
	hasSnapshot := false
	for replicaName, address := range e.ReplicaAddressMap {
		if e.ReplicaModeMap[replicaName] != types.ModeRW {
			continue
		}
		replicaServiceCli, err := GetServiceClient(address)
		if err != nil {
			e.log.WithError(err).Warnf("failed to get service client for replica %s with address %s, will skip this replica and continue info update from replica", replicaName, address)
			continue
		}
		replica, err := replicaServiceCli.ReplicaGet(replicaName)
		if err != nil {
			e.log.WithError(err).Warnf("failed to get replica %s with address %s, will skip this replica and continue info update from replica", replicaName, address)
			continue
		}

		// The ancestor check sequence: the backing image, then the oldest snapshot, finally head
		// TODO: Check the backing image first

		// if replica.BackingImage != nil {
		//	hasBackingImage = true
		//	replicaAncestorMap[replicaName] = replica.BackingImage
		// } else
		if len(replica.Snapshots) != 0 {
			// if hasBackingImage {
			//	e.log.Warnf("Found replica %s does not have a backing image while other replicas have during info update from replica", replicaName)
			// } else {}
			hasSnapshot = true
			for snapshotName, snapApiLvol := range replica.Snapshots {
				if snapApiLvol.Parent == "" {
					replicaAncestorMap[replicaName] = replica.Snapshots[snapshotName]
					break
				}
			}
		} else {
			if hasSnapshot {
				e.log.Warnf("Found replica %s does not have a snapshot while other replicas have during info update from replica", replicaName)
			} else {
				replicaAncestorMap[replicaName] = replica.Head
			}
		}
		if replicaAncestorMap[replicaName] == nil {
			e.log.Warnf("Cannot find replica %s ancestor, will skip this replica and continue info update from replica", replicaName)
			continue
		}
		replicaMap[replicaName] = replica
	}

	// If there are multiple candidates, the priority is:
	//  1. the earliest backing image if one replica contains a backing image
	//  2. the earliest snapshot if one replica contains a snapshot
	//  3. the earliest volume head
	candidateReplicaName := ""
	earliestCreationTime := time.Now()
	for replicaName, ancestorApiLvol := range replicaAncestorMap {
		// if hasBackingImage {
		//	if ancestorApiLvol.Name == types.VolumeHead || IsReplicaSnapshotLvol(replicaName, ancestorApiLvol.Name) {
		//		continue
		//	}
		// } else
		if hasSnapshot {
			if ancestorApiLvol.Name == types.VolumeHead {
				continue
			}
		} else {
			if ancestorApiLvol.Name != types.VolumeHead {
				continue
			}
		}

		creationTime, err := time.Parse(time.RFC3339, ancestorApiLvol.CreationTime)
		if err != nil {
			e.log.WithError(err).Warnf("Failed to parse replica %s ancestor creation time, will skip this replica and continue info update from replica: %+v", replicaName, ancestorApiLvol)
			continue
		}
		if earliestCreationTime.After(creationTime) {
			earliestCreationTime = creationTime
			e.SnapshotMap = replicaMap[replicaName].Snapshots
			e.Head = replicaMap[replicaName].Head
			e.ActualSize = replicaMap[replicaName].ActualSize
			if candidateReplicaName != replicaName {
				if candidateReplicaName != "" && replicaAncestorMap[candidateReplicaName].Name != ancestorApiLvol.Name {
					e.log.Warnf("Comparing with replica %s ancestor %s, replica %s has a different and earlier ancestor %s, will update info from this replica", candidateReplicaName, replicaAncestorMap[candidateReplicaName].Name, replicaName, ancestorApiLvol.Name)
				}
				candidateReplicaName = replicaName
			}
		}
	}
}

func (e *Engine) validateAndUpdateFrontend(subsystemMap map[string]*spdktypes.NvmfSubsystem) (err error) {
	if e.Frontend != types.FrontendEmpty && e.Frontend != types.FrontendSPDKTCPNvmf && e.Frontend != types.FrontendSPDKTCPBlockdev {
		return fmt.Errorf("unknown frontend type %s", e.Frontend)
	}

	nqn := helpertypes.GetNQN(e.Name)
	subsystem := subsystemMap[nqn]

	if e.Frontend == types.FrontendEmpty {
		if subsystem != nil {
			return fmt.Errorf("found nvmf subsystem %s for engine %s with empty frontend", nqn, e.Name)
		}
		if e.Endpoint != "" {
			return fmt.Errorf("found non-empty endpoint %s for engine %s with empty frontend", e.Endpoint, e.Name)
		}
		if e.Port != 0 {
			return fmt.Errorf("found non-zero port %v for engine %s with empty frontend", e.Port, e.Name)
		}
		return nil
	}

	if subsystem == nil || len(subsystem.ListenAddresses) == 0 {
		return fmt.Errorf("cannot find the Nvmf subsystem for engine %s", e.Name)
	}

	port := 0
	for _, listenAddr := range subsystem.ListenAddresses {
		if !strings.EqualFold(string(listenAddr.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
			!strings.EqualFold(string(listenAddr.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
			continue
		}
		if port, err = strconv.Atoi(listenAddr.Trsvcid); err != nil {
			return err
		}
		if e.Port == int32(port) {
			break
		}
	}
	if port == 0 || e.Port != int32(port) {
		return fmt.Errorf("cannot find a matching listener with port %d from Nvmf subsystem for engine %s", e.Port, e.Name)
	}

	switch e.Frontend {
	case types.FrontendSPDKTCPBlockdev:
		initiator, err := nvme.NewInitiator(e.VolumeName, nqn, nvme.HostProc)
		if err != nil {
			return err
		}
		if err := initiator.LoadNVMeDeviceInfo(initiator.TransportAddress, initiator.TransportServiceID, initiator.SubsystemNQN); err != nil {
			if strings.Contains(err.Error(), "connecting state") ||
				strings.Contains(err.Error(), "resetting state") {
				e.log.WithError(err).Warnf("Ignored to validate and update engine %v, because the device is still in a transient state", e.Name)
				return nil
			}
			return err
		}
		if err := initiator.LoadEndpoint(e.dmDeviceBusy); err != nil {
			return err
		}
		blockDevEndpoint := initiator.GetEndpoint()
		if e.Endpoint == "" {
			e.Endpoint = blockDevEndpoint
		}
		if e.Endpoint != blockDevEndpoint {
			return fmt.Errorf("found mismatching between engine endpoint %s and actual block device endpoint %s for engine %s", e.Endpoint, blockDevEndpoint, e.Name)
		}
	case types.FrontendSPDKTCPNvmf:
		nvmfEndpoint := GetNvmfEndpoint(nqn, e.IP, e.Port)
		if e.Endpoint == "" {
			e.Endpoint = nvmfEndpoint
		}
		if e.Endpoint != "" && e.Endpoint != nvmfEndpoint {
			return fmt.Errorf("found mismatching between engine endpoint %s and actual nvmf endpoint %s for engine %s", e.Endpoint, nvmfEndpoint, e.Name)
		}
	default:
		return fmt.Errorf("unknown frontend type %s", e.Frontend)
	}

	return nil
}

func (e *Engine) validateAndUpdateReplicaMode(replicaName string, bdev *spdktypes.BdevInfo) (mode types.Mode, err error) {
	if bdev == nil {
		return types.ModeERR, fmt.Errorf("cannot find a bdev for replica %s", replicaName)
	}
	bdevSpecSize := bdev.NumBlocks * uint64(bdev.BlockSize)
	if e.SpecSize != bdevSpecSize {
		return types.ModeERR, fmt.Errorf("found mismatching between replica %s bdev spec size %d and the engine spec size %d for engine %s", replicaName, bdevSpecSize, e.SpecSize, e.Name)
	}
	switch spdktypes.GetBdevType(bdev) {
	case spdktypes.BdevTypeLvol:
		replicaIP, _, err := net.SplitHostPort(e.ReplicaAddressMap[replicaName])
		if err != nil {
			return types.ModeERR, err
		}
		if e.IP != replicaIP {
			return types.ModeERR, fmt.Errorf("found mismatching between replica %s IP %s and the engine IP %s for engine %s", replicaName, replicaIP, e.IP, e.Name)
		}
	case spdktypes.BdevTypeNvme:
		if len(*bdev.DriverSpecific.Nvme) != 1 {
			return types.ModeERR, fmt.Errorf("found zero or multiple nvme info in a remote nvme base bdev %v for replica %s", bdev.Name, replicaName)
		}
		nvmeInfo := (*bdev.DriverSpecific.Nvme)[0]
		if !strings.EqualFold(string(nvmeInfo.Trid.Adrfam), string(spdktypes.NvmeAddressFamilyIPv4)) ||
			!strings.EqualFold(string(nvmeInfo.Trid.Trtype), string(spdktypes.NvmeTransportTypeTCP)) {
			return types.ModeERR, fmt.Errorf("found invalid address family %s and transport type %s in a remote nvme base bdev %s for replica %s", nvmeInfo.Trid.Adrfam, nvmeInfo.Trid.Trtype, bdev.Name, replicaName)
		}
		bdevAddr := net.JoinHostPort(nvmeInfo.Trid.Traddr, nvmeInfo.Trid.Trsvcid)
		if e.ReplicaAddressMap[replicaName] != bdevAddr {
			return types.ModeERR, fmt.Errorf("found mismatching between replica %s bdev address %s and the nvme bdev actual address %s", replicaName, e.ReplicaAddressMap[replicaName], bdevAddr)
		}
		// TODO: Validate NVMe controller state
		// TODO: Verify Mode WO
	default:
		return types.ModeERR, fmt.Errorf("found invalid bdev type %v for replica %s ", spdktypes.GetBdevType(bdev), replicaName)
	}

	return types.ModeRW, nil
}

func (e *Engine) ReplicaAddStart(spdkClient *spdkclient.Client, replicaName, replicaAddress string) (err error) {
	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		return fmt.Errorf("invalid state %v for engine %s replica %s add start", e.State, e.Name, replicaName)
	}

	if e.Frontend != types.FrontendEmpty {
		return fmt.Errorf("invalid frontend %v for engine %s replica %s add start", e.Frontend, e.Name, replicaName)
	}

	if _, exists := e.ReplicaAddressMap[replicaName]; exists {
		return fmt.Errorf("replica %s already exists", replicaName)
	}

	replicaClients, err := e.getReplicaClients()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				updateRequired = true
			}
			e.ErrorMsg = err.Error()
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	// TODO: For online rebuilding, the IO should be paused first
	snapshotName := GenerateRebuildingSnapshotName()
	opts := &api.SnapshotOptions{
		Timestamp: time.Now().String(),
	}
	updateRequired, err = e.snapshotOperationWithoutLock(spdkClient, replicaClients, snapshotName, SnapshotOperationCreate, opts)
	if err != nil {
		return err
	}

	// TODO: For online rebuilding, this replica should be attached (if it's a remote one) then added to the RAID base bdev list with mode WO
	e.ReplicaAddressMap[replicaName] = replicaAddress
	e.ReplicaBdevNameMap[replicaName] = ""
	e.ReplicaModeMap[replicaName] = types.ModeWO
	e.log = e.log.WithField("replicaAddressMap", e.ReplicaAddressMap)

	return nil
}

func (e *Engine) ReplicaAddFinish(spdkClient *spdkclient.Client, replicaName, replicaAddress string, localReplicaLvsNameMap map[string]string) (err error) {
	updateRequired := false

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		return fmt.Errorf("invalid state %v for engine %s replica %s add finish", e.State, e.Name, replicaName)
	}

	if e.Frontend != types.FrontendEmpty {
		return fmt.Errorf("invalid frontend %v for engine %s replica %s add finish", e.Frontend, e.Name, replicaName)
	}

	if _, exists := e.ReplicaAddressMap[replicaName]; !exists {
		return fmt.Errorf("replica %s does not exist in engine %s", replicaName, e.Name)
	}

	if e.ReplicaModeMap[replicaName] != types.ModeWO {
		return fmt.Errorf("invalid mode %s for engine %s replica %s add finish", e.ReplicaModeMap[replicaName], e.Name, replicaName)
	}

	replicaBdevList := []string{}
	for rName, rMode := range e.ReplicaModeMap {
		if rMode == types.ModeRW {
			replicaBdevList = append(replicaBdevList, e.ReplicaBdevNameMap[rName])
		}
		if rName == replicaName {
			bdevName, err := e.connectReplica(spdkClient, replicaName, replicaAddress)
			if err != nil {
				e.log.WithError(err).Errorf("Failed to get bdev from rebuilding replica %s with address %s, will mark it as ERR and error out", replicaName, replicaAddress)
				e.ReplicaModeMap[replicaName] = types.ModeERR
				e.ReplicaBdevNameMap[replicaName] = ""
				updateRequired = true
				return err
			}
			e.ReplicaModeMap[replicaName] = types.ModeRW
			e.ReplicaBdevNameMap[replicaName] = bdevName
			replicaBdevList = append(replicaBdevList, bdevName)
			updateRequired = true
		}
	}

	defer func() {
		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				updateRequired = true
			}
			e.ErrorMsg = err.Error()
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}
	if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList); err != nil {
		return err
	}

	return nil
}

func (e *Engine) ReplicaShallowCopy(dstReplicaName, dstReplicaAddress string) (err error) {
	e.RLock()

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		e.RUnlock()
		return fmt.Errorf("invalid state %v for engine %s replica %s shallow copy", e.State, e.Name, dstReplicaName)
	}
	if e.Frontend != types.FrontendEmpty {
		e.RUnlock()
		return fmt.Errorf("invalid frontend %v for engine %s replica %s shallow copy", e.Frontend, e.Name, dstReplicaName)
	}

	srcReplicaName, srcReplicaAddress := "", ""
	for replicaName, replicaMode := range e.ReplicaModeMap {
		if replicaMode != types.ModeRW {
			continue
		}
		srcReplicaName = replicaName
		srcReplicaAddress = e.ReplicaAddressMap[replicaName]
		break
	}
	e.RUnlock()

	if srcReplicaName == "" || srcReplicaAddress == "" {
		return fmt.Errorf("cannot find an RW replica for engine %s replica %s shallow copy", e.Name, dstReplicaName)
	}

	// TODO: Can we share the clients in the whole server?
	srcReplicaServiceCli, err := GetServiceClient(srcReplicaAddress)
	if err != nil {
		return err
	}
	dstReplicaServiceCli, err := GetServiceClient(dstReplicaAddress)
	if err != nil {
		return err
	}

	srcReplicaIP, _, _ := net.SplitHostPort(srcReplicaAddress)
	dstReplicaIP, _, _ := net.SplitHostPort(dstReplicaAddress)

	rpcSrcReplica, err := srcReplicaServiceCli.ReplicaGet(srcReplicaName)
	if err != nil {
		return err
	}

	ancestorSnapshotName, latestSnapshotName := "", ""
	for snapshotName, snapApiLvol := range rpcSrcReplica.Snapshots {
		if snapApiLvol.Parent == "" {
			ancestorSnapshotName = snapshotName
		}
		if snapApiLvol.Children[types.VolumeHead] {
			latestSnapshotName = snapshotName
		}
	}
	if ancestorSnapshotName == "" || latestSnapshotName == "" {
		return fmt.Errorf("cannot find the ancestor snapshot %s or latest snapshot %s from RW replica %s snapshot map during engine %s replica %s shallow copy", ancestorSnapshotName, latestSnapshotName, srcReplicaName, e.Name, dstReplicaName)
	}

	defer func() {
		// Blindly mark the rebuilding replica as mode ERR now.
		if err != nil {
			// Blindly send rebuilding finish for src replica.
			if srcReplicaErr := srcReplicaServiceCli.ReplicaRebuildingSrcFinish(srcReplicaName, dstReplicaName); srcReplicaErr != nil {
				e.log.WithError(srcReplicaErr).Errorf("Failed to finish rebuilding for src replica %s with address %s after rebuilding failure, will do nothing", srcReplicaName, srcReplicaAddress)
			}
			e.Lock()
			if e.ReplicaModeMap[dstReplicaName] != types.ModeERR {
				e.ReplicaModeMap[dstReplicaName] = types.ModeERR
				e.log.WithError(err).Errorf("Failed to rebuild replica %s with address %s from src replica %s with address %s, will mark the rebuilding replica mode as ERR", dstReplicaName, dstReplicaAddress, srcReplicaName, srcReplicaAddress)
			}
			e.Unlock()
		}
	}()

	dstRebuildingLvolAddress, err := dstReplicaServiceCli.ReplicaRebuildingDstStart(dstReplicaName, srcReplicaIP != dstReplicaIP)
	if err != nil {
		return err
	}
	if err = srcReplicaServiceCli.ReplicaRebuildingSrcStart(srcReplicaName, dstReplicaName, dstRebuildingLvolAddress); err != nil {
		return err
	}

	// Traverse the src replica snapshot tree with a DFS way and do shallow copy one by one
	rebuildingSnapshotList := []string{}
	rebuildingSnapshotList = getRebuildingSnapshotList(rpcSrcReplica, ancestorSnapshotName, rebuildingSnapshotList)
	currentSnapshotName, prevSnapshotName := "", ""
	for idx := 0; idx < len(rebuildingSnapshotList); idx++ {
		currentSnapshotName = rebuildingSnapshotList[idx]
		if prevSnapshotName != "" && rpcSrcReplica.Snapshots[currentSnapshotName].Parent != prevSnapshotName {
			if err = srcReplicaServiceCli.ReplicaRebuildingSrcDetach(srcReplicaName, dstReplicaName); err != nil {
				return err
			}
			if err = dstReplicaServiceCli.ReplicaRebuildingDstSnapshotRevert(dstReplicaName, rpcSrcReplica.Snapshots[currentSnapshotName].Parent); err != nil {
				return err
			}
			if err = srcReplicaServiceCli.ReplicaRebuildingSrcAttach(srcReplicaName, dstReplicaName, dstRebuildingLvolAddress); err != nil {
				return err
			}
		}
		if err = srcReplicaServiceCli.ReplicaSnapshotShallowCopy(srcReplicaName, currentSnapshotName); err != nil {
			return err
		}

		snapshotOptions := &api.SnapshotOptions{
			UserCreated: rpcSrcReplica.Snapshots[currentSnapshotName].UserCreated,
			Timestamp:   rpcSrcReplica.Snapshots[currentSnapshotName].SnapshotTimestamp,
		}

		if err = dstReplicaServiceCli.ReplicaRebuildingDstSnapshotCreate(dstReplicaName, currentSnapshotName, snapshotOptions); err != nil {
			return err
		}
		prevSnapshotName = currentSnapshotName
	}

	// TODO: The rebuilding lvol of the dst replica is actually the head. Need to make sure the head stands behind to the correct snapshot.
	//  Once we start to use a separate rebuilding lvol rather than the head, we can remove the below code.
	if !rpcSrcReplica.Snapshots[prevSnapshotName].Children[types.VolumeHead] {
		for snapshotName, snapApiLvol := range rpcSrcReplica.Snapshots {
			if !snapApiLvol.Children[types.VolumeHead] {
				continue
			}
			if err = srcReplicaServiceCli.ReplicaRebuildingSrcDetach(srcReplicaName, dstReplicaName); err != nil {
				return err
			}
			if err = dstReplicaServiceCli.ReplicaRebuildingDstSnapshotRevert(dstReplicaName, snapshotName); err != nil {
				return err
			}
			break
		}
	}

	if err = srcReplicaServiceCli.ReplicaRebuildingSrcFinish(srcReplicaName, dstReplicaName); err != nil {
		return err
	}
	// TODO: Connect the previously found latest snapshot lvol with the head lvol chain
	// TODO: For online rebuilding, the rebuilding lvol must be unexposed
	if err = dstReplicaServiceCli.ReplicaRebuildingDstFinish(dstReplicaName, e.IP == dstReplicaIP); err != nil {
		return err
	}

	return nil
}

func getRebuildingSnapshotList(rpcSrcReplica *api.Replica, currentSnapshotName string, rebuildingSnapshotList []string) []string {
	if currentSnapshotName == "" || currentSnapshotName == types.VolumeHead {
		return rebuildingSnapshotList
	}
	rebuildingSnapshotList = append(rebuildingSnapshotList, currentSnapshotName)
	for childSnapshotName := range rpcSrcReplica.Snapshots[currentSnapshotName].Children {
		rebuildingSnapshotList = getRebuildingSnapshotList(rpcSrcReplica, childSnapshotName, rebuildingSnapshotList)
	}
	return rebuildingSnapshotList
}

func (e *Engine) ReplicaDelete(spdkClient *spdkclient.Client, replicaName, replicaAddress string) (err error) {
	e.log.Infof("Deleting replica %s with address %s from engine", replicaName, replicaAddress)

	e.Lock()
	defer e.Unlock()

	if replicaName == "" {
		for rName, rAddr := range e.ReplicaAddressMap {
			if rAddr == replicaAddress {
				replicaName = rName
				break
			}
		}
	}
	if replicaName == "" {
		return fmt.Errorf("cannot find replica name with address %s for engine %s replica delete", replicaAddress, e.Name)
	}
	if e.ReplicaAddressMap[replicaName] == "" {
		return fmt.Errorf("cannot find replica %s for engine %s replica delete", replicaName, e.Name)
	}
	if replicaAddress != "" && e.ReplicaAddressMap[replicaName] != replicaAddress {
		return fmt.Errorf("replica %s recorded address %s does not match the input address %s for engine %s replica delete", replicaName, e.ReplicaAddressMap[replicaName], replicaAddress, e.Name)
	}

	e.log.Infof("Removing base bdev %v from engine", e.ReplicaBdevNameMap[replicaName])
	if _, err := spdkClient.BdevRaidRemoveBaseBdev(e.ReplicaBdevNameMap[replicaName]); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to remove base bdev %s for deleting replica %s", e.ReplicaBdevNameMap[replicaName], replicaName)
	}

	// Detaching the corresponding NVMf controller to remote replica
	controllerName := helperutil.GetNvmeControllerNameFromNamespaceName(replicaName)
	e.log.Infof("Detaching the corresponding NVMf controller %v during remote replica %s delete", controllerName, replicaName)
	if _, err := spdkClient.BdevNvmeDetachController(controllerName); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to detach controller %s for deleting replica %s", controllerName, replicaName)
	}

	delete(e.ReplicaAddressMap, replicaName)
	delete(e.ReplicaModeMap, replicaName)
	delete(e.ReplicaBdevNameMap, replicaName)
	e.log = e.log.WithField("replicaAddressMap", e.ReplicaAddressMap)

	return nil
}

type SnapshotOperationType string

const (
	SnapshotOperationCreate = SnapshotOperationType("snapshot-create")
	SnapshotOperationDelete = SnapshotOperationType("snapshot-delete")
	SnapshotOperationRevert = SnapshotOperationType("snapshot-revert")
)

func (e *Engine) SnapshotCreate(spdkClient *spdkclient.Client, inputSnapshotName string) (snapshotName string, err error) {
	e.log.Infof("Creating snapshot %s", inputSnapshotName)

	opts := &api.SnapshotOptions{
		UserCreated: true,
		Timestamp:   util.Now(),
	}

	return e.snapshotOperation(spdkClient, inputSnapshotName, SnapshotOperationCreate, opts)
}

func (e *Engine) SnapshotDelete(spdkClient *spdkclient.Client, snapshotName string) (err error) {
	e.log.Infof("Deleting snapshot %s", snapshotName)

	_, err = e.snapshotOperation(spdkClient, snapshotName, SnapshotOperationDelete, nil)
	return err
}

func (e *Engine) SnapshotRevert(spdkClient *spdkclient.Client, snapshotName string) (err error) {
	e.log.Infof("Reverting snapshot %s", snapshotName)

	_, err = e.snapshotOperation(spdkClient, snapshotName, SnapshotOperationRevert, nil)
	return err
}

func (e *Engine) snapshotOperation(spdkClient *spdkclient.Client, inputSnapshotName string, snapshotOp SnapshotOperationType, opts *api.SnapshotOptions) (snapshotName string, err error) {
	updateRequired := false

	if snapshotOp == SnapshotOperationCreate {
		e.RLock()
		devicePath := ""
		if e.State == types.InstanceStateRunning && e.Frontend == types.FrontendSPDKTCPBlockdev {
			devicePath = e.Endpoint
		}
		e.RUnlock()
		if devicePath != "" {
			ne, err := helperutil.NewExecutor(commonTypes.HostProcDirectory)
			if err != nil {
				e.log.WithError(err).Errorf("WARNING: failed to get the executor for snapshot op %v with snapshot %s, will skip the sync and continue", snapshotOp, inputSnapshotName)
			} else {
				e.log.Infof("Requesting system sync %v before snapshot", devicePath)
				if _, err := ne.Execute(nil, "sync", []string{devicePath}, SyncTimeout); err != nil {
					// sync should never fail though, so it more like due to the nsenter
					e.log.WithError(err).Errorf("WARNING: failed to sync for snapshot op %v with snapshot %s, will skip the sync and continue", snapshotOp, inputSnapshotName)
				}
			}
		}
	}

	e.Lock()
	defer func() {
		e.Unlock()

		if updateRequired {
			e.UpdateCh <- nil
		}
	}()

	// Syncing with the SPDK TGT server only when the engine is running.
	if e.State != types.InstanceStateRunning {
		return "", fmt.Errorf("invalid state %v for engine %s snapshot %s operation", e.State, e.Name, inputSnapshotName)
	}

	replicaClients, err := e.getReplicaClients()
	if err != nil {
		return "", err
	}

	if snapshotName, err = e.snapshotOperationPreCheckWithoutLock(replicaClients, inputSnapshotName, snapshotOp); err != nil {
		return "", err
	}

	defer func() {
		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				updateRequired = true
			}
			e.ErrorMsg = err.Error()
		} else {
			if e.State != types.InstanceStateError {
				e.ErrorMsg = ""
			}
		}
	}()

	if updateRequired, err = e.snapshotOperationWithoutLock(spdkClient, replicaClients, snapshotName, snapshotOp, opts); err != nil {
		return "", err
	}

	e.CheckAndUpdateInfoFromReplica()

	e.log.Infof("Engine finished snapshot %s for %v", snapshotOp, snapshotName)

	return snapshotName, nil
}

func (e *Engine) getReplicaClients() (replicaClients map[string]*client.SPDKClient, err error) {
	replicaClients = map[string]*client.SPDKClient{}
	for replicaName := range e.ReplicaAddressMap {
		if e.ReplicaModeMap[replicaName] == types.ModeERR {
			continue
		}
		c, err := GetServiceClient(e.ReplicaAddressMap[replicaName])
		if err != nil {
			return nil, err
		}
		replicaClients[replicaName] = c
	}

	return replicaClients, nil
}

func (e *Engine) snapshotOperationPreCheckWithoutLock(replicaClients map[string]*client.SPDKClient, snapshotName string, snapshotOp SnapshotOperationType) (string, error) {
	for replicaName := range replicaClients {
		switch snapshotOp {
		case SnapshotOperationCreate:
			if snapshotName == "" {
				snapshotName = util.UUID()[:8]
			}
		case SnapshotOperationDelete:
			if snapshotName == "" {
				return "", fmt.Errorf("empty snapshot name for engine %s snapshot deletion", e.Name)
			}
			if e.ReplicaModeMap[replicaName] == types.ModeWO {
				return "", fmt.Errorf("engine %s contains WO replica %s during snapshot %s delete", e.Name, replicaName, snapshotName)
			}
			e.CheckAndUpdateInfoFromReplica()
			if len(e.SnapshotMap[snapshotName].Children) > 1 {
				return "", fmt.Errorf("engine %s cannot delete snapshot %s since it contains multiple children %+v", e.Name, snapshotName, e.SnapshotMap[snapshotName].Children)
			}
			// TODO: SPDK allows deleting the parent of the volume head. To make the behavior consistent between v1 and v2 engines, we manually disable if for now.
			for childName := range e.SnapshotMap[snapshotName].Children {
				if childName == types.VolumeHead {
					return "", fmt.Errorf("engine %s cannot delete snapshot %s since it is the parent of volume head", e.Name, snapshotName)
				}
			}
		case SnapshotOperationRevert:
			if snapshotName == "" {
				return "", fmt.Errorf("empty snapshot name for engine %s snapshot deletion", e.Name)
			}
			if e.Frontend != types.FrontendEmpty {
				return "", fmt.Errorf("invalid frontend %v for engine %s snapshot %s revert", e.Frontend, e.Name, snapshotName)
			}
			if e.ReplicaModeMap[replicaName] == types.ModeWO {
				return "", fmt.Errorf("engine %s contains WO replica %s during snapshot %s revert", e.Name, replicaName, snapshotName)
			}
			r, err := replicaClients[replicaName].ReplicaGet(replicaName)
			if err != nil {
				return "", err
			}
			if r.Snapshots[snapshotName] == nil {
				return "", fmt.Errorf("replica %s does not contain the reverting snapshot %s", replicaName, snapshotName)
			}
		default:
			return "", fmt.Errorf("unknown replica snapshot operation %s", snapshotOp)
		}
	}

	return snapshotName, nil
}

func (e *Engine) snapshotOperationWithoutLock(spdkClient *spdkclient.Client, replicaClients map[string]*client.SPDKClient, snapshotName string, snapshotOp SnapshotOperationType, opts *api.SnapshotOptions) (updated bool, err error) {
	if snapshotOp == SnapshotOperationRevert {
		if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
			e.log.WithError(err).Errorf("Failed to delete RAID after snapshot %s revert", snapshotName)
			return false, err
		}
	}

	for replicaName := range replicaClients {
		if err := e.replicaSnapshotOperation(spdkClient, replicaClients[replicaName], replicaName, snapshotName, snapshotOp, opts); err != nil && e.ReplicaModeMap[replicaName] != types.ModeERR {
			e.ReplicaModeMap[replicaName] = types.ModeERR
			e.ReplicaBdevNameMap[replicaName] = ""
			e.log.WithError(err).Errorf("Failed to issue operation %s for replica %s snapshot %s, will mark the replica as mode ERR", snapshotOp, replicaName, snapshotName)
			updated = true
		}
	}

	if snapshotOp == SnapshotOperationRevert {
		replicaBdevList := []string{}
		for replicaName, bdevName := range e.ReplicaBdevNameMap {
			if e.ReplicaModeMap[replicaName] != types.ModeRW {
				continue
			}
			if e.ReplicaBdevNameMap[replicaName] == "" {
				continue
			}
			replicaBdevList = append(replicaBdevList, bdevName)
		}
		if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList); err != nil {
			e.log.WithError(err).Errorf("Failed to re-create RAID after snapshot %s revert", snapshotName)
			return false, err
		}
	}

	return updated, nil
}

func (e *Engine) replicaSnapshotOperation(spdkClient *spdkclient.Client, replicaClient *client.SPDKClient, replicaName, snapshotName string, snapshotOp SnapshotOperationType, opts *api.SnapshotOptions) error {
	switch snapshotOp {
	case SnapshotOperationCreate:
		// TODO: execute `sync` for the nvme initiator before snapshot start
		return replicaClient.ReplicaSnapshotCreate(replicaName, snapshotName, opts)
	case SnapshotOperationDelete:
		return replicaClient.ReplicaSnapshotDelete(replicaName, snapshotName)
	case SnapshotOperationRevert:
		if err := e.disconnectReplica(spdkClient, replicaName); err != nil {
			return err
		}
		if err := replicaClient.ReplicaSnapshotRevert(replicaName, snapshotName); err != nil {
			return err
		}
		bdevName, err := e.connectReplica(spdkClient, replicaName, e.ReplicaAddressMap[replicaName])
		if err != nil {
			return err
		}
		if bdevName != "" {
			e.ReplicaBdevNameMap[replicaName] = bdevName
		}
	default:
		return fmt.Errorf("unknown replica snapshot operation %s", snapshotOp)
	}

	return nil
}

func (e *Engine) ReplicaList(spdkClient *spdkclient.Client) (ret map[string]*api.Replica, err error) {
	e.Lock()
	defer e.Unlock()

	replicas := map[string]*api.Replica{}

	for name, address := range e.ReplicaAddressMap {
		replicaServiceCli, err := GetServiceClient(address)
		if err != nil {
			e.log.WithError(err).Errorf("Failed to get service client for replica %s with address %s", name, address)
			continue
		}

		replica, err := replicaServiceCli.ReplicaGet(name)
		if err != nil {
			e.log.WithError(err).Errorf("Failed to get replica %s with address %s", name, address)
			continue
		}

		replicas[name] = replica
	}

	return replicas, nil
}

func (e *Engine) SetErrorState() {
	needUpdate := false

	e.Lock()
	defer func() {
		e.Unlock()

		if needUpdate {
			e.UpdateCh <- nil
		}
	}()

	if e.State != types.InstanceStateStopped && e.State != types.InstanceStateError {
		e.State = types.InstanceStateError
		needUpdate = true
	}
}

func (e *Engine) BackupCreate(backupName, volumeName, engineName, snapshotName, backingImageName, backingImageChecksum string,
	labels []string, backupTarget string, credential map[string]string, concurrentLimit int32, compressionMethod, storageClassName string, size uint64) (*BackupCreateInfo, error) {
	e.log.Infof("Creating backup %s", backupName)

	e.Lock()
	defer e.Unlock()

	replicaName, replicaAddress := "", ""
	for name, mode := range e.ReplicaModeMap {
		if mode != types.ModeRW {
			continue
		}
		replicaName = name
		replicaAddress = e.ReplicaAddressMap[name]
		break
	}

	e.log.Infof("Creating backup %s for volume %s on replica %s address %s", backupName, volumeName, replicaName, replicaAddress)

	replicaServiceCli, err := GetServiceClient(replicaAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, err.Error())
	}

	recv, err := replicaServiceCli.ReplicaBackupCreate(&client.BackupCreateRequest{
		BackupName:           backupName,
		SnapshotName:         snapshotName,
		VolumeName:           volumeName,
		ReplicaName:          replicaName,
		Size:                 size,
		BackupTarget:         backupTarget,
		StorageClassName:     storageClassName,
		BackingImageName:     backingImageName,
		BackingImageChecksum: backingImageChecksum,
		CompressionMethod:    compressionMethod,
		ConcurrentLimit:      concurrentLimit,
		Labels:               labels,
		Credential:           credential,
	})
	if err != nil {
		return nil, err
	}
	return &BackupCreateInfo{
		BackupName:     recv.Backup,
		IsIncremental:  recv.IsIncremental,
		ReplicaAddress: replicaAddress,
	}, nil
}

func (e *Engine) BackupStatus(backupName, replicaAddress string) (*spdkrpc.BackupStatusResponse, error) {
	e.Lock()
	defer e.Unlock()

	found := false
	for name, mode := range e.ReplicaModeMap {
		if e.ReplicaAddressMap[name] == replicaAddress {
			if mode != types.ModeRW {
				return nil, grpcstatus.Errorf(grpccodes.Internal, "replica %s is not in RW mode", name)
			}
			found = true
			break
		}
	}

	if !found {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, fmt.Sprintf("replica address %s is not found in engine %s for getting backup %v status", replicaAddress, e.Name, backupName))
	}

	replicaServiceCli, err := GetServiceClient(replicaAddress)
	if err != nil {
		return nil, grpcstatus.Errorf(grpccodes.Internal, err.Error())
	}

	return replicaServiceCli.ReplicaBackupStatus(backupName)
}

func (e *Engine) BackupRestore(spdkClient *spdkclient.Client, backupUrl, engineName, snapshotName string, credential map[string]string, concurrentLimit int32) (*spdkrpc.EngineBackupRestoreResponse, error) {
	e.log.Infof("Restoring backup %s", backupUrl)

	e.Lock()
	defer e.Unlock()

	e.log.Infof("Deleting raid bdev %s before restoration", e.Name)
	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return nil, errors.Wrapf(err, "failed to delete raid bdev %s before restoration", e.Name)
	}

	e.log.Info("Disconnecting all replicas before restoration")
	for replicaName := range e.ReplicaAddressMap {
		if err := e.disconnectReplica(spdkClient, replicaName); err != nil {
			e.log.Infof("Failed to remove replica %s before restoration", replicaName)
			return nil, errors.Wrapf(err, "failed to remove replica %s before restoration", replicaName)
		}
	}

	e.IsRestoring = true

	// TODO: support DR volume
	if len(e.SnapshotMap) == 0 {
		if snapshotName == "" {
			snapshotName = util.UUID()
			e.log.Infof("Generating a snapshot name %s for the full restore", snapshotName)
		}
	} else {
		return nil, errors.Errorf("incremental restore is not supported yet")
	}

	resp := &spdkrpc.EngineBackupRestoreResponse{
		Errors: map[string]string{},
	}
	for replicaName, replicaAddress := range e.ReplicaAddressMap {
		e.log.Infof("Restoring backup on replica %s address %s", replicaName, replicaAddress)

		replicaServiceCli, err := GetServiceClient(replicaAddress)
		if err != nil {
			e.log.WithError(err).Errorf("Failed to restore backup on replica %s address %s", replicaName, replicaAddress)
			resp.Errors[replicaAddress] = err.Error()
			continue
		}

		err = replicaServiceCli.ReplicaBackupRestore(&client.BackupRestoreRequest{
			BackupUrl:       backupUrl,
			ReplicaName:     replicaName,
			SnapshotName:    snapshotName,
			Credential:      credential,
			ConcurrentLimit: concurrentLimit,
		})
		if err != nil {
			e.log.WithError(err).Errorf("Failed to restore backup on replica %s address %s", replicaName, replicaAddress)
			resp.Errors[replicaAddress] = err.Error()
		}
	}

	return resp, nil
}

func (e *Engine) BackupRestoreFinish(spdkClient *spdkclient.Client) error {
	e.Lock()
	defer e.Unlock()

	replicaBdevList := []string{}
	for replicaName, bdevName := range e.ReplicaBdevNameMap {
		replicaAddress := e.ReplicaAddressMap[replicaName]
		replicaIP, replicaPort, err := net.SplitHostPort(replicaAddress)
		if err != nil {
			return err
		}
		e.log.Infof("Attaching replica %s with address %s before finishing restoration", replicaName, replicaAddress)
		_, err = spdkClient.BdevNvmeAttachController(replicaName, helpertypes.GetNQN(replicaName), replicaIP, replicaPort, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4,
			helpertypes.DefaultCtrlrLossTimeoutSec, helpertypes.DefaultReconnectDelaySec, helpertypes.DefaultFastIOFailTimeoutSec, helpertypes.DefaultMultipath)
		if err != nil {
			return err
		}
		replicaBdevList = append(replicaBdevList, bdevName)
	}

	e.log.Infof("Creating raid bdev %s with replicas %+v before finishing restoration", e.Name, replicaBdevList)
	if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList); err != nil {
		if !jsonrpc.IsJSONRPCRespErrorFileExists(err) {
			e.log.WithError(err).Errorf("Failed to create raid bdev before finishing restoration")
			return err
		}
	}

	e.IsRestoring = false

	return nil
}

func (e *Engine) RestoreStatus() (*spdkrpc.RestoreStatusResponse, error) {
	resp := &spdkrpc.RestoreStatusResponse{
		Status: map[string]*spdkrpc.ReplicaRestoreStatusResponse{},
	}

	e.Lock()
	defer e.Unlock()

	for replicaName, replicaAddress := range e.ReplicaAddressMap {
		if e.ReplicaModeMap[replicaName] != types.ModeRW {
			continue
		}

		replicaServiceCli, err := GetServiceClient(replicaAddress)
		if err != nil {
			return nil, err
		}
		status, err := replicaServiceCli.ReplicaRestoreStatus(replicaName)
		if err != nil {
			return nil, err
		}
		resp.Status[replicaAddress] = status
	}

	return resp, nil
}

// Suspend suspends the engine. IO operations will be suspended.
func (e *Engine) Suspend(spdkClient *spdkclient.Client) (err error) {
	e.Lock()
	defer func() {
		e.Unlock()

		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				e.log.WithError(err).Info("Failed to suspend engine, will mark the engine as error")
			}
			e.ErrorMsg = err.Error()
		} else {
			e.State = types.InstanceStateSuspended
			e.ErrorMsg = ""

			e.log.Infof("Suspended engine")
		}

		e.UpdateCh <- nil
	}()

	e.log.Info("Creating initiator for suspending engine")
	initiator, err := nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for suspending engine %s", e.Name)
	}

	e.log.Info("Suspending engine")
	return initiator.Suspend(false, false)
}

// Resume resumes the engine. IO operations will be resumed.
func (e *Engine) Resume(spdkClient *spdkclient.Client) (err error) {
	e.Lock()
	defer func() {
		e.Unlock()

		if err != nil {
			if e.State != types.InstanceStateError {
				e.State = types.InstanceStateError
				e.log.WithError(err).Info("Failed to resume engine, will mark the engine as error")
			}
			e.ErrorMsg = err.Error()
		} else {
			e.State = types.InstanceStateRunning
			e.ErrorMsg = ""

			e.log.Infof("Resumed engine")
		}

		e.UpdateCh <- nil
	}()

	if e.State == types.InstanceStateRunning {
		return nil
	}

	e.log.Info("Creating initiator for resuming engine")
	initiator, err := nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for resuming engine %s", e.Name)
	}

	e.log.Info("Resuming engine")
	return initiator.Resume()
}

// SwitchOverTarget function in the Engine struct is responsible for switching the engine's target to a new address.
func (e *Engine) SwitchOverTarget(spdkClient *spdkclient.Client, targetAddress string) (err error) {
	e.log.Infof("Switching over engine to target address %s", targetAddress)

	currentTargetAddress := ""

	e.Lock()
	defer func() {
		e.Unlock()

		if err != nil {
			e.log.WithError(err).Warnf("Failed to switch over engine to target address %s", targetAddress)

			if disconnected, errCheck := e.IsTargetDisconnected(); errCheck != nil {
				e.log.WithError(errCheck).Warnf("Failed to check if target %s is disconnected", targetAddress)
			} else if disconnected {
				if errConnect := e.connectTarget(currentTargetAddress); errConnect != nil {
					e.log.WithError(errConnect).Warnf("Failed to connect target back to %s", currentTargetAddress)
				} else {
					e.log.Infof("Connected target back to %s", currentTargetAddress)

					if errReload := e.reloadDevice(); errReload != nil {
						e.log.WithError(errReload).Warnf("Failed to reload device mapper")
					} else {
						e.log.Infof("Reloaded device mapper for connecting target back to %s", currentTargetAddress)
					}
				}
			}
		} else {
			e.ErrorMsg = ""

			e.log.Infof("Switched over target to %s", targetAddress)
		}

		e.UpdateCh <- nil
	}()

	initiator, err := nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %s target switchover", e.Name)
	}

	suspended, err := initiator.IsSuspended()
	if err != nil {
		return errors.Wrapf(err, "failed to check if engine %s is suspended", e.Name)
	}
	if !suspended {
		return fmt.Errorf("engine %s must be suspended before target switchover", e.Name)
	}

	if err := initiator.LoadNVMeDeviceInfo(initiator.TransportAddress, initiator.TransportServiceID, initiator.SubsystemNQN); err != nil {
		if !nvme.IsValidNvmeDeviceNotFound(err) {
			return errors.Wrapf(err, "failed to load NVMe device info for engine %s target switchover", e.Name)
		}
	}

	currentTargetAddress = net.JoinHostPort(initiator.TransportAddress, initiator.TransportServiceID)
	if e.isSwitchOverTargetRequired(currentTargetAddress, targetAddress) {
		if currentTargetAddress != "" {
			if err := e.disconnectTarget(currentTargetAddress); err != nil {
				return err
			}
		}

		if err := e.connectTarget(targetAddress); err != nil {
			return err
		}
	}

	// Replace IP and Port with the new target address.
	// No need to update TargetIP and TargetPort, because target is not delete yet.
	targetIP, targetPort, err := splitHostPort(targetAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to split target address %s", targetAddress)
	}

	e.IP = targetIP
	e.Port = targetPort

	e.log.Info("Reloading device mapper after target switchover")
	if err := e.reloadDevice(); err != nil {
		return err
	}

	return nil
}

func (e *Engine) IsTargetDisconnected() (bool, error) {
	initiator, err := nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return false, errors.Wrapf(err, "failed to create initiator for checking engine %s target disconnected", e.Name)
	}

	suspended, err := initiator.IsSuspended()
	if err != nil {
		return false, errors.Wrapf(err, "failed to check if engine %s is suspended", e.Name)
	}
	if !suspended {
		return false, fmt.Errorf("engine %s must be suspended before checking target disconnected", e.Name)
	}

	if err := initiator.LoadNVMeDeviceInfo(initiator.TransportAddress, initiator.TransportServiceID, initiator.SubsystemNQN); err != nil {
		if !nvme.IsValidNvmeDeviceNotFound(err) {
			return false, errors.Wrapf(err, "failed to load NVMe device info for checking engine %s target disconnected", e.Name)
		}
	}

	return initiator.TransportAddress == "" && initiator.TransportServiceID == "", nil
}

func (e *Engine) reloadDevice() error {
	initiator, err := nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to recreate initiator after engine %s target switchover", e.Name)
	}

	if err := initiator.LoadNVMeDeviceInfo(initiator.TransportAddress, initiator.TransportServiceID, initiator.SubsystemNQN); err != nil {
		return errors.Wrapf(err, "failed to load NVMe device info after engine %s target switchover", e.Name)
	}

	if err := initiator.ReloadDmDevice(); err != nil {
		return errors.Wrapf(err, "failed to reload device mapper after engine %s target switchover", e.Name)
	}

	return nil
}

func (e *Engine) disconnectTarget(targetAddress string) error {
	initiator, err := nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %s disconnect target %v", e.Name, targetAddress)
	}

	if err := initiator.LoadNVMeDeviceInfo(initiator.TransportAddress, initiator.TransportServiceID, initiator.SubsystemNQN); err != nil {
		if !nvme.IsValidNvmeDeviceNotFound(err) {
			return errors.Wrapf(err, "failed to load NVMe device info for engine %s disconnect target %v", e.Name, targetAddress)
		}
	}

	e.log.Infof("Disconnecting from old target %s before target switchover", targetAddress)
	if err := initiator.DisconnectTarget(); err != nil {
		return errors.Wrapf(err, "failed to disconnect from old target %s for engine %s", targetAddress, e.Name)
	}

	// Make sure the old target is disconnected before connecting to the new targets
	if err := initiator.WaitForDisconnect(maxNumRetries, retryInterval); err != nil {
		return errors.Wrapf(err, "failed to wait for disconnect from old target %s for engine %s", targetAddress, e.Name)
	}

	e.log.Infof("Disconnected from old target %s before target switchover", targetAddress)

	return nil
}

func (e *Engine) connectTarget(targetAddress string) error {
	if targetAddress == "" {
		return fmt.Errorf("failed to connect target for engine %s: missing required parameter target address", e.Name)
	}

	targetIP, targetPort, err := splitHostPort(targetAddress)
	if err != nil {
		return errors.Wrapf(err, "failed to split target address %s", targetAddress)
	}

	initiator, err := nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %s connect target %v:%v", e.Name, targetIP, targetPort)
	}

	if err := initiator.LoadNVMeDeviceInfo(initiator.TransportAddress, initiator.TransportServiceID, initiator.SubsystemNQN); err != nil {
		if !nvme.IsValidNvmeDeviceNotFound(err) {
			return errors.Wrapf(err, "failed to load NVMe device info for engine %s connect target %v:%v", e.Name, targetIP, targetPort)
		}
	}

	for r := 0; r < maxNumRetries; r++ {
		e.log.Infof("Discovering target %v:%v before target switchover", targetIP, targetPort)
		subsystemNQN, err := initiator.DiscoverTarget(targetIP, strconv.Itoa(int(targetPort)))
		if err != nil {
			e.log.WithError(err).Warnf("Failed to discover target %v:%v for target switchover", targetIP, targetPort)
			time.Sleep(retryInterval)
			continue
		}
		initiator.SubsystemNQN = subsystemNQN

		e.log.Infof("Connecting to target %v:%v before target switchover", targetIP, targetPort)
		controllerName, err := initiator.ConnectTarget(targetIP, strconv.Itoa(int(targetPort)), e.Nqn)
		if err != nil {
			e.log.WithError(err).Warnf("Failed to connect to target %v:%v for target switchover", targetIP, targetPort)
			time.Sleep(retryInterval)
			continue
		}
		initiator.ControllerName = controllerName
		break
	}

	if initiator.SubsystemNQN == "" || initiator.ControllerName == "" {
		return fmt.Errorf("failed to connect to target %v:%v for engine %v target switchover", targetIP, targetPort, e.Name)
	}

	// Target is switched over, to avoid the error "failed to wait for connect to target",
	// create a new initiator and wait for connect
	initiator, err = nvme.NewInitiator(e.VolumeName, e.Nqn, nvme.HostProc)
	if err != nil {
		return errors.Wrapf(err, "failed to create initiator for engine %s wait for connect target %v:%v", e.Name, targetIP, targetPort)
	}

	if err := initiator.WaitForConnect(maxNumRetries, retryInterval); err == nil {
		return errors.Wrapf(err, "failed to wait for connect to target %v:%v for engine %v target switchover", targetIP, targetPort, e.Name)
	}

	return nil
}

// DeleteTarget deletes the target
func (e *Engine) DeleteTarget(spdkClient *spdkclient.Client, superiorPortAllocator *util.Bitmap) (err error) {
	e.log.Infof("Deleting target")

	if err := spdkClient.StopExposeBdev(e.Nqn); err != nil {
		return errors.Wrapf(err, "failed to stop expose bdev after engine %s target switchover", e.Name)
	}

	if e.TargetPort != 0 {
		if err := superiorPortAllocator.ReleaseRange(e.TargetPort, e.TargetPort); err != nil {
			return err
		}
		e.TargetPort = 0
	}

	e.log.Infof("Deleting raid bdev %s before target switchover", e.Name)
	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return errors.Wrapf(err, "failed to delete raid bdev after engine %s target switchover", e.Name)
	}

	for replicaName := range e.ReplicaAddressMap {
		e.log.Infof("Disconnecting replica %s after target switchover", replicaName)
		if err := e.disconnectReplica(spdkClient, replicaName); err != nil {
			e.log.WithError(err).Warnf("Failed to disconnect replica %s after target switchover", replicaName)
			if e.ReplicaModeMap[replicaName] != types.ModeERR {
				e.ReplicaModeMap[replicaName] = types.ModeERR
			}
		}
	}
	return nil
}

func (e *Engine) isSwitchOverTargetRequired(oldTargetAddress, newTargetAddress string) bool {
	return oldTargetAddress != newTargetAddress
}

func engineOnlyContainsInitiator(e *Engine) bool {
	return e.Port != 0 && e.TargetPort == 0
}

func engineOnlyContainsTarget(e *Engine) bool {
	return e.Port == 0 && e.TargetPort != 0
}
