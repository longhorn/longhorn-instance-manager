package spdk

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/jsonrpc"
	"github.com/longhorn/go-spdk-helper/pkg/nvme"
	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
	helpertypes "github.com/longhorn/go-spdk-helper/pkg/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
	"github.com/longhorn/longhorn-spdk-engine/pkg/util"
	"github.com/longhorn/longhorn-spdk-engine/proto/spdkrpc"
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
	Frontend           string
	Endpoint           string

	State types.InstanceState

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

		UpdateCh: engineUpdateCh,

		log: log,
	}
}

func (e *Engine) Create(spdkClient *spdkclient.Client, replicaAddressMap, localReplicaLvsNameMap map[string]string, portCount int32, superiorPortAllocator *util.Bitmap) (ret *spdkrpc.Engine, err error) {
	requireUpdate := true

	e.Lock()
	defer func() {
		e.Unlock()

		if requireUpdate {
			e.UpdateCh <- nil
		}
	}()

	if e.State != types.InstanceStatePending {
		requireUpdate = false
		return nil, fmt.Errorf("invalid state %s for engine %s creation", e.State, e.Name)
	}

	defer func() {
		if err != nil && e.State != types.InstanceStateError {
			e.State = types.InstanceStateError
		}
	}()

	podIP, err := util.GetIPForPod()
	if err != nil {
		return nil, err
	}
	e.IP = podIP
	e.log = e.log.WithField("ip", podIP)

	replicaBdevList := []string{}
	for replicaName, replicaAddr := range replicaAddressMap {
		bdevName, err := getBdevNameForReplica(spdkClient, localReplicaLvsNameMap, replicaName, replicaAddr, podIP)
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

	e.log.Infof("Launching RAID during engine creation")
	if _, err := spdkClient.BdevRaidCreate(e.Name, spdktypes.BdevRaidLevel1, 0, replicaBdevList); err != nil {
		return nil, err
	}

	e.log.Infof("Launching Frontend during engine creation")
	if err := e.handleFrontend(spdkClient, portCount, superiorPortAllocator); err != nil {
		return nil, err
	}

	e.State = types.InstanceStateRunning

	e.log.Infof("Created engine")

	return e.getWithoutLock(), nil
}

func getBdevNameForReplica(spdkClient *spdkclient.Client, localReplicaLvsNameMap map[string]string, replicaName, replicaAddress, podIP string) (bdevName string, err error) {
	replicaIP, replicaPort, err := net.SplitHostPort(replicaAddress)
	if err != nil {
		return "", err
	}
	if replicaIP == podIP {
		if localReplicaLvsNameMap[replicaName] == "" {
			return "", fmt.Errorf("cannot find local replica %s from the local replica map", replicaName)

		}
		return spdktypes.GetLvolAlias(localReplicaLvsNameMap[replicaName], replicaName), nil
	}

	nvmeBdevNameList, err := spdkClient.BdevNvmeAttachController(replicaName, helpertypes.GetNQN(replicaName), replicaIP, replicaPort, spdktypes.NvmeTransportTypeTCP, spdktypes.NvmeAddressFamilyIPv4,
		helpertypes.DefaultCtrlrLossTimeoutSec, helpertypes.DefaultReconnectDelaySec, helpertypes.DefaultFastIOFailTimeoutSec)
	if err != nil {
		return "", err
	}
	if len(nvmeBdevNameList) != 1 {
		return "", fmt.Errorf("got zero or multiple results when attaching replica %s with address %s as a NVMe bdev: %+v", replicaName, replicaAddress, nvmeBdevNameList)
	}
	return nvmeBdevNameList[0], nil
}

func (e *Engine) handleFrontend(spdkClient *spdkclient.Client, portCount int32, superiorPortAllocator *util.Bitmap) error {
	if e.Frontend != types.FrontendEmpty && e.Frontend != types.FrontendSPDKTCPNvmf && e.Frontend != types.FrontendSPDKTCPBlockdev {
		return fmt.Errorf("unknown frontend type %s", e.Frontend)
	}

	if e.Frontend == types.FrontendEmpty {
		e.log.Infof("No frontend specified, will not expose the volume %s", e.VolumeName)
		return nil
	}

	nqn := helpertypes.GetNQN(e.Name)
	port, _, err := superiorPortAllocator.AllocateRange(portCount)
	if err != nil {
		return err
	}
	if err := spdkClient.StartExposeBdev(nqn, e.Name, e.IP, strconv.Itoa(int(port))); err != nil {
		return err
	}
	e.Port = port
	e.log = e.log.WithField("port", port)

	if e.Frontend == types.FrontendSPDKTCPNvmf {
		e.Endpoint = GetNvmfEndpoint(nqn, e.IP, e.Port)
		return nil
	}

	initiator, err := nvme.NewInitiator(e.VolumeName, nqn, nvme.HostProc)
	if err != nil {
		return err
	}
	if err = initiator.Start(e.IP, strconv.Itoa(int(port))); err != nil {
		return err
	}
	e.Endpoint = initiator.GetEndpoint()
	e.log = e.log.WithField("endpoint", e.Endpoint)

	return nil
}

func (e *Engine) Delete(spdkClient *spdkclient.Client, superiorPortAllocator *util.Bitmap) (err error) {
	requireUpdate := false

	e.Lock()
	defer func() {
		if err != nil && e.State != types.InstanceStateError {
			e.State = types.InstanceStateError
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
		if err := initiator.Stop(); err != nil {
			return err
		}

		if err := spdkClient.StopExposeBdev(nqn); err != nil {
			return err
		}

		e.Endpoint = ""
		requireUpdate = true
	}

	if e.Port != 0 {
		if err := superiorPortAllocator.ReleaseRange(e.Port, e.Port); err != nil {
			return err
		}
		e.Port = 0
		requireUpdate = true
	}

	if _, err := spdkClient.BdevRaidDelete(e.Name); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	for replicaName := range e.ReplicaAddressMap {
		if err := e.removeReplica(spdkClient, replicaName); err != nil {
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

	e.log.Infof("Deleted engine")

	return nil
}

func (e *Engine) removeReplica(spdkClient *spdkclient.Client, replicaName string) (err error) {
	replicaIP, _, err := net.SplitHostPort(e.ReplicaAddressMap[replicaName])
	if err != nil {
		return err
	}
	if replicaIP == e.IP {
		return nil
	}

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
		Frontend:          e.Frontend,
		Endpoint:          e.Endpoint,
		State:             string(e.State),
	}

	for replicaName, replicaMode := range e.ReplicaModeMap {
		res.ReplicaModeMap[replicaName] = spdkrpc.ReplicaModeToGRPCReplicaMode(replicaMode)
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
		if err != nil && e.State != types.InstanceStateError {
			e.State = types.InstanceStateError
			e.log.WithError(err).Error("Found error during engine validation and update")
			updateRequired = true
		}
	}()

	podIP, err := util.GetIPForPod()
	if err != nil {
		return err
	}
	if e.IP != podIP {
		return fmt.Errorf("found mismatching between engine IP %s and pod IP %s for engine %s", e.IP, podIP, e.Name)
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
				e.log.WithError(err).Errorf("Replica %s is invalid, will update the mode from %s to %s", replicaName, e.ReplicaModeMap[replicaName], types.ModeERR)
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

	return nil
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
		if err := initiator.LoadNVMeDeviceInfo(); err != nil {
			if strings.Contains(err.Error(), "connecting state") ||
				strings.Contains(err.Error(), "resetting state") {
				e.log.WithError(err).Warnf("Ignored to validate and update engine %v, because the device is still in a transient state", e.Name)
				return nil
			}
			return err
		}
		if err := initiator.LoadEndpoint(); err != nil {
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

func (e *Engine) ReplicaAddStart(replicaName, replicaAddress string) (err error) {
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

	defer func() {
		if err != nil && e.State != types.InstanceStateError {
			e.State = types.InstanceStateError
			updateRequired = true
		}
	}()

	// TODO: For online rebuilding, the IO should be paused first
	snapshotName := GenerateRebuildingSnapshotName()
	updateRequired = e.snapshotOperationWithoutLock(snapshotName, SnapshotOperationCreate)

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
			bdevName, err := getBdevNameForReplica(spdkClient, localReplicaLvsNameMap, replicaName, replicaAddress, e.IP)
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
		if err != nil && e.State != types.InstanceStateError {
			e.State = types.InstanceStateError
			updateRequired = true
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

	// snapLvol.Name is the snapshot lvol name "<REPLICA NAME>-snap-<SNAPSHOT NAME>" rather than the snapshot name
	ancestorSnapshotName, latestSnapshotName := "", ""
	for snapshotName, rpcSnapLvol := range rpcSrcReplica.Snapshots {
		if rpcSnapLvol.Parent == "" {
			ancestorSnapshotName = snapshotName
		}
		if rpcSnapLvol.Children[srcReplicaName] {
			latestSnapshotName = snapshotName
		}
	}
	if ancestorSnapshotName == "" || latestSnapshotName == "" {
		return fmt.Errorf("cannot find the ancestor snapshot %s or latest snapshot %s from RW replica %s snapshot map during engine %s replica %s shallow copy", ancestorSnapshotName, latestSnapshotName, srcReplicaName, e.Name, dstReplicaName)
	}

	defer func() {
		// Blindly mark the rebuilding replica as mode ERR now.
		if err != nil {
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

	// Reverse the src replica snapshot tree with a DFS way and do shallow copy one by one
	stack := []string{ancestorSnapshotName}
	for currentSnapshotName := stack[len(stack)-1]; len(stack) > 0; {
		currentSnapshotName = stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if err = srcReplicaServiceCli.ReplicaSnapshotShallowCopy(srcReplicaName, currentSnapshotName); err != nil {
			return err
		}
		if err = dstReplicaServiceCli.ReplicaRebuildingDstSnapshotCreate(dstReplicaName, currentSnapshotName); err != nil {
			return err
		}

		hasChildSnap := false
		for childSnapLvolName := range rpcSrcReplica.Snapshots[currentSnapshotName].Children {
			if childSnapLvolName == srcReplicaName {
				continue
			}
			stack = append(stack, GetSnapshotNameFromReplicaSnapshotLvolName(srcReplicaName, childSnapLvolName))
			hasChildSnap = true
		}
		if !hasChildSnap {
			// TODO: Ask the dst replica to do snapshot revert
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

func (e *Engine) ReplicaDelete(spdkClient *spdkclient.Client, replicaName, replicaAddress string) (err error) {
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

	if _, err := spdkClient.BdevRaidRemoveBaseBdev(e.ReplicaBdevNameMap[replicaName]); err != nil && !jsonrpc.IsJSONRPCRespErrorNoSuchDevice(err) {
		return err
	}

	delete(e.ReplicaAddressMap, replicaName)
	delete(e.ReplicaModeMap, replicaName)
	delete(e.ReplicaBdevNameMap, replicaName)
	e.log = e.log.WithField("replicaAddressMap", e.ReplicaAddressMap)

	return nil
}

const SnapshotOperationCreate = "snapshot-create"
const SnapshotOperationDelete = "snapshot-delete"

func (e *Engine) SnapshotCreate(snapshotName string) (res *spdkrpc.Engine, err error) {
	return e.snapshotOperation(snapshotName, SnapshotOperationCreate)
}

func (e *Engine) SnapshotDelete(snapshotName string) (res *spdkrpc.Engine, err error) {
	return e.snapshotOperation(snapshotName, SnapshotOperationDelete)
}

func (e *Engine) snapshotOperation(snapshotName, snapshotOp string) (res *spdkrpc.Engine, err error) {
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
		return nil, fmt.Errorf("invalid state %v for engine %s snapshot %s create", e.State, e.Name, snapshotName)
	}

	defer func() {
		if err != nil && e.State != types.InstanceStateError {
			e.State = types.InstanceStateError
			updateRequired = true
		}
	}()

	updateRequired = e.snapshotOperationWithoutLock(snapshotName, snapshotOp)

	e.log.Infof("Engine finished snapshot %s for %v", snapshotOp, snapshotName)

	return e.getWithoutLock(), nil
}

func (e *Engine) snapshotOperationWithoutLock(snapshotName, snapshotOp string) (updated bool) {
	for replicaName := range e.ReplicaAddressMap {
		if err := e.replicaSnapshotOperation(replicaName, snapshotName, snapshotOp); err != nil {
			if e.ReplicaModeMap[replicaName] != types.ModeRW {
				continue
			}
			e.ReplicaModeMap[replicaName] = types.ModeERR
			e.log.WithError(err).Errorf("Failed to issue operation %s for replica %s snapshot %s, will mark the replica as mode ERR", snapshotOp, replicaName, snapshotName)
			updated = true
		}
	}

	return updated
}

func (e *Engine) replicaSnapshotOperation(replicaName, snapshotName, snapshotOp string) error {
	c, err := GetServiceClient(e.ReplicaAddressMap[replicaName])
	if err != nil {
		return err
	}

	switch snapshotOp {
	case SnapshotOperationCreate:
		// TODO: execute `sync` for the nvme initiator before snapshot start
		return c.ReplicaSnapshotCreate(replicaName, snapshotName)
	case SnapshotOperationDelete:
		return c.ReplicaSnapshotDelete(replicaName, snapshotName)
	default:
		return fmt.Errorf("unknown replica snapshot operation %s", snapshotOp)
	}
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
