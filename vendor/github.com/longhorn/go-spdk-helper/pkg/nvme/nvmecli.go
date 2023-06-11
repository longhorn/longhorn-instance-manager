package nvme

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/longhorn/go-spdk-helper/pkg/util"
)

const (
	nvmeBinary = "nvme"

	DefaultTransportType = "tcp"
)

type Device struct {
	Subsystem    string
	SubsystemNQN string
	Controllers  []Controller
	Namespaces   []Namespace
}

type DiscoveryPageEntry struct {
	PortID  uint16 `json:"portid"`
	TrsvcID string `json:"trsvcid"`
	Subnqn  string `json:"subnqn"`
	Traddr  string `json:"traddr"`
	SubType string `json:"subtype"`
}

type Controller struct {
	Controller string
	Transport  string
	Address    string
	State      string
}

// Use signed integers instead, because the output of buggy nvme-cli 2.x is possibly negative.
type Namespace struct {
	NameSpace    string
	NSID         int32
	UsedBytes    int64
	MaximumLBA   int32
	PhysicalSize int64
	SectorSize   int32
}

type Subsystem struct {
	Name  string `json:"Name,omitempty"`
	NQN   string `json:"NQN,omitempty"`
	Paths []Path `json:"Paths,omitempty"`
}

type Path struct {
	Name      string `json:"Name,omitempty"`
	Transport string `json:"Transport,omitempty"`
	Address   string `json:"Address,omitempty"`
	State     string `json:"State,omitempty"`
}

func getNvmeVersion(executor util.Executor) (major, minor int, err error) {
	opts := []string{
		"--version",
	}
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return 0, 0, err
	}

	versionStr := ""
	lines := strings.Split(string(outputStr), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "nvme") {
			versionStr = strings.TrimSpace(line)
			break
		}
	}

	var version string
	for _, s := range strings.Split(versionStr, " ") {
		if strings.Contains(s, ".") {
			version = s
			break
		}
	}
	if version == "" {
		return 0, 0, fmt.Errorf("failed to get version from %s", outputStr)
	}
	versionArr := strings.Split(version, ".")
	if len(versionArr) >= 1 {
		major, _ = strconv.Atoi(versionArr[0])
	}
	if len(versionArr) >= 2 {
		minor, _ = strconv.Atoi(versionArr[1])
	}
	return major, minor, nil
}

func performNvmeShowHostNQN(executor util.Executor) (string, error) {
	opts := []string{
		"--show-hostnqn",
	}

	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(outputStr), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "nqn.") {
			return strings.TrimSpace(line), nil
		}
	}
	return "", fmt.Errorf("failed to get host NQN from %s", outputStr)
}

func performNvmeListSubsystems(device string, executor util.Executor) ([]Subsystem, error) {
	major, _, err := getNvmeVersion(executor)
	if err != nil {
		return nil, err
	}

	opts := []string{
		"list-subsys",
		"-o", "json",
	}

	if device != "" {
		opts = append(opts, device)
	}

	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return nil, err
	}
	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return nil, err
	}

	if major == 1 {
		return performNvmeListSubsystemsV1(jsonStr, executor)
	}
	return performNvmeListSubsystemsV2(jsonStr, executor)
}

func performNvmeListSubsystemsV1(jsonStr string, executor util.Executor) ([]Subsystem, error) {
	output := map[string][]Subsystem{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return nil, err
	}

	return output["Subsystems"], nil
}

type ListSubsystemsV2Output struct {
	HostNQN    string      `json:"HostNQN"`
	HostID     string      `json:"HostID"`
	Subsystems []Subsystem `json:"Subsystems"`
}

func performNvmeListSubsystemsV2(jsonStr string, executor util.Executor) ([]Subsystem, error) {
	var output []ListSubsystemsV2Output
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return nil, err
	}

	if len(output) != 1 {
		return nil, fmt.Errorf("unexpected output: %+v", output)
	}

	return output[0].Subsystems, nil
}

// Use signed integers instead, because the output of buggy nvme-cli 2.x is possibly negative.
type NvmeDevice struct {
	NameSpace    int32  `json:"Namespace,omitempty"`
	DevicePath   string `json:"DevicePath,omitempty"`
	Firmware     string `json:"Firmware,omitempty"`
	Index        int32  `json:"Index,omitempty"`
	ModelNumber  string `json:"ModelNumber,omitempty"`
	SerialNumber string `json:"SerialNumber,omitempty"`
	UsedBytes    int64  `json:"UsedBytes,omitempty"`
	MaximumLBA   int32  `json:"MaximumLBA,omitempty"`
	PhysicalSize int64  `json:"PhysicalSize,omitempty"`
	SectorSize   int32  `json:"SectorSize,omitempty"`
}

func performNvmeList(executor util.Executor) ([]NvmeDevice, error) {
	opts := []string{
		"list",
		"-o", "json",
	}
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return nil, err
	}
	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return nil, err
	}
	output := map[string][]NvmeDevice{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return nil, err
	}

	return output["Devices"], nil
}

func performNvmeDiscovery(ip, port string, executor util.Executor) ([]DiscoveryPageEntry, error) {
	hostNQN, err := performNvmeShowHostNQN(executor)
	if err != nil {
		return nil, err
	}

	opts := []string{
		"discover",
		"-q", hostNQN,
		"-t", DefaultTransportType,
		"-a", ip,
		"-s", port,
		"-o", "json",
	}

	// A valid output is like below:
	// # nvme discover -t tcp -a 10.42.2.20 -s 20011 -o json
	//	{
	//		"device" : "nvme0",
	//		"genctr" : 2,
	//		"records" : [
	//		  {
	//			"trtype" : "tcp",
	//			"adrfam" : "ipv4",
	//			"subtype" : "nvme subsystem",
	//			"treq" : "not required",
	//			"portid" : 0,
	//			"trsvcid" : "20001",
	//			"subnqn" : "nqn.2023-01.io.longhorn.spdk:pvc-81bab972-8e6b-48be-b691-18eaa430a897-r-0881c7b4",
	//			"traddr" : "10.42.2.20",
	//			"sectype" : "none"
	//		  },
	//		  {
	//			"trtype" : "tcp",
	//			"adrfam" : "ipv4",
	//			"subtype" : "nvme subsystem",
	//			"treq" : "not required",
	//			"portid" : 0,
	//			"trsvcid" : "20011",
	//			"subnqn" : "nqn.2023-01.io.longhorn.spdk:pvc-5f94d59d-baec-40e5-9e8b-25b79909d14e-e-49c947f5",
	//			"traddr" : "10.42.2.20",
	//			"sectype" : "none"
	//		  }
	//		]
	//	  }

	// nvme discover does not respect the -s option, so we need to filter the output
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return nil, err
	}

	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return nil, err
	}

	var output struct {
		Entries []DiscoveryPageEntry `json:"records"`
	}

	err = json.Unmarshal([]byte(jsonStr), &output)
	if err != nil {
		return nil, err
	}

	return output.Entries, nil
}

func performNvmeConnect(ip, port, nqn string, executor util.Executor) (string, error) {
	hostNQN, err := performNvmeShowHostNQN(executor)
	if err != nil {
		return "", err
	}

	opts := []string{
		"connect",
		"-q", hostNQN,
		"-t", DefaultTransportType,
		"-a", ip,
		"-s", port,
		"--nqn", nqn,
		"-o", "json",
	}

	// The output example:
	// {
	//  "device" : "nvme0"
	// }
	outputStr, err := executor.Execute(nvmeBinary, opts)
	if err != nil {
		return "", err
	}

	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return "", err
	}

	output := map[string]string{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return "", err
	}

	return output["device"], nil
}

func performNvmeDisconnect(nqn string, executor util.Executor) error {
	opts := []string{
		"disconnect",
		"--nqn", nqn,
	}

	// The output example:
	// NQN:nqn.2023-01.io.spdk:raid01 disconnected 1 controller(s)
	//
	// And trying to disconnect a non-existing target would return exit code 0
	_, err := executor.Execute(nvmeBinary, opts)
	return err
}

func extractJSONString(str string) (string, error) {
	startIndex := strings.Index(str, "{")
	startIndexBracket := strings.Index(str, "[")

	if startIndex == -1 && startIndexBracket == -1 {
		return "", fmt.Errorf("invalid JSON string")
	}

	if startIndex != -1 && (startIndexBracket == -1 || startIndex < startIndexBracket) {
		endIndex := strings.LastIndex(str, "}")
		if endIndex == -1 {
			return "", fmt.Errorf("invalid JSON string")
		}
		return str[startIndex : endIndex+1], nil
	} else if startIndexBracket != -1 {
		endIndex := strings.LastIndex(str, "]")
		if endIndex == -1 {
			return "", fmt.Errorf("invalid JSON string")
		}
		return str[startIndexBracket : endIndex+1], nil
	}

	return "", fmt.Errorf("invalid JSON string")
}

// GetIPAndPortFromControllerAddress returns the IP and port from the controller address
// Input can be either "traddr=10.42.2.18 trsvcid=20006" or "traddr=10.42.2.18,trsvcid=20006"
func GetIPAndPortFromControllerAddress(address string) (string, string) {
	var traddr, trsvcid string

	parts := strings.FieldsFunc(address, func(r rune) bool {
		return r == ',' || r == ' '
	})

	for _, part := range parts {
		keyVal := strings.Split(part, "=")
		if len(keyVal) == 2 {
			key := strings.TrimSpace(keyVal[0])
			value := strings.TrimSpace(keyVal[1])
			switch key {
			case "traddr":
				traddr = value
			case "trsvcid":
				trsvcid = value
			}
		}
	}

	return traddr, trsvcid
}
