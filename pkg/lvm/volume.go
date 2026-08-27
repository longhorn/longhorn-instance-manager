package lvm

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	lhexec "github.com/longhorn/go-common-libs/exec"
	lhtypes "github.com/longhorn/go-common-libs/types"
)

const lvmLogicalVolumeFields = "vg_name,lv_name,lv_size,lv_active,lv_path,lv_tags,lv_attr,pool_lv,data_percent"

// LogicalVolume is the LVM metadata used by local-engine instance and
// metrics operations.
type LogicalVolume struct {
	VGName string
	Name   string
	Size   int64
	Active bool
	Path   string
	Tags   []string
	Attr   string
	PoolLV string
	// DataUsagePercentage is set for thin LVs and thin pools.
	DataUsagePercentage float64
}

type lvmLogicalVolumeReport struct {
	Reports []struct {
		LVs []lvmLogicalVolumeEntry `json:"lv"`
	} `json:"report"`
}

type lvmLogicalVolumeEntry struct {
	VGName      string `json:"vg_name"`
	LVName      string `json:"lv_name"`
	LVSize      string `json:"lv_size"`
	LVActive    string `json:"lv_active"`
	LVPath      string `json:"lv_path"`
	LVTags      string `json:"lv_tags"`
	LVAttr      string `json:"lv_attr"`
	PoolLV      string `json:"pool_lv"`
	DataPercent string `json:"data_percent"`
}

func (lv LogicalVolume) IsThinPool() bool {
	return lv.Name == ThinPoolName && len(lv.Attr) > 0 && lv.Attr[0] == 't'
}

func (lv LogicalVolume) IsThinVolume() bool {
	return lv.PoolLV == ThinPoolName
}

// ListLogicalVolumes returns all LVs visible through the Longhorn devices file.
func ListLogicalVolumes(executor lhexec.ExecuteInterface) ([]LogicalVolume, error) {
	return queryLogicalVolumes(executor, "")
}

// GetLogicalVolume returns one LV from a Longhorn-managed VG, or nil when
// it does not exist.
func GetLogicalVolume(executor lhexec.ExecuteInterface, vgName, lvName string) (*LogicalVolume, error) {
	selection := fmt.Sprintf("vg_name=%s && lv_name=%s", vgName, lvName)
	lvs, err := queryLogicalVolumes(executor, selection)
	if err != nil {
		return nil, err
	}
	if len(lvs) == 0 {
		return nil, nil
	}
	if len(lvs) != 1 {
		return nil, fmt.Errorf("expected one logical volume %v/%v, found %v", vgName, lvName, len(lvs))
	}
	return &lvs[0], nil
}

func queryLogicalVolumes(executor lhexec.ExecuteInterface, selection string) ([]LogicalVolume, error) {
	args := []string{"--reportformat", "json", "--units", "b", "--nosuffix"}
	if selection != "" {
		args = append(args, "--select", selection)
	}
	args = append(args, "-o", lvmLogicalVolumeFields)
	output, err := executor.Execute(nil, "lvs", CommandArgs(args...), lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return nil, fmt.Errorf("failed to query logical volumes: %w", err)
	}
	return parseLogicalVolumes(output)
}

func parseLogicalVolumes(output string) ([]LogicalVolume, error) {
	var report lvmLogicalVolumeReport
	if err := json.Unmarshal([]byte(output), &report); err != nil {
		return nil, fmt.Errorf("failed to parse lvs JSON output: %w", err)
	}

	var lvs []LogicalVolume
	for _, section := range report.Reports {
		for _, entry := range section.LVs {
			size, err := strconv.ParseInt(strings.TrimSpace(entry.LVSize), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse size of logical volume %v/%v: %w", entry.VGName, entry.LVName, err)
			}
			var tags []string
			for _, tag := range strings.Split(entry.LVTags, ",") {
				if tag = strings.TrimSpace(tag); tag != "" {
					tags = append(tags, tag)
				}
			}
			dataUsage, err := parsePercentage(entry.DataPercent)
			if err != nil {
				return nil, fmt.Errorf("failed to parse data usage of logical volume %v/%v: %w", entry.VGName, entry.LVName, err)
			}
			lvs = append(lvs, LogicalVolume{
				VGName:              entry.VGName,
				Name:                entry.LVName,
				Size:                size,
				Active:              strings.TrimSpace(entry.LVActive) == "active",
				Path:                strings.TrimSpace(entry.LVPath),
				Tags:                tags,
				Attr:                strings.TrimSpace(entry.LVAttr),
				PoolLV:              strings.TrimSpace(entry.PoolLV),
				DataUsagePercentage: dataUsage,
			})
		}
	}
	return lvs, nil
}

func parsePercentage(value string) (float64, error) {
	value = strings.TrimSpace(value)
	if value == "" || value == "-" {
		return 0, nil
	}
	return strconv.ParseFloat(value, 64)
}
