package lvm

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	lhexec "github.com/longhorn/go-common-libs/exec"
	enginerpc "github.com/longhorn/types/pkg/generated/enginerpc"
	"golang.org/x/sys/unix"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	kernelSectorSize = 512

	// sampleRetention bounds the sampler's memory to devices that are still
	// being polled. Entries are refreshed on every metrics scrape while an
	// engine or disk exists, so one that has not been touched for this long
	// belongs to a device that is gone. It must stay well above any scrape
	// interval, or a live device would lose its baseline between scrapes.
	sampleRetention = 10 * time.Minute
)

type blockCounters struct {
	readOps, readSectors, readMillis    uint64
	writeOps, writeSectors, writeMillis uint64
}

type blockSample struct {
	at       time.Time
	counters blockCounters
}

// KernelBlockMetricsSampler converts cumulative Linux block statistics into
// byte/s, IOPS, and average latency in nanoseconds.
type KernelBlockMetricsSampler struct {
	mu      sync.Mutex
	samples map[string]blockSample
	now     func() time.Time
	read    func(string) (blockCounters, error)
}

type EngineMetrics struct {
	executor       lhexec.ExecuteInterface
	metricsSampler *KernelBlockMetricsSampler
}

func NewEngineMetrics() EngineMetrics {
	return EngineMetrics{
		executor:       NewExecutor(),
		metricsSampler: NewKernelBlockMetricsSampler(),
	}
}

func (ops EngineMetrics) Get(engineName string) (*enginerpc.Metrics, error) {
	lvs, err := ListLogicalVolumes(ops.executor)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}

	engineTag := lvmEngineTagPrefix + engineName
	var devicePath string
	for _, lv := range lvs {
		if !lv.Active || lv.Path == "" {
			continue
		}
		for _, tag := range lv.Tags {
			if tag != engineTag {
				continue
			}
			if devicePath != "" {
				return nil, grpcstatus.Errorf(grpccodes.FailedPrecondition, "found multiple active local volumes for engine %v", engineName)
			}
			devicePath = lv.Path
			break
		}
	}
	if devicePath == "" {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find active local volume for engine %v", engineName)
	}

	metrics, err := ops.metricsSampler.Sample("engine:"+engineName, devicePath)
	if err != nil {
		return nil, grpcstatus.Error(grpccodes.Internal, err.Error())
	}
	return metrics, nil
}

func NewKernelBlockMetricsSampler() *KernelBlockMetricsSampler {
	return &KernelBlockMetricsSampler{
		samples: map[string]blockSample{},
		now:     time.Now,
		read:    readKernelBlockCounters,
	}
}

func (s *KernelBlockMetricsSampler) Sample(key, devicePath string) (*enginerpc.Metrics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	current, err := s.read(devicePath)
	if err != nil {
		return nil, err
	}

	now := s.now()
	for staleKey, sample := range s.samples {
		if now.Sub(sample.at) > sampleRetention {
			delete(s.samples, staleKey)
		}
	}
	previous, exists := s.samples[key]
	s.samples[key] = blockSample{at: now, counters: current}
	if !exists || !countersAtLeast(current, previous.counters) {
		return &enginerpc.Metrics{}, nil
	}
	elapsed := now.Sub(previous.at).Seconds()
	if elapsed <= 0 {
		return &enginerpc.Metrics{}, nil
	}

	readOps := current.readOps - previous.counters.readOps
	writeOps := current.writeOps - previous.counters.writeOps
	metrics := &enginerpc.Metrics{
		ReadThroughput:  uint64(float64((current.readSectors-previous.counters.readSectors)*kernelSectorSize) / elapsed),
		WriteThroughput: uint64(float64((current.writeSectors-previous.counters.writeSectors)*kernelSectorSize) / elapsed),
		ReadIOPS:        uint64(float64(readOps) / elapsed),
		WriteIOPS:       uint64(float64(writeOps) / elapsed),
	}
	if readOps > 0 {
		metrics.ReadLatency = (current.readMillis - previous.counters.readMillis) * uint64(time.Millisecond) / readOps
	}
	if writeOps > 0 {
		metrics.WriteLatency = (current.writeMillis - previous.counters.writeMillis) * uint64(time.Millisecond) / writeOps
	}
	return metrics, nil
}

func countersAtLeast(current, previous blockCounters) bool {
	return current.readOps >= previous.readOps &&
		current.readSectors >= previous.readSectors &&
		current.readMillis >= previous.readMillis &&
		current.writeOps >= previous.writeOps &&
		current.writeSectors >= previous.writeSectors &&
		current.writeMillis >= previous.writeMillis
}

func readKernelBlockCounters(devicePath string) (blockCounters, error) {
	var stat unix.Stat_t
	if err := unix.Stat(devicePath, &stat); err != nil {
		return blockCounters{}, fmt.Errorf("failed to stat block device %v: %w", devicePath, err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFBLK {
		return blockCounters{}, fmt.Errorf("device path %v is not a block device", devicePath)
	}
	major := unix.Major(uint64(stat.Rdev))
	minor := unix.Minor(uint64(stat.Rdev))
	statPath := fmt.Sprintf("/sys/dev/block/%d:%d/stat", major, minor)
	data, err := os.ReadFile(statPath)
	if err != nil {
		return blockCounters{}, fmt.Errorf("failed to read block statistics for %v: %w", devicePath, err)
	}
	fields := strings.Fields(string(data))
	if len(fields) < 8 {
		return blockCounters{}, fmt.Errorf("unexpected block statistics for %v: %q", devicePath, strings.TrimSpace(string(data)))
	}
	values := make([]uint64, 8)
	for i := range values {
		values[i], err = strconv.ParseUint(fields[i], 10, 64)
		if err != nil {
			return blockCounters{}, fmt.Errorf("failed to parse block statistics for %v: %w", devicePath, err)
		}
	}
	return blockCounters{
		readOps:      values[0],
		readSectors:  values[2],
		readMillis:   values[3],
		writeOps:     values[4],
		writeSectors: values[6],
		writeMillis:  values[7],
	}, nil
}
