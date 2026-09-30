package lvm

import (
	"testing"
	"time"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

func TestLVMDiskMetricsUseUnderlyingDevice(t *testing.T) {
	now := time.Unix(100, 0)
	counters := blockCounters{writeOps: 10, writeSectors: 20, writeMillis: 30}
	sampler := &KernelBlockMetricsSampler{
		samples: map[string]blockSample{},
		now:     func() time.Time { return now },
		read: func(path string) (blockCounters, error) {
			if path != "/dev/sdc" {
				t.Fatalf("expected underlying disk path, got %v", path)
			}
			return counters, nil
		},
	}
	ops := Disk{metricsSampler: sampler}

	if _, err := ops.Metrics(&rpc.DiskGetRequest{DiskName: "local", DiskPath: "/dev/sdc"}); err != nil {
		t.Fatalf("initial disk metrics sample failed: %v", err)
	}
	if _, exists := sampler.samples["disk:/dev/sdc"]; !exists {
		t.Fatalf("disk metrics baseline was not keyed by the underlying device")
	}

	now = now.Add(time.Second)
	counters = blockCounters{writeOps: 14, writeSectors: 28, writeMillis: 38}
	reply, err := ops.Metrics(&rpc.DiskGetRequest{DiskName: "local", DiskPath: "/dev/sdc"})
	if err != nil {
		t.Fatalf("second disk metrics sample failed: %v", err)
	}
	if reply.Metrics.WriteIOPS != 4 || reply.Metrics.WriteThroughput != 4096 || reply.Metrics.WriteLatency != 2_000_000 {
		t.Fatalf("unexpected underlying disk metrics: %+v", reply.Metrics)
	}
}

func TestKernelBlockMetricsSampler(t *testing.T) {
	now := time.Unix(100, 0)
	counters := blockCounters{readOps: 10, readSectors: 100, readMillis: 20, writeOps: 20, writeSectors: 200, writeMillis: 40}
	sampler := &KernelBlockMetricsSampler{
		samples: map[string]blockSample{},
		now:     func() time.Time { return now },
		read:    func(string) (blockCounters, error) { return counters, nil },
	}

	metrics, err := sampler.Sample("device:test", "/dev/vg/a")
	if err != nil {
		t.Fatalf("initial sample failed: %v", err)
	}
	if metrics.ReadIOPS != 0 || metrics.WriteThroughput != 0 {
		t.Fatalf("initial sample should establish a zero baseline: %+v", metrics)
	}

	now = now.Add(2 * time.Second)
	counters = blockCounters{readOps: 30, readSectors: 300, readMillis: 60, writeOps: 60, writeSectors: 600, writeMillis: 120}
	metrics, err = sampler.Sample("device:test", "/dev/vg/a")
	if err != nil {
		t.Fatalf("second sample failed: %v", err)
	}
	if metrics.ReadIOPS != 10 || metrics.WriteIOPS != 20 {
		t.Fatalf("unexpected IOPS: %+v", metrics)
	}
	if metrics.ReadThroughput != 51200 || metrics.WriteThroughput != 102400 {
		t.Fatalf("unexpected throughput: %+v", metrics)
	}
	if metrics.ReadLatency != 2_000_000 || metrics.WriteLatency != 2_000_000 {
		t.Fatalf("unexpected latency: %+v", metrics)
	}
}

func TestListLogicalVolumes(t *testing.T) {
	executor := &fakeExecutor{outputs: map[string]string{
		"lvs": `{"report":[{"lv":[` +
			`{"vg_name":"longhorn-disk-a","lv_name":"replica-a","lv_size":"1073741824","lv_active":"active","lv_path":"/dev/longhorn-disk-a/replica-a","lv_tags":"longhorn-engine=engine-a,other"},` +
			`{"vg_name":"another-vg","lv_name":"replica-b","lv_size":"2147483648","lv_active":"","lv_path":"/dev/another-vg/replica-b","lv_tags":""}` +
			`]}]}`,
	}}
	lvs, err := ListLogicalVolumes(executor)
	if err != nil {
		t.Fatalf("failed to list logical volumes: %v", err)
	}
	if len(lvs) != 2 || lvs[0].VGName != "longhorn-disk-a" || lvs[0].Size != 1073741824 || !lvs[0].Active || len(lvs[0].Tags) != 2 || lvs[1].VGName != "another-vg" {
		t.Fatalf("unexpected logical volumes: %+v", lvs)
	}
}

func TestParseLogicalVolumesRejectsMalformedJSON(t *testing.T) {
	if _, err := parseLogicalVolumes("not-json"); err == nil {
		t.Fatal("expected malformed lvs JSON to be rejected")
	}
}

func TestKernelBlockMetricsSamplerForgetsStaleDevices(t *testing.T) {
	now := time.Unix(100, 0)
	counters := blockCounters{readOps: 10, readSectors: 100, readMillis: 20, writeOps: 20, writeSectors: 200, writeMillis: 40}
	sampler := &KernelBlockMetricsSampler{
		samples: map[string]blockSample{},
		now:     func() time.Time { return now },
		read:    func(string) (blockCounters, error) { return counters, nil },
	}

	if _, err := sampler.Sample("engine:gone", "/dev/vg/gone"); err != nil {
		t.Fatalf("sample failed: %v", err)
	}
	now = now.Add(sampleRetention / 2)
	if _, err := sampler.Sample("engine:live", "/dev/vg/live"); err != nil {
		t.Fatalf("sample failed: %v", err)
	}
	if len(sampler.samples) != 2 {
		t.Fatalf("entries within the retention window must be kept: %v", sampler.samples)
	}

	// The gone engine is never sampled again; the live one keeps being polled.
	now = now.Add(sampleRetention/2 + time.Second)
	if _, err := sampler.Sample("engine:live", "/dev/vg/live"); err != nil {
		t.Fatalf("sample failed: %v", err)
	}
	if _, exists := sampler.samples["engine:gone"]; exists {
		t.Fatalf("stale entry must be forgotten: %v", sampler.samples)
	}
	if _, exists := sampler.samples["engine:live"]; !exists {
		t.Fatalf("live entry must be kept: %v", sampler.samples)
	}
}
