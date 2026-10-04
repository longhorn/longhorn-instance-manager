package lvm

import (
	"strings"
	"testing"
	"time"
)

func TestExecutorKillsProcessGroupOnTimeout(t *testing.T) {
	start := time.Now()
	_, err := NewExecutor().Execute(nil, "/bin/sh", []string{"-c", "sleep 10 & wait"}, 100*time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timeout executing") {
		t.Fatalf("expected timeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("timed-out process group was not terminated promptly: %v", elapsed)
	}
}
