package cmd

import (
	"context"
	"flag"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urfave/cli"
)

func newProcessClientTestContext(t *testing.T, tlsDir string) *cli.Context {
	t.Helper()
	set := flag.NewFlagSet("test", flag.ContinueOnError)
	set.String("url", "tcp://127.0.0.1:1", "")
	set.String("tls-dir", tlsDir, "")
	if err := set.Set("url", "tcp://127.0.0.1:1"); err != nil {
		t.Fatalf("set url: %v", err)
	}
	if err := set.Set("tls-dir", tlsDir); err != nil {
		t.Fatalf("set tls-dir: %v", err)
	}
	return cli.NewContext(cli.NewApp(), set, nil)
}

func TestGetProcessManagerClientFailsClosedWhenTLSDirInvalid(t *testing.T) {
	c := newProcessClientTestContext(t, filepath.Join(t.TempDir(), "missing"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := getProcessManagerClient(c, ctx, cancel)

	if err == nil {
		t.Fatal("expected TLS initialization failure")
	}
	if cli != nil {
		t.Fatal("client must be nil when TLS initialization fails")
	}
	if !strings.Contains(err.Error(), "failed to initialize TLS ProcessManager client") {
		t.Fatalf("unexpected error: %v", err)
	}
}
