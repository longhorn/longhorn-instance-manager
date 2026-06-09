package cmd

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urfave/cli/v3"
)

func newProcessClientTestCommand(t *testing.T, tlsDir string) *cli.Command {
	t.Helper()
	cmd := &cli.Command{
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "url", Value: "tcp://127.0.0.1:1"},
			&cli.StringFlag{Name: "tls-dir", Value: tlsDir},
		},
	}
	if err := cmd.Run(t.Context(), []string{"test"}); err != nil {
		t.Fatalf("initialize command: %v", err)
	}
	return cmd
}

func TestGetProcessManagerClientFailsClosedWhenTLSDirInvalid(t *testing.T) {
	c := newProcessClientTestCommand(t, filepath.Join(t.TempDir(), "missing"))
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
