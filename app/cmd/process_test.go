package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
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

func TestGetProcessManagerClientTLSMaterial(t *testing.T) {
	tests := []struct {
		name       string
		missingDir bool
		tlsCert    string
		wantErr    bool
	}{
		{
			name:       "missing-directory",
			missingDir: true,
			wantErr:    true,
		},
		{
			name: "empty-directory",
		},
		{
			name:    "partial-material",
			tlsCert: "partial TLS material",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tlsDir := t.TempDir()
			if test.missingDir {
				tlsDir = filepath.Join(tlsDir, "missing")
			}
			if test.tlsCert != "" {
				if err := os.WriteFile(filepath.Join(tlsDir, types.TLSCertFile), []byte(test.tlsCert), 0644); err != nil {
					t.Fatalf("failed to write partial TLS material: %v", err)
				}
			}

			c := newProcessClientTestCommand(t, tlsDir)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cli, err := getProcessManagerClient(c, ctx, cancel)
			if (err != nil) != test.wantErr {
				t.Fatalf("getProcessManagerClient error presence = %t, want %t: %v", err != nil, test.wantErr, err)
			}
			if test.wantErr {
				if cli != nil {
					t.Fatal("client must be nil when TLS initialization fails")
				}
				if !strings.Contains(err.Error(), "failed to initialize TLS ProcessManager client") {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if cli == nil {
				t.Fatal("expected non-nil plaintext ProcessManager client")
			}
			t.Cleanup(func() {
				if err := cli.Close(); err != nil {
					t.Errorf("failed to close plaintext ProcessManager client: %v", err)
				}
			})
		})
	}
}
