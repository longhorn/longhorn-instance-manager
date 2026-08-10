package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

func TestLoadTLSConfigsFromDirMaterialPresence(t *testing.T) {
	type tlsFile struct {
		name     string
		contents string
	}

	tests := []struct {
		name    string
		files   []tlsFile
		wantErr bool
	}{
		{
			name: "empty",
		},
		{
			name: "unrelated-entry-only",
			files: []tlsFile{
				{name: "namespace", contents: "default"},
				{name: "token", contents: "synthetic-token"},
			},
		},
		{
			name: "ca-only",
			files: []tlsFile{
				{name: types.TLSCAFile, contents: "malformed CA certificate"},
			},
			wantErr: true,
		},
		{
			name: "cert-only",
			files: []tlsFile{
				{name: types.TLSCertFile, contents: "malformed TLS certificate"},
			},
			wantErr: true,
		},
		{
			name: "key-only",
			files: []tlsFile{
				{name: types.TLSKeyFile, contents: "malformed TLS key"},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tlsDir := t.TempDir()
			for _, file := range test.files {
				if err := os.WriteFile(filepath.Join(tlsDir, file.name), []byte(file.contents), 0644); err != nil {
					t.Fatalf("%s: failed to write %s: %v", test.name, file.name, err)
				}
			}

			serverTLSConfig, clientTLSConfig, err := loadTLSConfigsFromDir(tlsDir)
			if (err != nil) != test.wantErr {
				t.Fatalf("%s: loadTLSConfigsFromDir error presence = %t, want %t", test.name, err != nil, test.wantErr)
			}
			if serverTLSConfig != nil || clientTLSConfig != nil {
				t.Fatalf("%s: expected nil server and client TLS configs", test.name)
			}
		})
	}
}
