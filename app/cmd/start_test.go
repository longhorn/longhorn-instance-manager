package cmd

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

// generateTestCertificates creates a self-signed CA and certificate pair for testing.
func generateTestCertificates(t *testing.T) (caCertPEM, certPEM, keyPEM []byte) {
	// Generate CA private key
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate CA key: %v", err)
	}

	// Create CA certificate template
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Test CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Create CA certificate
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Failed to create CA certificate: %v", err)
	}

	caCertPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	// Generate server private key
	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate server key: %v", err)
	}

	// Create server certificate template
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject: pkix.Name{
			Organization: []string{"Test Server"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:    []string{"localhost"},
	}

	// Create server certificate signed by CA
	serverCertDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caTemplate, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("Failed to create server certificate: %v", err)
	}

	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverCertDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})

	return caCertPEM, certPEM, keyPEM
}

// setupTestCerts creates a temporary directory with test certificate files.
func setupTestCerts(t *testing.T) string {
	certsDir := filepath.Join(t.TempDir(), "certs")
	err := os.MkdirAll(certsDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test certs directory: %v", err)
	}

	caCertPEM, certPEM, keyPEM := generateTestCertificates(t)

	err = os.WriteFile(filepath.Join(certsDir, types.TLSCAFile), caCertPEM, 0644)
	if err != nil {
		t.Fatalf("Failed to write test CA certificate: %v", err)
	}

	err = os.WriteFile(filepath.Join(certsDir, types.TLSCertFile), certPEM, 0644)
	if err != nil {
		t.Fatalf("Failed to write test TLS certificate: %v", err)
	}

	err = os.WriteFile(filepath.Join(certsDir, types.TLSKeyFile), keyPEM, 0600)
	if err != nil {
		t.Fatalf("Failed to write test TLS key: %v", err)
	}

	return certsDir
}

// loadTestTLSConfig loads test certificates and returns a *tls.Config.
func loadTestTLSConfig(t *testing.T) *tls.Config {
	certsDir := setupTestCerts(t)

	caCertPEM, err := os.ReadFile(filepath.Join(certsDir, types.TLSCAFile))
	if err != nil {
		t.Fatalf("Failed to read CA certificate: %v", err)
	}

	certPEM, err := os.ReadFile(filepath.Join(certsDir, types.TLSCertFile))
	if err != nil {
		t.Fatalf("Failed to read TLS certificate: %v", err)
	}

	keyPEM, err := os.ReadFile(filepath.Join(certsDir, types.TLSKeyFile))
	if err != nil {
		t.Fatalf("Failed to read TLS key: %v", err)
	}

	// Create a minimal TLS config for testing
	certPool := x509.NewCertPool()
	ok := certPool.AppendCertsFromPEM(caCertPEM)
	if !ok {
		t.Fatal("Failed to append CA certificate to pool")
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatalf("Failed to create X509 key pair: %v", err)
	}

	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientCAs:    certPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}

	return tlsConfig
}

func TestLoadTLSConfigsFromDirReturnsErrorForInvalidDir(t *testing.T) {
	serverTLSConfig, clientTLSConfig, err := loadTLSConfigsFromDir(filepath.Join(t.TempDir(), "missing"))

	if err == nil {
		t.Fatal("expected invalid TLS directory to fail")
	}
	if serverTLSConfig != nil {
		t.Fatal("server TLS config must be nil when TLS initialization fails")
	}
	if clientTLSConfig != nil {
		t.Fatal("client TLS config must be nil when TLS initialization fails")
	}
	if !strings.Contains(err.Error(), "failed to initialize reloadable server TLS") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadTLSConfigsFromDirReturnsReloadableConfigs(t *testing.T) {
	tlsDir := setupTestCerts(t)

	serverTLSConfig, clientTLSConfig, err := loadTLSConfigsFromDir(tlsDir)

	if err != nil {
		t.Fatalf("loadTLSConfigsFromDir should succeed: %v", err)
	}
	if serverTLSConfig == nil || serverTLSConfig.GetConfigForClient == nil {
		t.Fatal("server TLS config must be reloadable")
	}
	if clientTLSConfig == nil || clientTLSConfig.GetClientCertificate == nil || clientTLSConfig.VerifyConnection == nil {
		t.Fatal("client TLS config must be reloadable")
	}
}

// TestSetupProcessManagerGRPCServer_WithTLS verifies ProcessManager server can be created with TLS config.
func TestSetupProcessManagerGRPCServer_WithTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tlsConfig := loadTestTLSConfig(t)
	portRange := "18500-18501"
	logsDir := t.TempDir()
	listen := "localhost:18500"

	pm, server, listener, err := setupProcessManagerGRPCServer(ctx, portRange, logsDir, listen, tlsConfig)

	if err != nil {
		t.Fatalf("setupProcessManagerGRPCServer should not return error with valid TLS config: %v", err)
	}
	if pm == nil {
		t.Fatal("ProcessManager should not be nil")
	}
	if server == nil {
		t.Fatal("gRPC server should not be nil")
	}
	if listener == nil {
		t.Fatal("Listener should not be nil")
	}

	// Clean up resources
	defer func() {
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Logf("Failed to close listener: %v", err)
		}
	}()
}

// TestSetupProcessManagerGRPCServer_WithoutTLS verifies ProcessManager server can be created without TLS (nil config).
func TestSetupProcessManagerGRPCServer_WithoutTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	portRange := "18502-18503"
	logsDir := t.TempDir()
	listen := "localhost:18502"

	pm, server, listener, err := setupProcessManagerGRPCServer(ctx, portRange, logsDir, listen, nil)

	if err != nil {
		t.Fatalf("setupProcessManagerGRPCServer should not return error without TLS config: %v", err)
	}
	if pm == nil {
		t.Fatal("ProcessManager should not be nil")
	}
	if server == nil {
		t.Fatal("gRPC server should not be nil")
	}
	if listener == nil {
		t.Fatal("Listener should not be nil")
	}

	// Clean up resources
	defer func() {
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Logf("Failed to close listener: %v", err)
		}
	}()
}

// TestSetupDiskGRPCServer_WithTLS verifies DiskService server can be created with TLS config.
func TestSetupDiskGRPCServer_WithTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tlsConfig := loadTestTLSConfig(t)
	listen := "localhost:18504"
	spdkServiceAddress := "localhost:18508" // dummy address, not actually used in this test
	spdkEnabled := false

	server, listener, err := setupDiskGRPCServer(ctx, listen, spdkServiceAddress, spdkEnabled, tlsConfig, tlsConfig)

	if err != nil {
		t.Fatalf("setupDiskGRPCServer should not return error with valid TLS config: %v", err)
	}
	if server == nil {
		t.Fatal("gRPC server should not be nil")
	}
	if listener == nil {
		t.Fatal("Listener should not be nil")
	}

	// Clean up resources
	defer func() {
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Logf("Failed to close listener: %v", err)
		}
	}()
}

// TestSetupSPDKGRPCServer_WithTLS verifies SPDK server can be created with TLS config.
// This test requires a running SPDK daemon (spdk_tgt) with socket at /var/tmp/spdk.sock.
func TestSetupSPDKGRPCServer_WithTLS(t *testing.T) {
	// Check if SPDK socket exists; skip if not available
	spdkSocket := "/var/tmp/spdk.sock"
	if _, err := os.Stat(spdkSocket); os.IsNotExist(err) {
		t.Skipf("SPDK daemon socket not found at %s; skipping SPDK server test", spdkSocket)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tlsConfig := loadTestTLSConfig(t)
	portRange := "18506-18507"
	listen := "localhost:18506"

	server, listener, err := setupSPDKGRPCServer(ctx, portRange, listen, tlsConfig, nil)

	if err != nil {
		t.Fatalf("setupSPDKGRPCServer should not return error with valid TLS config: %v", err)
	}
	if server == nil {
		t.Fatal("gRPC server should not be nil")
	}
	if listener == nil {
		t.Fatal("Listener should not be nil")
	}

	// Clean up resources
	defer func() {
		server.Stop()
		if err := listener.Close(); err != nil {
			t.Logf("Failed to close listener: %v", err)
		}
	}()
}
