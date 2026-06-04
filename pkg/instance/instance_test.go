package instance

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"reflect"
	"testing"
	"time"

	rpc "github.com/longhorn/types/pkg/generated/imrpc"
)

func buildTestTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	certPool := x509.NewCertPool()
	certPool.AppendCertsFromPEM(caPEM)
	return &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    certPool,
	}
}

// TestV1DataEngineInstanceOps_StructureCorrect verifies that V1DataEngineInstanceOps
// has the correct fields and does NOT have spdkServiceAddress (which is only for V2).
func TestV1DataEngineInstanceOps_StructureCorrect(t *testing.T) {
	v1Ops := V1DataEngineInstanceOps{}

	v1Type := reflect.TypeOf(v1Ops)

	// V1 should have exactly 2 fields: processManagerServiceAddress and clientTLSConfig
	expectedFieldCount := 2
	actualFieldCount := v1Type.NumField()

	if actualFieldCount != expectedFieldCount {
		t.Errorf("V1DataEngineInstanceOps should have %d fields, but has %d", expectedFieldCount, actualFieldCount)
	}

	_, hasProcessManager := v1Type.FieldByName("processManagerServiceAddress")
	if !hasProcessManager {
		t.Error("V1DataEngineInstanceOps should have processManagerServiceAddress field")
	}

	_, hasClientTLS := v1Type.FieldByName("clientTLSConfig")
	if !hasClientTLS {
		t.Error("V1DataEngineInstanceOps should have clientTLSConfig field")
	}

	_, hasSPDK := v1Type.FieldByName("spdkServiceAddress")
	if hasSPDK {
		t.Error("V1DataEngineInstanceOps should NOT have spdkServiceAddress field (V1 data engine only uses ProcessManager)")
	}
}

// TestV1DataEngineInstanceOps_TLSConfigPropagation verifies that V1DataEngineInstanceOps
// receives the correct clientTLSConfig and processManagerServiceAddress when NewServer is called.
func TestV1DataEngineInstanceOps_TLSConfigPropagation(t *testing.T) {
	ctx := context.Background()
	logsDir := t.TempDir()
	processManagerServiceAddress := "localhost:8500"
	spdkServiceAddress := "localhost:8504"
	tlsConfig := buildTestTLSConfig(t)

	server, err := NewServer(ctx, logsDir, processManagerServiceAddress, spdkServiceAddress, tlsConfig, false)

	if err != nil {
		t.Fatalf("NewServer should succeed, but got error: %v", err)
	}
	if server == nil {
		t.Fatal("Server should not be nil")
	}

	v1Ops, ok := server.ops[rpc.DataEngine_DATA_ENGINE_V1].(V1DataEngineInstanceOps)
	if !ok {
		t.Fatal("ops[DATA_ENGINE_V1] should be V1DataEngineInstanceOps type")
	}

	if v1Ops.clientTLSConfig == nil {
		t.Error("V1 ops clientTLSConfig should not be nil when TLS is enabled")
	}

	if v1Ops.processManagerServiceAddress != processManagerServiceAddress {
		t.Errorf("V1 ops processManagerServiceAddress = %q, want %q", v1Ops.processManagerServiceAddress, processManagerServiceAddress)
	}
}

// TestV2DataEngineInstanceOps_TLSConfigPropagation verifies that V2DataEngineInstanceOps
// receives the correct spdkTLSConfig and spdkServiceAddress when NewServer is called.
func TestV2DataEngineInstanceOps_TLSConfigPropagation(t *testing.T) {
	ctx := context.Background()
	logsDir := t.TempDir()
	processManagerServiceAddress := "localhost:8500"
	spdkServiceAddress := "localhost:8504"
	tlsConfig := buildTestTLSConfig(t)

	server, err := NewServer(ctx, logsDir, processManagerServiceAddress, spdkServiceAddress, tlsConfig, true)

	if err != nil {
		t.Fatalf("NewServer should succeed, but got error: %v", err)
	}
	if server == nil {
		t.Fatal("Server should not be nil")
	}

	v2Ops, ok := server.ops[rpc.DataEngine_DATA_ENGINE_V2].(V2DataEngineInstanceOps)
	if !ok {
		t.Fatal("ops[DATA_ENGINE_V2] should be V2DataEngineInstanceOps type")
	}

	if v2Ops.spdkTLSConfig == nil {
		t.Error("V2 ops spdkTLSConfig should not be nil when TLS is enabled")
	}

	if v2Ops.spdkServiceAddress != spdkServiceAddress {
		t.Errorf("V2 ops spdkServiceAddress = %q, want %q", v2Ops.spdkServiceAddress, spdkServiceAddress)
	}
}

// TestNewServer_WithoutTLS verifies that Server can be created without TLS,
// ensuring backwards compatibility when no TLS config is provided.
func TestNewServer_WithoutTLS(t *testing.T) {
	ctx := context.Background()
	logsDir := t.TempDir()
	processManagerServiceAddress := "localhost:8500"
	spdkServiceAddress := "localhost:8504"

	server, err := NewServer(ctx, logsDir, processManagerServiceAddress, spdkServiceAddress, nil, false)

	if err != nil {
		t.Fatalf("NewServer should succeed without TLS, but got error: %v", err)
	}
	if server == nil {
		t.Fatal("Server should not be nil")
	}

	v1Ops, ok := server.ops[rpc.DataEngine_DATA_ENGINE_V1].(V1DataEngineInstanceOps)
	if !ok {
		t.Fatal("ops[DATA_ENGINE_V1] should be V1DataEngineInstanceOps type")
	}

	if v1Ops.clientTLSConfig != nil {
		t.Error("V1 ops clientTLSConfig should be nil when TLS is not provided")
	}

	v2Ops, ok := server.ops[rpc.DataEngine_DATA_ENGINE_V2].(V2DataEngineInstanceOps)
	if !ok {
		t.Fatal("ops[DATA_ENGINE_V2] should be V2DataEngineInstanceOps type")
	}

	if v2Ops.spdkTLSConfig != nil {
		t.Error("V2 ops spdkTLSConfig should be nil when TLS is not provided")
	}
}
