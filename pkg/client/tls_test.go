package client

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
	"testing"
	"time"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

func writeClientTestCerts(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	clientTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: types.TLSPeerName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{types.TLSPeerName},
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTmpl, caCert, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}

	files := map[string][]byte{
		types.TLSCAFile:   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		types.TLSCertFile: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientDER}),
		types.TLSKeyFile:  pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(clientKey)}),
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), content, 0600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

func assertReloadableTLSConfig(t *testing.T, tlsConfig *tls.Config) {
	t.Helper()
	if tlsConfig == nil {
		t.Fatal("TLS config must not be nil")
	}
	if tlsConfig.GetClientCertificate == nil {
		t.Fatal("TLS config must reload client certificates")
	}
	if tlsConfig.VerifyConnection == nil {
		t.Fatal("TLS config must verify server certificates with reloadable CA data")
	}
}

func TestWithTLSConstructorsUseReloadableClientTLS(t *testing.T) {
	tlsDir := writeClientTestCerts(t)
	caFile := filepath.Join(tlsDir, types.TLSCAFile)
	certFile := filepath.Join(tlsDir, types.TLSCertFile)
	keyFile := filepath.Join(tlsDir, types.TLSKeyFile)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	diskClient, err := NewDiskServiceClientWithTLS(ctx, cancel, "tcp://127.0.0.1:1", caFile, certFile, keyFile, types.TLSPeerName)
	if err != nil {
		t.Fatalf("NewDiskServiceClientWithTLS: %v", err)
	}
	defer diskClient.Close()
	assertReloadableTLSConfig(t, diskClient.tlsConfig)

	instanceClient, err := NewInstanceServiceClientWithTLS(ctx, cancel, "tcp://127.0.0.1:1", caFile, certFile, keyFile, types.TLSPeerName)
	if err != nil {
		t.Fatalf("NewInstanceServiceClientWithTLS: %v", err)
	}
	defer instanceClient.Close()
	assertReloadableTLSConfig(t, instanceClient.tlsConfig)

	processClient, err := NewProcessManagerClientWithTLS(ctx, cancel, "tcp://127.0.0.1:1", caFile, certFile, keyFile, types.TLSPeerName)
	if err != nil {
		t.Fatalf("NewProcessManagerClientWithTLS: %v", err)
	}
	defer processClient.Close()
	assertReloadableTLSConfig(t, processClient.tlsConfig)

	proxyClient, err := NewProxyClientWithTLS(ctx, cancel, "127.0.0.1", 1, caFile, certFile, keyFile, types.TLSPeerName)
	if err != nil {
		t.Fatalf("NewProxyClientWithTLS: %v", err)
	}
	defer proxyClient.Close()
	assertReloadableTLSConfig(t, proxyClient.tlsConfig)
}
