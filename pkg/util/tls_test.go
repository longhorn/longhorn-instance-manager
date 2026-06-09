package util

import (
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
)

func generateSelfSignedCert(t *testing.T) (caPEM, certPEM, keyPEM []byte) {
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
	caPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	serverKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	serverTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-server"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"test-server"},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTmpl, caCert, &serverKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(serverKey)})
	return
}

func writeCerts(t *testing.T, dir string, caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), caPEM, 0644); err != nil {
		t.Fatalf("write ca.crt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), certPEM, 0644); err != nil {
		t.Fatalf("write tls.crt: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tls.key"), keyPEM, 0600); err != nil {
		t.Fatalf("write tls.key: %v", err)
	}
}

func TestTLSConfigProvider_InitialLoad(t *testing.T) {
	dir := t.TempDir()
	caPEM, certPEM, keyPEM := generateSelfSignedCert(t)
	writeCerts(t, dir, caPEM, certPEM, keyPEM)

	p, err := newTLSConfigProvider(
		filepath.Join(dir, "ca.crt"),
		filepath.Join(dir, "tls.crt"),
		filepath.Join(dir, "tls.key"),
		"test-server",
	)
	if err != nil {
		t.Fatalf("newTLSConfigProvider: %v", err)
	}
	if p.snapshot == nil {
		t.Fatal("snapshot must not be nil after initial load")
	}
	if p.snapshot.certPool == nil {
		t.Fatal("certPool must not be nil after initial load")
	}
}

func TestTLSConfigProvider_ReloadOnChange(t *testing.T) {
	dir := t.TempDir()
	caPEM, certPEM, keyPEM := generateSelfSignedCert(t)
	writeCerts(t, dir, caPEM, certPEM, keyPEM)

	p, err := newTLSConfigProvider(
		filepath.Join(dir, "ca.crt"),
		filepath.Join(dir, "tls.crt"),
		filepath.Join(dir, "tls.key"),
		"test-server",
	)
	if err != nil {
		t.Fatalf("newTLSConfigProvider: %v", err)
	}
	oldSnap := p.snapshot
	oldCertDER := make([]byte, len(oldSnap.cert.Certificate[0]))
	copy(oldCertDER, oldSnap.cert.Certificate[0])

	caPEM2, certPEM2, keyPEM2 := generateSelfSignedCert(t)
	writeCerts(t, dir, caPEM2, certPEM2, keyPEM2)
	// Advance mtime so statFiles detects a change.
	future := time.Now().Add(2 * time.Second)
	for _, f := range []string{"ca.crt", "tls.crt", "tls.key"} {
		if err := os.Chtimes(filepath.Join(dir, f), future, future); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	p.maybeReload()

	if p.snapshot == oldSnap {
		t.Error("snapshot must be replaced after file change")
	}
	newCertDER := p.snapshot.cert.Certificate[0]
	if string(newCertDER) == string(oldCertDER) {
		t.Error("cert content must differ after reload with new certificates")
	}
}

func TestTLSConfigProvider_LastKnownGoodOnBadReload(t *testing.T) {
	dir := t.TempDir()
	caPEM, certPEM, keyPEM := generateSelfSignedCert(t)
	writeCerts(t, dir, caPEM, certPEM, keyPEM)

	p, err := newTLSConfigProvider(
		filepath.Join(dir, "ca.crt"),
		filepath.Join(dir, "tls.crt"),
		filepath.Join(dir, "tls.key"),
		"test-server",
	)
	if err != nil {
		t.Fatalf("newTLSConfigProvider: %v", err)
	}
	oldSnap := p.snapshot

	// Corrupt the cert file and advance mtime so maybeReload attempts a reload.
	if err := os.WriteFile(filepath.Join(dir, "tls.crt"), []byte("not a valid cert"), 0644); err != nil {
		t.Fatalf("corrupt cert file: %v", err)
	}
	future := time.Now().Add(2 * time.Second)
	for _, f := range []string{"ca.crt", "tls.crt", "tls.key"} {
		if err := os.Chtimes(filepath.Join(dir, f), future, future); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	p.maybeReload()

	if p.snapshot != oldSnap {
		t.Error("snapshot must be retained (last known good) when reload fails")
	}
}

func TestTLSConfigProvider_ServerConfig(t *testing.T) {
	dir := t.TempDir()
	caPEM, certPEM, keyPEM := generateSelfSignedCert(t)
	writeCerts(t, dir, caPEM, certPEM, keyPEM)

	p, err := newTLSConfigProvider(
		filepath.Join(dir, "ca.crt"),
		filepath.Join(dir, "tls.crt"),
		filepath.Join(dir, "tls.key"),
		"test-server",
	)
	if err != nil {
		t.Fatalf("newTLSConfigProvider: %v", err)
	}
	cfg := p.ServerConfig()
	if cfg == nil {
		t.Fatal("ServerConfig must not return nil")
	}
	if cfg.GetConfigForClient == nil {
		t.Error("ServerConfig must set GetConfigForClient")
	}
	inner, err := cfg.GetConfigForClient(&tls.ClientHelloInfo{})
	if err != nil {
		t.Fatalf("GetConfigForClient: %v", err)
	}
	if len(inner.Certificates) == 0 {
		t.Error("inner config must include the server certificate")
	}
}

func TestTLSConfigProvider_ClientConfig(t *testing.T) {
	dir := t.TempDir()
	caPEM, certPEM, keyPEM := generateSelfSignedCert(t)
	writeCerts(t, dir, caPEM, certPEM, keyPEM)

	p, err := newTLSConfigProvider(
		filepath.Join(dir, "ca.crt"),
		filepath.Join(dir, "tls.crt"),
		filepath.Join(dir, "tls.key"),
		"test-server",
	)
	if err != nil {
		t.Fatalf("newTLSConfigProvider: %v", err)
	}
	cfg := p.ClientConfig()
	if cfg == nil {
		t.Fatal("ClientConfig must not return nil")
	}
	if cfg.GetClientCertificate == nil {
		t.Error("ClientConfig must set GetClientCertificate")
	}
	if cfg.VerifyConnection == nil {
		t.Error("ClientConfig must set VerifyConnection")
	}
	cert, err := cfg.GetClientCertificate(nil)
	if err != nil {
		t.Fatalf("GetClientCertificate: %v", err)
	}
	if len(cert.Certificate) == 0 {
		t.Error("GetClientCertificate must return a valid certificate")
	}
}
