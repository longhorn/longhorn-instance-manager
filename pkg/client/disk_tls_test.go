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
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/longhorn/longhorn-instance-manager/pkg/types"
)

type minimalHealthServer struct {
	healthpb.UnimplementedHealthServer
}

func (s *minimalHealthServer) Check(_ context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// testMTLSCerts generates a shared CA, a server cert, and a client cert all signed
// by that CA. Both serverTLS and clientTLS require mutual authentication.
func testMTLSCerts(t *testing.T) (serverTLS, clientTLS *tls.Config) {
	t.Helper()

	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{Organization: []string{"Test CA"}},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caCertDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(caCertDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	caCertPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caCertDER})

	makeCert := func(serial int64, dnsName string) tls.Certificate {
		key, kerr := rsa.GenerateKey(rand.Reader, 2048)
		if kerr != nil {
			t.Fatalf("generate cert key: %v", kerr)
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: dnsName},
			DNSNames:     []string{dnsName},
			NotBefore:    time.Now(),
			NotAfter:     time.Now().Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
			ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		}
		certDER, cerr := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if cerr != nil {
			t.Fatalf("create cert: %v", cerr)
		}
		cert, terr := tls.X509KeyPair(
			pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
			pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}),
		)
		if terr != nil {
			t.Fatalf("load cert pair: %v", terr)
		}
		return cert
	}

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCertPEM)

	serverTLS = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{makeCert(2, types.TLSPeerName)},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	clientTLS = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{makeCert(3, "longhorn-client")},
		RootCAs:      pool,
		ServerName:   types.TLSPeerName,
	}
	return serverTLS, clientTLS
}

// startHealthGRPCServer starts a gRPC server with the health service registered.
// If serverTLS is nil the server is plaintext. Returns the listen address and a stop function.
func startHealthGRPCServer(t *testing.T, serverTLS *tls.Config) (addr string, stop func()) {
	t.Helper()
	var cred grpc.ServerOption
	if serverTLS != nil {
		cred = grpc.Creds(credentials.NewTLS(serverTLS))
	} else {
		cred = grpc.Creds(insecure.NewCredentials())
	}
	srv := grpc.NewServer(cred)
	healthpb.RegisterHealthServer(srv, &minimalHealthServer{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), srv.Stop
}

// TestDiskServiceClient_MTLSHandshakeSuccess verifies a TLS client connecting to
// a TLS server with mutual authentication completes the handshake successfully.
func TestDiskServiceClient_MTLSHandshakeSuccess(t *testing.T) {
	serverTLS, clientTLS := testMTLSCerts(t)
	addr, stop := startHealthGRPCServer(t, serverTLS)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	c, err := NewDiskServiceClient(ctx, cancel, "tcp://"+addr, clientTLS)
	if err != nil {
		t.Fatalf("NewDiskServiceClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.CheckConnection(); err != nil {
		t.Errorf("mTLS handshake should succeed, got: %v", err)
	}
}

// TestDiskServiceClient_RejectedWithoutClientCert verifies the server rejects a
// client that presents no certificate when mTLS is required.
func TestDiskServiceClient_RejectedWithoutClientCert(t *testing.T) {
	serverTLS, clientTLS := testMTLSCerts(t)
	addr, stop := startHealthGRPCServer(t, serverTLS)
	defer stop()

	// Strip client cert; keep RootCAs so the TLS dial gets past DNS/routing to the actual handshake.
	noClientCertTLS := &tls.Config{
		MinVersion: tls.VersionTLS13,
		RootCAs:    clientTLS.RootCAs,
		ServerName: types.TLSPeerName,
	}

	ctx, cancel := context.WithCancel(context.Background())
	c, err := NewDiskServiceClient(ctx, cancel, "tcp://"+addr, noClientCertTLS)
	if err != nil {
		t.Fatalf("NewDiskServiceClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.CheckConnection(); err == nil {
		t.Error("server must reject client that presents no cert when mTLS is required, but got nil error")
	}
}

// TestDiskServiceClient_WrongCAFailure verifies the client rejects a server whose
// certificate was not signed by the trusted CA.
func TestDiskServiceClient_WrongCAFailure(t *testing.T) {
	serverTLS, _ := testMTLSCerts(t)
	addr, stop := startHealthGRPCServer(t, serverTLS)
	defer stop()

	// Second CA: client trusts this but server cert was signed by the first CA.
	_, wrongClientTLS := testMTLSCerts(t)
	wrongClientTLS.ServerName = types.TLSPeerName

	ctx, cancel := context.WithCancel(context.Background())
	c, err := NewDiskServiceClient(ctx, cancel, "tcp://"+addr, wrongClientTLS)
	if err != nil {
		t.Fatalf("NewDiskServiceClient: %v", err)
	}
	defer func() { _ = c.Close() }()

	if err := c.CheckConnection(); err == nil {
		t.Error("client must reject server cert signed by an untrusted CA, but got nil error")
	}
}
