package util

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
)

type tlsSnapshot struct {
	cert     tls.Certificate
	certPool *x509.CertPool
}

// TLSConfigProvider loads TLS credentials from disk and reloads them lazily on each
// new TLS handshake if any of the three cert files have changed since the last
// successful load. On reload failure the last known good snapshot is retained
// and a warning is logged - the provider never downgrades to plaintext.
type TLSConfigProvider struct {
	caFile   string
	certFile string
	keyFile  string
	peerName string

	log      *logrus.Entry
	mu       sync.RWMutex
	snapshot *tlsSnapshot
	mtimes   [3]time.Time // [caFile, certFile, keyFile]
}

func newTLSConfigProvider(caFile, certFile, keyFile, peerName string) (*TLSConfigProvider, error) {
	p := &TLSConfigProvider{
		caFile:   caFile,
		certFile: certFile,
		keyFile:  keyFile,
		peerName: peerName,
		log: logrus.WithFields(logrus.Fields{
			"caFile":   caFile,
			"certFile": certFile,
			"keyFile":  keyFile,
			"peerName": peerName,
		}),
	}
	snap, mtimes, err := p.doLoad()
	if err != nil {
		return nil, err
	}
	p.snapshot = snap
	p.mtimes = mtimes
	return p, nil
}

func (p *TLSConfigProvider) statFiles() ([3]time.Time, error) {
	var mtimes [3]time.Time
	for i, f := range []string{p.caFile, p.certFile, p.keyFile} {
		fi, err := os.Stat(f)
		if err != nil {
			return mtimes, fmt.Errorf("stat %s: %w", f, err)
		}
		mtimes[i] = fi.ModTime()
	}
	return mtimes, nil
}

func (p *TLSConfigProvider) doLoad() (*tlsSnapshot, [3]time.Time, error) {
	mtimes, err := p.statFiles()
	if err != nil {
		return nil, mtimes, err
	}

	caCert, err := os.ReadFile(p.caFile)
	if err != nil {
		return nil, mtimes, errors.Wrap(err, "read CA file")
	}

	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(caCert); !ok {
		return nil, mtimes, errors.New("failed to parse CA certificate")
	}

	cert, err := tls.LoadX509KeyPair(p.certFile, p.keyFile)
	if err != nil {
		return nil, mtimes, errors.Wrap(err, "load key pair")
	}

	return &tlsSnapshot{cert: cert, certPool: certPool}, mtimes, nil
}

// maybeReload stats the cert files and reloads the snapshot if any file has
// changed. On failure the previous snapshot is retained and a warning is logged.
func (p *TLSConfigProvider) maybeReload() {
	newMtimes, err := p.statFiles()
	if err != nil {
		p.log.WithError(err).Warn("TLSConfigProvider: cannot stat cert files; keeping last known good certificates")
		return
	}

	p.mu.RLock()
	unchanged := newMtimes == p.mtimes
	p.mu.RUnlock()
	if unchanged {
		return
	}

	snap, mtimes, err := p.doLoad()
	if err != nil {
		p.log.WithError(err).Warn("TLSConfigProvider: certificate reload failed; keeping last known good certificates")
		return
	}

	p.mu.Lock()
	p.snapshot = snap
	p.mtimes = mtimes
	p.mu.Unlock()
	p.log.Info("TLSConfigProvider: TLS certificates reloaded successfully")
}

// ServerConfig returns a *tls.Config that calls maybeReload on every incoming
// TLS handshake so the server always uses the freshest available certificates.
func (p *TLSConfigProvider) ServerConfig() *tls.Config {
	return &tls.Config{
		GetConfigForClient: func(info *tls.ClientHelloInfo) (*tls.Config, error) {
			if info == nil {
				return nil, errors.New("nil ClientHelloInfo")
			}
			p.maybeReload()
			p.mu.RLock()
			snap := p.snapshot
			p.mu.RUnlock()
			return serverConfigFromSnapshot(snap, p.peerName), nil
		},
	}
}

// ClientConfig returns a *tls.Config that reloads the client certificate and CA
// pool lazily on each outgoing TLS handshake. InsecureSkipVerify is set to true
// so that Go's built-in static server verification is bypassed; VerifyConnection
// performs equivalent verification using the latest CA pool from the snapshot.
func (p *TLSConfigProvider) ClientConfig() *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		Renegotiation:      tls.RenegotiateNever,
		ServerName:         p.peerName,
		InsecureSkipVerify: true, // server cert verified via VerifyConnection below
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			p.maybeReload()
			p.mu.RLock()
			cert := p.snapshot.cert
			p.mu.RUnlock()
			return &cert, nil
		},
		VerifyConnection: func(cs tls.ConnectionState) error {
			p.mu.RLock()
			certPool := p.snapshot.certPool
			p.mu.RUnlock()
			if len(cs.PeerCertificates) == 0 {
				return errors.New("server presented no certificate")
			}
			opts := x509.VerifyOptions{
				Roots:         certPool,
				DNSName:       p.peerName,
				Intermediates: x509.NewCertPool(),
			}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		},
	}
}

func serverConfigFromSnapshot(snap *tlsSnapshot, peerName string) *tls.Config {
	cfg := &tls.Config{
		MinVersion:    tls.VersionTLS13,
		Renegotiation: tls.RenegotiateNever,
		Certificates:  []tls.Certificate{snap.cert},
		ClientCAs:     snap.certPool,
		VerifyPeerCertificate: func(_ [][]byte, verifiedChains [][]*x509.Certificate) error {
			if peerName == "" {
				return nil
			}
			if len(verifiedChains) == 0 || len(verifiedChains[0]) == 0 {
				return errors.New("no valid certificate chain")
			}
			peer := verifiedChains[0][0]
			for _, name := range peer.DNSNames {
				if name == peerName {
					return nil
				}
			}
			if peer.Subject.CommonName == peerName {
				return nil
			}
			return fmt.Errorf("certificate is not signed for %q hostname", peerName)
		},
	}
	if peerName != "" {
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return cfg
}
