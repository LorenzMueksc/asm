// Package certs creates a certificate authority and certificates for ASM nodes and tools: for tests, demos and
// development. Production clusters use their own PKI with the same conventions:
//
//   - The common name of a certificate is the node id (eg. "3"), or the name of a tool.
//   - The organizational units are the roles: member (raft member), client (client node), admin (may remove other
//     nodes), observer (may inspect nodes and watch their events).
//   - Node certificates are used for both sides of a connection (server and client auth).
package certs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// CA is a certificate authority which issues node and tool certificates.
type CA struct {
	Cert    *x509.Certificate
	CertPEM []byte
	key     *ecdsa.PrivateKey
}

// validity of the certificates created here
const validity = 365 * 24 * time.Hour

// NewCA creates a self-signed CA with the name.
func NewCA(name string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(validity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &CA{Cert: cert, CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), key: key}, nil
}

// Pool returns a pool with the CA certificate, to verify the certificates it issued.
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	return pool
}

// Issue creates a certificate and its key for the id (a node id or a tool name) with the roles, in PEM.
// It is valid for localhost too, so tools verifying the host name accept it.
func (ca *CA) Issue(id string, roles ...string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := serialNumber()
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: id, OrganizationalUnit: roles},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

// TLSCertificate issues a certificate for the id with the roles, ready for a tls.Config.
func (ca *CA) TLSCertificate(id string, roles ...string) (tls.Certificate, error) {
	certPEM, keyPEM, err := ca.Issue(id, roles...)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

// WriteCA writes the CA certificate to dir/ca.pem and returns the path.
func (ca *CA) WriteCA(dir string) (string, error) {
	path := filepath.Join(dir, "ca.pem")
	return path, os.WriteFile(path, ca.CertPEM, 0o644)
}

// WriteFiles issues a certificate for the id with the roles and writes it to dir/<name>.pem and its key to
// dir/<name>-key.pem.
func (ca *CA) WriteFiles(dir, name, id string, roles ...string) (certFile, keyFile string, err error) {
	certPEM, keyPEM, err := ca.Issue(id, roles...)
	if err != nil {
		return "", "", err
	}
	certFile = filepath.Join(dir, name+".pem")
	keyFile = filepath.Join(dir, name+"-key.pem")
	if err := os.WriteFile(certFile, certPEM, 0o644); err != nil {
		return "", "", err
	}
	if err := os.WriteFile(keyFile, keyPEM, 0o600); err != nil {
		return "", "", err
	}
	return certFile, keyFile, nil
}

func serialNumber() (*big.Int, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, fmt.Errorf("creating a serial number: %w", err)
	}
	return serial, nil
}
