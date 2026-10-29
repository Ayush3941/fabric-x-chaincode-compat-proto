// SPDX-License-Identifier: Apache-2.0

// Package testutil creates isolated MSPs and enrollment identities for tests.
package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"compatibility_service/pkg/auth"
	"github.com/hyperledger/fabric-x-sdk/identity"
)

type MSP struct {
	Config auth.MSPConfig
	CA     *x509.Certificate
	Key    *ecdsa.PrivateKey
}

type Actor struct {
	Signer      identity.Signer
	Certificate *x509.Certificate
	CertPath    string
}

func NewMSP(t *testing.T, name string) *MSP {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{SerialNumber: serial(t), Subject: pkix.Name{CommonName: name + " CA"}, NotBefore: now.Add(-24 * time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	WriteFile(t, filepath.Join(dir, "cacerts", "ca.pem"), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	config := "NodeOUs:\n  Enable: true\n"
	for _, role := range []string{"Client", "Peer", "Admin", "Orderer"} {
		lower := map[string]string{"Client": "client", "Peer": "peer", "Admin": "admin", "Orderer": "orderer"}[role]
		config += fmt.Sprintf("  %sOUIdentifier:\n    Certificate: cacerts/ca.pem\n    OrganizationalUnitIdentifier: %s\n", role, lower)
	}
	WriteFile(t, filepath.Join(dir, "config.yaml"), []byte(config))
	return &MSP{Config: auth.MSPConfig{MSPID: name, MSPDir: dir}, CA: ca, Key: key}
}

func (m *MSP) Issue(t *testing.T, role string, notBefore, notAfter time.Time) Actor {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: serial(t), Subject: pkix.Name{CommonName: role, OrganizationalUnit: []string{role}}, NotBefore: notBefore, NotAfter: notAfter, KeyUsage: x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, template, m.CA, &key.PublicKey, m.Key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	WriteFile(t, filepath.Join(dir, "keystore", "key_sk"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	certPath := filepath.Join(dir, "signcerts", "cert.pem")
	WriteFile(t, certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	signer, err := identity.SignerFromMSP(dir, m.Config.MSPID)
	if err != nil {
		t.Fatal(err)
	}
	return Actor{Signer: signer, Certificate: cert, CertPath: certPath}
}

func WriteFile(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func serial(t *testing.T) *big.Int {
	t.Helper()
	n, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	return n.Add(n, big.NewInt(1))
}
