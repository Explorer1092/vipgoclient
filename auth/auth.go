// Copyright 2019 zhvala
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package auth

import (
	"crypto/tls"
)

const (
	// AuthToken auth client by token
	AuthToken = "token"
	// AuthTLS auth client by client side cert
	AuthTLS = "tls"
	// AuthAthenz auth client by athenz
	AuthAthenz = "athenz"
)

// Authentication opaque interface that represents the authentication credentials
type Authentication interface {
	GetAuthName() string
	GetHTTPHeaders() string
	GetCommandData() string
}

type tokenAuth struct {
	TokenSupplier func() string
}

func (ta *tokenAuth) GetAuthName() string {
	return AuthToken
}

func (ta *tokenAuth) GetHTTPHeaders() string {
	return "Authorization: Bearer " + ta.TokenSupplier()
}

func (ta *tokenAuth) GetCommandData() string {
	return ta.TokenSupplier()
}

// NewAuthenticationToken create new Authentication provider with specified auth token
func NewAuthenticationToken(token string) (Authentication, error) {
	return &tokenAuth{
		TokenSupplier: func() string { return token },
	}, nil
}

// NewAuthenticationTokenSupplier create new Authentication provider with specified auth token supplier
func NewAuthenticationTokenSupplier(tokenSupplier func() string) (Authentication, error) {
	return &tokenAuth{
		TokenSupplier: tokenSupplier,
	}, nil
}

type TLSAuthentication struct {
	cliCert tls.Certificate
}

func (ta *TLSAuthentication) GetAuthName() string {
	return AuthTLS
}

func (ta *TLSAuthentication) GetHTTPHeaders() string {
	return ""
}

func (ta *TLSAuthentication) GetCommandData() string {
	return ""
}

func (ta *TLSAuthentication) ConfigClientAuth(cfg *tls.Config) {
	if cfg != nil {
		cfg.Certificates = append(cfg.Certificates, ta.cliCert)
	}
}

// NewAuthenticationTLS create new Authentication provider with specified TLS certificate and private key
func NewAuthenticationTLS(certificatePath string, privateKeyPath string) (Authentication, error) {
	cert, err := tls.LoadX509KeyPair(certificatePath, privateKeyPath)
	if err != nil {
		return nil, err
	}

	return &TLSAuthentication{
		cliCert: cert,
	}, nil
}

type ZTSClient interface {
	GetHeader() string
	GetRoleToken() string
}

type athenzAuth struct {
	ztsCli ZTSClient
}

func (aa *athenzAuth) GetAuthName() string {
	return AuthAthenz
}

func (aa *athenzAuth) GetHTTPHeaders() string {
	return aa.ztsCli.GetHeader() + ": " + aa.ztsCli.GetRoleToken()
}

func (aa *athenzAuth) GetCommandData() string {
	return aa.ztsCli.GetRoleToken()
}

// NewAuthenticationAthenz create new Athenz Authentication provider with configuration in JSON form
func NewAuthenticationAthenz(ztsCli ZTSClient) (Authentication, error) {
	return &athenzAuth{
		ztsCli: ztsCli,
	}, nil
}
