// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/validator"
	"github.com/google/uuid"
)

// DefaultConnectionNamePrefix is the default connection name prefix.
const DefaultConnectionNamePrefix = "conduit-connection-"

const (
	// KeyURLs is a config name for a connection URLs.
	KeyURLs = "urls"
	// KeySubject is a config name for a subject.
	KeySubject = "subject"
	// KeyConnectionName is a config name for a connection name.
	KeyConnectionName = "connectionName"
	// KeyNKeyPath is a config name for a path pointed to a NKey pair.
	KeyNKeyPath = "nkeyPath"
	// KeyCredentialsFilePath is config name for a path pointed to a credentials file.
	KeyCredentialsFilePath = "credentialsFilePath"
	// KeyTLSClientCertPath is a config name for a path pointed to a TLS client certificate.
	KeyTLSClientCertPath = "tls.clientCertPath"
	// KeyTLSClientPrivateKeyPath is a config name for a path pointed to a TLS client private key.
	KeyTLSClientPrivateKeyPath = "tls.clientPrivateKeyPath"
	// KeyTLSRootCACertPath is a config name for a path pointed to a TLS root certificate.
	KeyTLSRootCACertPath = "tls.rootCACertPath"
)

// Config contains configurable values
// shared between source and destination NATS JetStream connector.
type Config struct {
	URLs    []string `key:"urls" validate:"required,dive,url"`
	Subject string   `key:"subject" validate:"required"`
	// ConnectionName might come in handy when it comes to monitoring and so.
	// See https://docs.nats.io/using-nats/developer/connecting/name.
	ConnectionName string `key:"connectionName"`
	// See https://docs.nats.io/using-nats/developer/connecting/nkey.
	NKeyPath string `key:"nkeyPath" validate:"omitempty,file"`
	// See https://docs.nats.io/using-nats/developer/connecting/creds.
	CredentialsFilePath string `key:"credentialsFilePath" validate:"omitempty,file"`
	// Optional parameters for a TLS encrypted connection.
	// For more details see https://docs.nats.io/using-nats/developer/connecting/tls.
	TLSClientCertPath string `key:"tls.clientCertPath" validate:"required_with=TLSClientPrivateKeyPath,omitempty,file"`
	//nolint:lll // "validate" tag can be pretty verbose
	TLSClientPrivateKeyPath string `key:"tls.clientPrivateKeyPath" validate:"required_with=TLSClientCertPath,omitempty,file"`
	TLSRootCACertPath       string `key:"tls.rootCACertPath" validate:"omitempty,file"`
}

// Parse maps the incoming map to the Config and validates it.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		URLs:                    strings.Split(cfg[KeyURLs], ","),
		Subject:                 cfg[KeySubject],
		ConnectionName:          cfg[KeyConnectionName],
		NKeyPath:                cfg[KeyNKeyPath],
		CredentialsFilePath:     cfg[KeyCredentialsFilePath],
		TLSClientCertPath:       cfg[KeyTLSClientCertPath],
		TLSClientPrivateKeyPath: cfg[KeyTLSClientPrivateKeyPath],
		TLSRootCACertPath:       cfg[KeyTLSRootCACertPath],
	}

	config.setDefaults()

	if err := validator.Validate(&config); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}

// setDefaults set default values for empty fields.
func (c *Config) setDefaults() {
	if c.ConnectionName == "" {
		c.ConnectionName = c.generateConnectionName()
	}
}

// generateConnectionName generates a random connection name.
// The connection name will be made up of the default connection name and a random UUID.
func (c *Config) generateConnectionName() string {
	return DefaultConnectionNamePrefix + uuid.New().String()
}
