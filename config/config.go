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
	"strconv"
	"strings"
	"time"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/validator"
	"github.com/google/uuid"
)

const (
	// DefaultConnectionNamePrefix is the default connection name prefix.
	DefaultConnectionNamePrefix = "conduit-connection-"
	// DefaultMaxReconnects is the default max reconnects count.
	DefaultMaxReconnects = 5
	// DefaultReconnectWait is the default reconnect wait timeout.
	DefaultReconnectWait = time.Second * 5
)

const (
	// KeyURLs is a config name for a connection URLs.
	KeyURLs = "urls"
	// KeyStream is a config name for a stream.
	KeyStream = "stream"
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
	// KeyMaxReconnects is a config name for a max reconnects.
	KeyMaxReconnects = "maxReconnects"
	// KeyReconnectWait is a config name for reconnect wait duration.
	KeyReconnectWait = "reconnectWait"
)

// Config contains configurable values
// shared between source and destination NATS JetStream connector.
type Config struct {
	URLs    []string `key:"urls" validate:"required,dive,url"`
	Stream  string   `key:"stream" validate:"required"`
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
	// MaxReconnect sets the number of reconnect attempts that will be
	// tried before giving up. If negative, then it will never give up
	// trying to reconnect.
	MaxReconnects int `key:"maxReconnects"`
	// ReconnectWait sets the time to backoff after attempting a reconnect
	// to a server that we were already connected to previously.
	ReconnectWait time.Duration `key:"reconnectWait"`
}

// Parse maps the incoming map to the Config and validates it.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		URLs:                    strings.Split(cfg[KeyURLs], ","),
		Stream:                  cfg[KeyStream],
		Subject:                 cfg[KeySubject],
		NKeyPath:                cfg[KeyNKeyPath],
		CredentialsFilePath:     cfg[KeyCredentialsFilePath],
		TLSClientCertPath:       cfg[KeyTLSClientCertPath],
		TLSClientPrivateKeyPath: cfg[KeyTLSClientPrivateKeyPath],
		TLSRootCACertPath:       cfg[KeyTLSRootCACertPath],
		MaxReconnects:           DefaultMaxReconnects,
		ReconnectWait:           DefaultReconnectWait,
	}

	if connectionName, ok := cfg[KeyConnectionName]; ok && connectionName != "" {
		config.ConnectionName = connectionName
	} else {
		config.ConnectionName = generateConnectionName()
	}

	if err := config.parseMaxReconnects(cfg[KeyMaxReconnects]); err != nil {
		return Config{}, fmt.Errorf("parse max reconnects: %w", err)
	}

	if err := config.parseReconnectWait(cfg[KeyReconnectWait]); err != nil {
		return Config{}, fmt.Errorf("parse reconnect wait: %w", err)
	}

	if err := validator.Validate(&config); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}

// ToURL joins the Config's URLs strings together and returns them as one string.
func (c *Config) ToURL() string {
	return strings.Join(c.URLs, ",")
}

// parseMaxReconnects parses the maxReconnects string and
// if it's not empty set cfg.MaxReconnects to its integer representation.
func (c *Config) parseMaxReconnects(maxReconnectsStr string) error {
	if maxReconnectsStr != "" {
		maxReconnects, err := strconv.Atoi(maxReconnectsStr)
		if err != nil {
			return fmt.Errorf("\"%s\" must be an integer", KeyMaxReconnects)
		}

		c.MaxReconnects = maxReconnects
	}

	return nil
}

// parseReconnectWait parses the reconnectWait string and
// if it's not empty set cfg.ReconnectWait to its time.Duration representation.
func (c *Config) parseReconnectWait(reconnectWaitStr string) error {
	if reconnectWaitStr != "" {
		reconnectWait, err := time.ParseDuration(reconnectWaitStr)
		if err != nil {
			return fmt.Errorf("\"%s\" must be a valid duration", KeyReconnectWait)
		}

		c.ReconnectWait = reconnectWait
	}

	return nil
}

// generateConnectionName generates a random connection name.
// The connection name will be made up of the default connection name and a random UUID.
func generateConnectionName() string {
	return DefaultConnectionNamePrefix + uuid.NewString()
}
