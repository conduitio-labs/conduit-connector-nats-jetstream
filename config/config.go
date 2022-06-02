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

	"github.com/conduitio-labs/conduit-connector-nats/validator"
)

// ConsumeMode represents a communication model.
type ConsumeMode string

const (
	// PubSubConsumeMode is a Pub/Sub communication model.
	PubSubConsumeMode ConsumeMode = "pubsub"
	// JetStreamConsumeMode is a JetStream communication model.
	JetStreamConsumeMode ConsumeMode = "jetstream"
)

const (
	// ConfigKeyURLs is a config name for a connection URLs.
	ConfigKeyURLs = "urls"
	// ConfigKeySubject is a config name for a subject.
	ConfigKeySubject = "subject"
	// ConfigKeyMode is a config name for a mode.
	ConfigKeyMode = "mode"
	// ConfigKeyConnectionName is a config name for a connection name.
	ConfigKeyConnectionName = "connectionName"
	// ConfigKeyNKeyPath is a config name for a path pointed to a NKey pair.
	ConfigKeyNKeyPath = "nkeyPath"
	// ConfigKeyCredentialsFilePath is config name for a path pointed to a credentials file.
	ConfigKeyCredentialsFilePath = "credentialsFilePath"
	// ConfigKeyTLSClientCertPath is a config name for a path pointed to a TLS client certificate.
	ConfigKeyTLSClientCertPath = "tlsClientCertPath"
	// ConfigKeyTLSClientPrivateKeyPath is a config name for a path pointed to a TLS client private key.
	ConfigKeyTLSClientPrivateKeyPath = "tlsClientPrivateKeyPath"
	// ConfigKeyTLSRootCACertPath is a config name for a path pointed to a TLS root certificate.
	ConfigKeyTLSRootCACertPath = "tlsRootCACertPath"
)

// Config contains configurable values
// shared between source and destination NATS connector.
type Config struct {
	URLs    []string    `key:"urls" validate:"required,dive,url"`
	Subject string      `key:"subject" validate:"required"`
	Mode    ConsumeMode `key:"mode" validate:"required,oneof=pubsub jetstream"`
	// ConnectionName might come in handy when it comes to monitoring and so.
	// See https://docs.nats.io/using-nats/developer/connecting/name.
	ConnectionName string `key:"connectionName"`
	// See https://docs.nats.io/using-nats/developer/connecting/nkey.
	NKeyPath string `key:"nkeyPath" validate:"omitempty,file"`
	// See https://docs.nats.io/using-nats/developer/connecting/creds.
	CredentialsFilePath string `key:"credentialsFilePath" validate:"omitempty,file"`
	// Optional parameters for a TLS encrypted connection.
	// For more details see https://docs.nats.io/using-nats/developer/connecting/tls.
	TLSClientCertPath string `key:"tlsClientCertPath" validate:"required_with=TLSClientPrivateKeyPath,omitempty,file"`
	//nolint:lll // "validate" tag can be pretty verbose
	TLSClientPrivateKeyPath string `key:"tlsClientPrivateKeyPath" validate:"required_with=TLSClientCertPath,omitempty,file"`
	TLSRootCACertPath       string `key:"tlsRootCACertPath" validate:"omitempty,file"`
}

// Parse maps the incoming map to the Config and validates it.
func Parse(cfg map[string]string) (Config, error) {
	config := Config{
		URLs:                    strings.Split(cfg[ConfigKeyURLs], ","),
		Subject:                 cfg[ConfigKeySubject],
		Mode:                    ConsumeMode(cfg[ConfigKeyMode]),
		ConnectionName:          cfg[ConfigKeyConnectionName],
		NKeyPath:                cfg[ConfigKeyNKeyPath],
		CredentialsFilePath:     cfg[ConfigKeyCredentialsFilePath],
		TLSClientCertPath:       cfg[ConfigKeyTLSClientCertPath],
		TLSClientPrivateKeyPath: cfg[ConfigKeyTLSClientPrivateKeyPath],
		TLSRootCACertPath:       cfg[ConfigKeyTLSRootCACertPath],
	}

	if err := validator.Validate(&config); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}

	return config, nil
}
