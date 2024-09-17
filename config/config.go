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
	"errors"
	"net/url"
	"strings"
	"time"
)

// Config contains configurable values
// shared between source and destination NATS JetStream connector.
type Config struct {
	// URLs defines connection URLs.
	URLs []string `json:"urls" validate:"required"`
	// Subject is the subject name.
	Subject string `json:"subject" validate:"required"`
	// ConnectionName is the name of the connection that the connector establishes.
	// Setting the connection is useful when monitoring the connector.
	// The default value is the connector ID.
	// See https://docs.nats.io/using-nats/developer/connecting/name.
	ConnectionName string `json:"connectionName"`
	// NKeyPath is the path to an NKey.
	// See https://docs.nats.io/using-nats/developer/connecting/nkey.
	NKeyPath string `json:"nkeyPath"`
	// CredentialsFilePath is the path to a credentials file.
	// See https://docs.nats.io/using-nats/developer/connecting/creds.
	CredentialsFilePath string `json:"credentialsFilePath"`
	// MaxReconnects sets the number of reconnect attempts that will be
	// tried before giving up. If negative, then it will never give up
	// trying to reconnect.
	MaxReconnects int `json:"maxReconnects" default:"5"`
	// ReconnectWait is the wait time between reconnect attempts.
	ReconnectWait time.Duration `json:"reconnectWait" default:"5s"`

	ConfigTLS
}

func (c *Config) Validate() error {
	var errs []error

	// Validate URLs
	for _, urlStr := range c.URLs {
		if _, err := url.ParseRequestURI(urlStr); err != nil {
			errs = append(errs, err)
		}
	}

	// Validate TLS configuration
	if err := c.ConfigTLS.Validate(); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}

// ToURL joins the Config's URLs strings together and returns them as one string.
func (c *Config) ToURL() string {
	return strings.Join(c.URLs, ",")
}

//nolint:revive // Consistent with Config above
type ConfigTLS struct {
	// TLSClientCertPath is the path to a client certificate.
	// For more details see https://docs.nats.io/using-nats/developer/connecting/tls.
	TLSClientCertPath string `json:"tls.clientCertPath"`
	// TLSClientPrivateKeyPath is the path to a private key.
	// For more details see https://docs.nats.io/using-nats/developer/connecting/tls.
	TLSClientPrivateKeyPath string `json:"tls.clientPrivateKeyPath"`
	// TLSRootCACertPath is the path to a root CA certificate.
	TLSRootCACertPath string `json:"tls.rootCACertPath"`
}

func (cfg ConfigTLS) Validate() error {
	switch {
	case cfg.TLSClientCertPath == "" && cfg.TLSClientPrivateKeyPath == "":
		// Both fields are empty, this is valid, so return nil.
		return nil
	case cfg.TLSClientCertPath != "" && cfg.TLSClientPrivateKeyPath != "":
		// Both fields are non-empty, this is valid, so return nil.
		return nil
	case cfg.TLSClientCertPath == "":
		return errors.New("TLSClientCertPath is missing")
	case cfg.TLSClientPrivateKeyPath == "":
		return errors.New("TLSClientPrivateKeyPath is missing")
	}

	return nil
}
