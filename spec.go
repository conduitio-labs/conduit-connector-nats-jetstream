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

package nats

import (
	"github.com/conduitio-labs/conduit-connector-nats/config"
	"github.com/conduitio-labs/conduit-connector-nats/source"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

type Spec struct{}

// Specification returns the Plugin's Specification.
func Specification() sdk.Specification {
	return sdk.Specification{
		Name:    "nats",
		Summary: "A NATS source and destination plugin for Conduit, written in Go.",
		Description: "The NATS connector is one of Conduit plugins. " +
			"It provides both, a source and a destination NATS connector.",
		Version: "v0.1.0",
		Author:  "Meroxa, Inc.",
		DestinationParams: map[string]sdk.Parameter{
			config.ConfigKeyURLs: {
				Default:     "",
				Required:    true,
				Description: "The connection URLs pointed to NATS instances.",
			},
			config.ConfigKeySubject: {
				Default:     "",
				Required:    true,
				Description: "A name of a subject from which or to which the connector should read/write.",
			},
			config.ConfigKeyMode: {
				Default:     "",
				Required:    true,
				Description: "A communication mode to be used, must be either pubsub or jetstream.",
			},
			config.ConfigKeyConnectionName: {
				Default:     "",
				Required:    false,
				Description: "Optional connection name which will come in handy when it comes to monitoring.",
			},
			config.ConfigKeyNKeyPath: {
				Default:     "",
				Required:    false,
				Description: "A path pointed to a NKey pair.",
			},
			config.ConfigKeyCredentialsFilePath: {
				Default:     "",
				Required:    false,
				Description: "A path pointed to a credentials file.",
			},
			config.ConfigKeyTLSClientCertPath: {
				Default:  "",
				Required: false,
				//nolint:lll // long description
				Description: "A path pointed to a TLS client certificate, must be present if tlsClientPrivateKeyPath field is also present.",
			},
			config.ConfigKeyTLSClientPrivateKeyPath: {
				Default:  "",
				Required: false,
				//nolint:lll // long description
				Description: "A path pointed to a TLS client private key, must be present if tlsClientCertPath field is also present.",
			},
			config.ConfigKeyTLSRootCACertPath: {
				Default:     "",
				Required:    false,
				Description: "A path pointed to a TLS root certificate, provide if you want to verify server’s identity.",
			},
			source.ConfigKeyBufferSize: {
				Default:     "512",
				Required:    false,
				Description: "A buffer size for consumed messages.",
			},
			source.ConfigKeyStreamName: {
				Default:     "",
				Required:    false,
				Description: "A stream name. Required if the connector in JetStream mode.",
			},
			source.ConfigKeyDurable: {
				Default:     "conduit_push_consumer",
				Required:    false,
				Description: "A consumer name.",
			},
			source.ConfigKeyDeliveryPolicy: {
				Default:     "all",
				Required:    false,
				Description: "A delivery policy.",
			},
			source.ConfigKeyAckPolicy: {
				Default:     "all",
				Required:    false,
				Description: "An acknowledge policy.",
			},
		},
	}
}
