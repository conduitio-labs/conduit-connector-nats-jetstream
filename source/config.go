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

package source

import (
	"fmt"

	"strconv"

	"github.com/conduitio-labs/conduit-connector-nats/config"
	"github.com/conduitio-labs/conduit-connector-nats/validator"
	"github.com/nats-io/nats.go"
)

const (
	// defaultBufferSize is a default buffer size for consumed messages.
	// It must be set to avoid the problem with slow consumers.
	// See details about slow consumers here https://docs.nats.io/using-nats/developer/connecting/events/slow.
	defaultBufferSize = 512
	// defaultConsumerName is the default consumer name.
	defaultConsumerName = "conduit_push_consumer"
	// defaultAckPolicy is the default message acknowledge policy.
	defaultAckPolicy = nats.AckExplicitPolicy
)

const (
	// ConfigKeyBufferSize is a config name for a buffer size.
	ConfigKeyBufferSize = "bufferSize"
	// ConfigKeyStreamName is a config name for a stream name.
	ConfigKeyStreamName = "streamName"
	// ConfigKeyDurable is a config name for a durable name.
	ConfigKeyDurable = "durable"
	// ConfigKeyAckPolicy is a config name for a message acknowledge policy.
	ConfigKeyAckPolicy = "ackPolicy"
)

// Config holds source specific configurable values.
type Config struct {
	config.Config
	BufferSize int `key:"bufferSize" validate:"omitempty,min=64"`
	// For more detailed naming conventions see
	// https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/naming.
	StreamName string `key:"streamName" validate:"required_if=Mode jetstream,omitempty,alphanum,max=32"`
	// Durable is the name of the Consumer, if set will make a consumer durable,
	// allowing resuming consumption where left off.
	Durable string `key:"durable" validate:"required_if=Mode jetstream,omitempty"`
	// AckPolicy defines how messages should be acknowledged.
	AckPolicy nats.AckPolicy `key:"ackPolicy" validate:"required_if=Mode jetstream,omitempty,oneof=0 1 2"`
}

// Parse maps the incoming map to the Config and validates it.
func Parse(cfg map[string]string) (Config, error) {
	common, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	sourceConfig := Config{
		Config:     common,
		StreamName: cfg[ConfigKeyStreamName],
		Durable:    cfg[ConfigKeyDurable],
	}

	if err := parseBufferSize(cfg[ConfigKeyBufferSize], &sourceConfig); err != nil {
		return Config{}, fmt.Errorf("parse buffer size: %w", err)
	}

	if err := parseAckPolicy(cfg[ConfigKeyAckPolicy], &sourceConfig); err != nil {
		return Config{}, fmt.Errorf("parse ack policy: %w", err)
	}

	setDefaults(&sourceConfig)

	if err := validator.Validate(&sourceConfig); err != nil {
		return Config{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
}

// parseBufferSize parses the bufferSize string and
// if it's not empty set cfg.BufferSize to its integer representation.
func parseBufferSize(bufferSizeStr string, cfg *Config) error {
	if bufferSizeStr != "" {
		bufferSize, err := strconv.Atoi(bufferSizeStr)
		if err != nil {
			return fmt.Errorf("\"%s\" must be an integer", ConfigKeyBufferSize)
		}

		cfg.BufferSize = bufferSize
	}

	return nil
}

// parseAckPolicy parses and converts the ackPolicy string into nats.AckPolicy.
func parseAckPolicy(ackPolicyStr string, cfg *Config) error {
	if ackPolicyStr != "" {
		ackPolicy := nats.AckPolicy(0)

		// the method requires ack policy string to be a JSON string, so we need that quotes
		if err := ackPolicy.UnmarshalJSON([]byte("\"" + ackPolicyStr + "\"")); err != nil {
			return fmt.Errorf("unmarshal ack policy: %w", err)
		}

		cfg.AckPolicy = ackPolicy
	}

	return nil
}

// setDefaults set default values for empty fields.
func setDefaults(cfg *Config) {
	if cfg.BufferSize == 0 {
		cfg.BufferSize = defaultBufferSize
	}

	if cfg.Mode == config.JetStreamConsumeMode {
		if cfg.Durable == "" {
			cfg.Durable = defaultConsumerName
		}

		if cfg.AckPolicy == 0 {
			cfg.AckPolicy = defaultAckPolicy
		}
	}
}
