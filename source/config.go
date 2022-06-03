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
)

const (
	// defaultBufferSize is a default buffer size for consumed messages.
	// It must be set to avoid the problem with slow consumers.
	// See details about slow consumers here https://docs.nats.io/using-nats/developer/connecting/events/slow.
	defaultBufferSize = 512
	// defaultConsumerName is the default consumer name.
	defaultConsumerName = "conduit_push_consumer"
	// defaultDeliveryPolicy is the default delivery policy.
	defaultDeliveryPolicy = "all"
	// defaultAckPolicy is the default ack policy.
	defaultAckPolicy = "all"
)

const (
	// ConfigKeyBufferSize is a config name for a buffer size.
	ConfigKeyBufferSize = "bufferSize"
	// ConfigKeyStreamName is a config name for a stream name.
	ConfigKeyStreamName = "streamName"
	// ConfigKeyDurable is a config name for a durable name.
	ConfigKeyDurable = "durable"
	// ConfigKeyDeliveryPolicy is a config name for a delivery policy.
	ConfigKeyDeliveryPolicy = "deliveryPolicy"
	// ConfigKeyAckPolicy is a config name for an ack policy.
	ConfigKeyAckPolicy = "ackPolicy"
)

// Config holds source specific configurable values.
type Config struct {
	config.Config
	BufferSize int `key:"bufferSize" validate:"omitempty,min=64"`
	// For more detailed naming conventions see
	// https://docs.nats.io/running-a-nats-service/nats_admin/jetstream_admin/naming.
	StreamName string `key:"streamName" validate:"required_if=Mode jetstream,omitempty,alphanum,max=32"`
	// The name of the Consumer, if set will make a consumer durable,
	// allowing resuming consumption where left off.
	Durable        string `key:"durable" validate:"required_if=Mode jetstream,omitempty"`
	DeliveryPolicy string `key:"deliveryPolicy" validate:"omitempty,oneof=all new"`
	AckPolicy      string `key:"ackPolicy" validate:"omitempty,oneof=all explicit none"`
}

// Parse maps the incoming map to the Config and validates it.
func Parse(cfg map[string]string) (Config, error) {
	common, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	sourceConfig := Config{
		Config:         common,
		StreamName:     cfg[ConfigKeyStreamName],
		Durable:        cfg[ConfigKeyDurable],
		DeliveryPolicy: cfg[ConfigKeyDeliveryPolicy],
		AckPolicy:      cfg[ConfigKeyAckPolicy],
	}

	if cfg[ConfigKeyBufferSize] != "" {
		bufferSize, err := strconv.Atoi(cfg[ConfigKeyBufferSize])
		if err != nil {
			return Config{}, fmt.Errorf("\"%s\" must be an integer", ConfigKeyBufferSize)
		}

		sourceConfig.BufferSize = bufferSize
	}

	setDefaults(&sourceConfig)

	if err := validator.Validate(&sourceConfig); err != nil {
		return Config{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
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

		if cfg.DeliveryPolicy == "" {
			cfg.DeliveryPolicy = defaultDeliveryPolicy
		}

		if cfg.AckPolicy == "" {
			cfg.AckPolicy = defaultAckPolicy
		}
	}
}
