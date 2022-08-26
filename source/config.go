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
	"strings"

	"strconv"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	"github.com/conduitio-labs/conduit-connector-nats-jetstream/validator"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
	// defaultBufferSize is a default buffer size for consumed messages.
	// It must be set to avoid the problem with slow consumers.
	// See details about slow consumers here https://docs.nats.io/using-nats/developer/connecting/events/slow.
	defaultBufferSize = 1024
	// defaultDurablePrefix is the default consumer name prefix.
	defaultDurablePrefix = "conduit-"
	// defaultDeliverSubjectSuffix is the default deliver subject suffix.
	defaultDeliverSubjectSuffix = "conduit"
	// defaultDeliverPolicy is the default message deliver policy.
	defaultDeliverPolicy = nats.DeliverAllPolicy
	// defaultAckPolicy is the default message acknowledge policy.
	defaultAckPolicy = nats.AckExplicitPolicy
)

const (
	// ConfigKeyBufferSize is a config name for a buffer size.
	ConfigKeyBufferSize = "bufferSize"
	// ConfigKeyDeliverSubject is a config name for a deliver subject.
	ConfigKeyDeliverSubject = "deliverSubject"
	// ConfigKeyDurable is a config name for a durable name.
	ConfigKeyDurable = "durable"
	// ConfigKeyDeliverPolicy is a config name for a message deliver policy.
	ConfigKeyDeliverPolicy = "deliverPolicy"
	// ConfigKeyAckPolicy is a config name for a message acknowledge policy.
	ConfigKeyAckPolicy = "ackPolicy"
)

// Config holds source specific configurable values.
type Config struct {
	config.Config

	BufferSize int `key:"bufferSize" validate:"omitempty,min=64"`
	// Durable is the name of the Consumer, if set will make a consumer durable,
	// allowing resuming consumption where left off.
	Durable string `key:"durable" validate:"required"`
	// DeliverSubject specifies the JetStream consumer deliver subject.
	DeliverSubject string `json:"deliverSubject" validate:"required"`
	// DeliverPolicy defines where in the stream the connector should start receiving messages.
	DeliverPolicy nats.DeliverPolicy `key:"deliverPolicy" validate:"oneof=0 2"`
	// AckPolicy defines how messages should be acknowledged.
	AckPolicy nats.AckPolicy `key:"ackPolicy" validate:"oneof=0 1 2"`
}

// Parse maps the incoming map to the Config and validates it.
func Parse(cfg map[string]string) (Config, error) {
	common, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	sourceConfig := Config{
		Config:         common,
		DeliverSubject: cfg[ConfigKeyDeliverSubject],
		Durable:        cfg[ConfigKeyDurable],
	}

	if err := sourceConfig.parseBufferSize(cfg[ConfigKeyBufferSize]); err != nil {
		return Config{}, fmt.Errorf("parse buffer size: %w", err)
	}

	if err := sourceConfig.parseDeliverPolicy(cfg[ConfigKeyDeliverPolicy]); err != nil {
		return Config{}, fmt.Errorf("parse deliver policy: %w", err)
	}

	if err := sourceConfig.parseAckPolicy(cfg[ConfigKeyAckPolicy]); err != nil {
		return Config{}, fmt.Errorf("parse ack policy: %w", err)
	}

	sourceConfig.setDefaults()

	if err := validator.Validate(&sourceConfig); err != nil {
		return Config{}, fmt.Errorf("validate source config: %w", err)
	}

	return sourceConfig, nil
}

// parseBufferSize parses the bufferSize string and
// if it's not empty set cfg.BufferSize to its integer representation.
func (c *Config) parseBufferSize(bufferSizeStr string) error {
	if bufferSizeStr != "" {
		bufferSize, err := strconv.Atoi(bufferSizeStr)
		if err != nil {
			return fmt.Errorf("\"%s\" must be an integer", ConfigKeyBufferSize)
		}

		c.BufferSize = bufferSize
	}

	return nil
}

// parseDeliverPolicy parses and converts the deliverPolicy string into nats.DeliverPolicy.
func (c *Config) parseDeliverPolicy(deliverPolicyStr string) error {
	switch strings.ToLower(deliverPolicyStr) {
	case "all", "":
		c.DeliverPolicy = nats.DeliverAllPolicy
	case "new":
		c.DeliverPolicy = nats.DeliverNewPolicy
	default:
		return fmt.Errorf("invalid deliver policy %q", deliverPolicyStr)
	}

	return nil
}

// parseAckPolicy parses and converts the ackPolicy string into nats.AckPolicy.
func (c *Config) parseAckPolicy(ackPolicyStr string) error {
	switch strings.ToLower(ackPolicyStr) {
	case "explicit", "":
		c.AckPolicy = nats.AckExplicitPolicy
	case "none":
		c.AckPolicy = nats.AckNonePolicy
	case "all":
		c.AckPolicy = nats.AckAllPolicy
	default:
		return fmt.Errorf("invalid ack policy %q", ackPolicyStr)
	}

	return nil
}

// setDefaults set default values for empty fields.
func (c *Config) setDefaults() {
	if c.BufferSize == 0 {
		c.BufferSize = defaultBufferSize
	}

	if c.Durable == "" {
		c.Durable = c.generateDurableName()
	}

	if c.DeliverSubject == "" {
		c.DeliverSubject = c.generateDeliverSubject()
	}
}

// generateDurableName generates a random durable (consumer) name.
// The durable name will be made up of the default durable prefix and a random UUID.
func (c *Config) generateDurableName() string {
	return defaultDurablePrefix + uuid.New().String()
}

// generateDeliverSubject generates a deliver subject in the format <durable>.conduit.
func (c *Config) generateDeliverSubject() string {
	return fmt.Sprintf("%s.%s", c.Durable, defaultDeliverSubjectSuffix)
}
