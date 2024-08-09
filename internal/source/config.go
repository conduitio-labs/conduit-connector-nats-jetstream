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

//go:generate paramgen -output=paramgen.go Config

package source

import (
	"context"
	"fmt"
	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	commonscfg "github.com/conduitio/conduit-commons/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

const (
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
	// ConfigKeyDeliverSubject is a config name for a deliver subject.
	ConfigKeyDeliverSubject = "deliverSubject"
	// ConfigKeyStream is a config name for a stream name.
	ConfigKeyStream = "stream"
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

	// BufferSize is a buffer size for consumed messages.
	// It must be set to avoid the problem with slow consumers.
	// See details about slow consumers here https://docs.nats.io/using-nats/developer/connecting/events/slow.
	BufferSize int `json:"bufferSize" validate:"greater-than=64" default:"1024"`
	// Stream is the name of the Stream to be consumed.
	Stream string `json:"stream" validate:"required"`
	// Durable is the name of the Consumer, if set will make a consumer durable,
	// allowing resuming consumption where left off.
	Durable string `json:"durable"`
	// DeliverSubject specifies the JetStream consumer deliver subject.
	DeliverSubject string `json:"deliverSubject"`
	// DeliverPolicy defines where in the stream the connector should start receiving messages.
	DeliverPolicy string `json:"deliverPolicy" validate:"inclusion=all|new" default:"all"`
	// AckPolicy defines how messages should be acknowledged.
	AckPolicy string `json:"ackPolicy" validate:"inclusion=explicit|none|all" default:"explicit"`
}

func ParseConfig(ctx context.Context, cfg commonscfg.Config, parameters commonscfg.Parameters) (Config, error) {
	durable := "conduit-connector-nats-jetstream-" + uuid.NewString()
	// set defaults
	parsedCfg := Config{
		Config: config.Config{
			// todo get connector ID from ctx
			ConnectionName: "connector-id",
		},
		Durable:        durable,
		DeliverSubject: fmt.Sprintf("%s.%s", durable, defaultDeliverSubjectSuffix),
	}

	err := sdk.Util.ParseConfig(ctx, cfg, &parsedCfg, parameters)
	if err != nil {
		return Config{}, err
	}

	err = parsedCfg.Validate()
	if err != nil {
		return Config{}, err
	}

	return parsedCfg, nil
}

func (c Config) NATSDeliverPolicy() nats.DeliverPolicy {
	switch c.DeliverPolicy {
	case "all", "":
		return nats.DeliverAllPolicy
	case "new":
		return nats.DeliverNewPolicy
	default:
		// shouldn't happen, because the SDK should limit the options to only the valid ones
		panic(fmt.Errorf("invalid deliver policy %q", c.DeliverPolicy))
	}
}

func (c Config) NATSAckPolicy() nats.AckPolicy {
	switch c.AckPolicy {
	case "explicit":
		return nats.AckExplicitPolicy
	case "none":
		return nats.AckNonePolicy
	case "all":
		return nats.AckAllPolicy
	default:
		// shouldn't happen, because the SDK should limit the options to only the valid ones
		panic(fmt.Errorf("invalid ack policy %q", c.AckPolicy))
	}
}
