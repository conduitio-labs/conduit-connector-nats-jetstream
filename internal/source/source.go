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
	"context"
	"fmt"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	"github.com/conduitio-labs/conduit-connector-nats-jetstream/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

// Source operates source logic.
type Source struct {
	sdk.UnimplementedSource

	config   Config
	nc       internal.NATSClient
	iterator *Iterator
}

// NewSource creates new instance of the Source.
func NewSource() sdk.Source {
	return sdk.SourceWithMiddleware(&Source{}, sdk.DefaultSourceMiddleware()...)
}

// Parameters is a map of named Parameters that describe how to configure the Source.
func (s *Source) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyURLs: {
			Default:     "",
			Required:    true,
			Description: "The connection URLs pointed to NATS instances.",
		},
		ConfigKeyStream: {
			Default:     "",
			Required:    true,
			Description: "A name of a stream from which the connector should read.",
		},
		config.KeySubject: {
			Default:     "",
			Required:    true,
			Description: "A name of a subject from which the connector should read.",
		},
		config.KeyConnectionName: {
			Default:     "",
			Required:    false,
			Description: "Optional connection name which will come in handy when it comes to monitoring.",
		},
		config.KeyNKeyPath: {
			Default:     "",
			Required:    false,
			Description: "A path pointed to a NKey pair.",
		},
		config.KeyCredentialsFilePath: {
			Default:     "",
			Required:    false,
			Description: "A path pointed to a credentials file.",
		},
		config.KeyTLSClientCertPath: {
			Default:  "",
			Required: false,
			Description: "A path pointed to a TLS client certificate, must be present " +
				"if tls.clientPrivateKeyPath field is also present.",
		},
		config.KeyTLSClientPrivateKeyPath: {
			Default:  "",
			Required: false,
			Description: "A path pointed to a TLS client private key, must be present " +
				"if tls.clientCertPath field is also present.",
		},
		config.KeyTLSRootCACertPath: {
			Default:     "",
			Required:    false,
			Description: "A path pointed to a TLS root certificate, provide if you want to verify server’s identity.",
		},
		config.KeyMaxReconnects: {
			Default:  "5",
			Required: false,
			Description: "Sets the number of reconnect attempts " +
				"that will be tried before giving up. If negative, " +
				"then it will never give up trying to reconnect.",
		},
		config.KeyReconnectWait: {
			Default:  "5s",
			Required: false,
			Description: "Sets the time to backoff after attempting a reconnect " +
				"to a server that we were already connected to previously.",
		},
		ConfigKeyBufferSize: {
			Default:     "1024",
			Required:    false,
			Description: "A buffer size for consumed messages.",
		},
		ConfigKeyDurable: {
			Default:     "",
			Required:    false,
			Description: "A consumer name.",
		},
		ConfigKeyDeliverSubject: {
			Default:     "<durable>.conduit", // FIXME it send the way that it is
			Required:    false,
			Description: "Specifies the JetStream consumer deliver subject.",
		},
		ConfigKeyDeliverPolicy: {
			Default:     "all",
			Required:    false,
			Description: "Defines where in the stream the connector should start receiving messages.",
		},
		ConfigKeyAckPolicy: {
			Default:     "explicit",
			Required:    false,
			Description: "Defines how messages should be acknowledged.",
		},
	}
}

// Configure parses and initializes the config.
func (s *Source) Configure(_ context.Context, cfg map[string]string) error {
	config, err := Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	s.config = config

	return nil
}

// Open opens a connection to NATS and initializes iterators.
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	opts, err := internal.GetConnectionOptions(s.config.Config)
	if err != nil {
		return fmt.Errorf("get connection options: %w", err)
	}

	conn, err := nats.Connect(s.config.ToURL(), opts...)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	s.nc = conn

	s.iterator, err = NewIterator(ctx, s.nc, IteratorParams{
		BufferSize:     s.config.BufferSize,
		Stream:         s.config.Stream,
		Durable:        s.config.Durable,
		DeliverSubject: s.config.DeliverSubject,
		Subject:        s.config.Subject,
		SDKPosition:    position,
		DeliverPolicy:  s.config.DeliverPolicy,
		AckPolicy:      s.config.AckPolicy,
	})
	if err != nil {
		return fmt.Errorf("init jetstream iterator: %w", err)
	}

	// Async handlers & callbacks
	conn.SetErrorHandler(internal.ErrorHandlerCallback(ctx))
	conn.SetDisconnectErrHandler(internal.DisconnectErrCallback(ctx, func(*nats.Conn) {
		if err := s.iterator.unAckAll(); err != nil {
			sdk.Logger(ctx).Error().Err(err).Send()
		}
	}))
	conn.SetReconnectHandler(internal.ReconnectCallback(ctx, func(*nats.Conn) {
		s.iterator, err = NewIterator(ctx, conn, s.iterator.params)
	}))
	conn.SetClosedHandler(internal.ClosedCallback(ctx))
	conn.SetDiscoveredServersHandler(internal.DiscoveredServersCallback(ctx))

	return nil
}

// Read fetches a record from an iterator.
// If there's no record will return sdk.ErrBackoffRetry.
func (s *Source) Read(ctx context.Context) (sdk.Record, error) {
	if !s.iterator.HasNext(ctx) {
		return sdk.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("read next record: %w", err)
	}

	return record, nil
}

// Ack acknowledges a message at the given position.
func (s *Source) Ack(_ context.Context, position sdk.Position) error {
	return s.iterator.Ack(position)
}

// Teardown closes connections, stops iterator.
func (s *Source) Teardown(context.Context) error {
	if s.iterator != nil {
		if err := s.iterator.Stop(); err != nil {
			return fmt.Errorf("stop source: %w", err)
		}
	}

	if s.nc != nil {
		// closing nats connection
		s.nc.Close()
	}

	return nil
}
