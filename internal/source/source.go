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

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/internal"
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
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
func (s *Source) Parameters() config.Parameters {
	return Config{}.Parameters()
}

// Configure parses and initializes the config.
func (s *Source) Configure(ctx context.Context, cfg config.Config) error {
	parsedCfg, err := ParseConfig(ctx, cfg, NewSource().Parameters())
	if err != nil {
		return err
	}

	s.config = parsedCfg

	return nil
}

// Open opens a connection to NATS and initializes iterators.
func (s *Source) Open(ctx context.Context, position opencdc.Position) error {
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
		DeliverPolicy:  s.config.NATSDeliverPolicy(),
		AckPolicy:      s.config.NATSAckPolicy(),
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
func (s *Source) Read(ctx context.Context) (opencdc.Record, error) {
	if !s.iterator.HasNext(ctx) {
		return opencdc.Record{}, sdk.ErrBackoffRetry
	}

	record, err := s.iterator.Next(ctx)
	if err != nil {
		return opencdc.Record{}, fmt.Errorf("read next record: %w", err)
	}

	return record, nil
}

// Ack acknowledges a message at the given position.
func (s *Source) Ack(_ context.Context, position opencdc.Position) error {
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
