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
	"strings"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/source/jetstream"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

// Iterator defines an iterator interface.
type Iterator interface {
	HasNext(ctx context.Context) bool
	Next(ctx context.Context) (sdk.Record, error)
	Ack(ctx context.Context, position sdk.Position) error
	Stop() error
}

// Source operates source logic.
type Source struct {
	sdk.UnimplementedSource
	config   Config
	iterator Iterator
}

// NewSource creates new instance of the Source.
func NewSource() sdk.Source {
	return &Source{}
}

// Configure parses and initializes the config.
func (s *Source) Configure(ctx context.Context, cfg map[string]string) error {
	config, err := Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	s.config = config

	return nil
}

// Open opens a connection to NATS and initializes iterators.
func (s *Source) Open(ctx context.Context, position sdk.Position) error {
	opts, err := s.getConnectionOptions()
	if err != nil {
		return fmt.Errorf("get connection options: %w", err)
	}

	conn, err := nats.Connect(strings.Join(s.config.URLs, ","), opts...)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}

	s.iterator, err = jetstream.NewIterator(ctx, jetstream.IteratorParams{
		Conn:          conn,
		BufferSize:    s.config.BufferSize,
		Durable:       s.config.Durable,
		Stream:        s.config.StreamName,
		Subject:       s.config.Subject,
		SDKPosition:   position,
		DeliverPolicy: s.config.DeliverPolicy,
		AckPolicy:     s.config.AckPolicy,
	})
	if err != nil {
		return fmt.Errorf("init jetstream iterator: %w", err)
	}

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
func (s *Source) Ack(ctx context.Context, position sdk.Position) error {
	return s.iterator.Ack(ctx, position)
}

// Teardown closes connections, stops iterator.
func (s *Source) Teardown(ctx context.Context) error {
	if s.iterator != nil {
		if err := s.iterator.Stop(); err != nil {
			return fmt.Errorf("stop iterator: %w", err)
		}
	}

	return nil
}

// getConnectionOptions returns connection options based on the config.
func (s *Source) getConnectionOptions() ([]nats.Option, error) {
	var opts []nats.Option

	if s.config.ConnectionName != "" {
		opts = append(opts, nats.Name(s.config.ConnectionName))
	}

	if s.config.NKeyPath != "" {
		opt, err := nats.NkeyOptionFromSeed(s.config.NKeyPath)
		if err != nil {
			return nil, fmt.Errorf("load NKey pair: %w", err)
		}

		opts = append(opts, opt)
	}

	if s.config.CredentialsFilePath != "" {
		opts = append(opts, nats.UserCredentials(s.config.CredentialsFilePath))
	}

	if s.config.TLSClientCertPath != "" && s.config.TLSClientPrivateKeyPath != "" {
		opts = append(opts, nats.ClientCert(
			s.config.TLSClientCertPath,
			s.config.TLSClientPrivateKeyPath,
		))
	}

	if s.config.TLSRootCACertPath != "" {
		opts = append(opts, nats.RootCAs(s.config.TLSRootCACertPath))
	}

	opts = append(opts, nats.MaxReconnects(s.config.MaxReconnects))
	opts = append(opts, nats.ReconnectWait(s.config.ReconnectWait))

	return opts, nil
}
