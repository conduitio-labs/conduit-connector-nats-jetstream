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

package destination

import (
	"context"
	"fmt"
	"strings"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	"github.com/conduitio-labs/conduit-connector-nats-jetstream/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

// Destination NATS Connector persists records to a NATS subject or stream.
type Destination struct {
	sdk.UnimplementedDestination

	nc     internal.NATSClient
	config Config
	writer *Writer
}

// NewDestination creates new instance of the Destination.
func NewDestination() sdk.Destination {
	return sdk.DestinationWithMiddleware(&Destination{}, sdk.DefaultDestinationMiddleware()...)
}

func (d *Destination) Parameters() map[string]sdk.Parameter {
	return map[string]sdk.Parameter{
		config.KeyURLs: {
			Default:     "",
			Required:    true,
			Description: "The connection URLs pointed to NATS instances.",
		},
		config.KeySubject: {
			Default:     "",
			Required:    true,
			Description: "A name of a subject to which the connector should write.",
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
			//nolint:lll // long description
			Description: "A path pointed to a TLS client certificate, must be present if tls.clientPrivateKeyPath field is also present.",
		},
		config.KeyTLSClientPrivateKeyPath: {
			Default:  "",
			Required: false,
			//nolint:lll // long description
			Description: "A path pointed to a TLS client private key, must be present if tls.clientCertPath field is also present.",
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
		ConfigKeyRetryWait: {
			Default:     "5s",
			Required:    false,
			Description: "Sets the timeout to wait for a message to be resent, if send fails.",
		},
		ConfigKeyRetryAttempts: {
			Default:     "3",
			Required:    false,
			Description: "Sets a numbers of attempts to send a message, if send fails.",
		},
	}
}

// Configure parses and initializes the config.
func (d *Destination) Configure(_ context.Context, cfg map[string]string) error {
	config, err := Parse(cfg)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	d.config = config

	return nil
}

// Open makes sure everything is prepared to receive records.
func (d *Destination) Open(ctx context.Context) error {
	opts, err := internal.GetConnectionOptions(d.config.Config)
	if err != nil {
		return fmt.Errorf("get connection options: %s", err)
	}

	conn, err := nats.Connect(strings.Join(d.config.URLs, ","), opts...)
	if err != nil {
		return fmt.Errorf("connect to NATS: %w", err)
	}
	d.nc = conn

	// Async handlers & callbacks
	conn.SetErrorHandler(internal.ErrorHandlerCallback(ctx))
	conn.SetDisconnectErrHandler(internal.DisconnectErrCallback(ctx, func(c *nats.Conn) {
		d.writer.stopWrites()
	}))
	conn.SetReconnectHandler(internal.ReconnectCallback(ctx, func(c *nats.Conn) {
		d.writer, err = NewWriter(writerParams{
			nc:            d.nc,
			subject:       d.config.Subject,
			retryWait:     d.config.RetryWait,
			retryAttempts: d.config.RetryAttempts,
		})
		d.writer.startWrites()
	}))
	conn.SetClosedHandler(internal.ClosedCallback(ctx))
	conn.SetDiscoveredServersHandler(internal.DiscoveredServersCallback(ctx))

	d.writer, err = NewWriter(writerParams{
		nc:            d.nc,
		subject:       d.config.Subject,
		retryWait:     d.config.RetryWait,
		retryAttempts: d.config.RetryAttempts,
	})
	if err != nil {
		return fmt.Errorf("init jetstream writer: %w", err)
	}

	return nil
}

// Write writes a record into a Destination.
func (d *Destination) Write(ctx context.Context, records []sdk.Record) (int, error) {
	recorded := 0
	for _, record := range records {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			sdk.Logger(ctx).Debug().
				Int("record total", len(records)).
				Int("record recorded", recorded).
				Err(err).
				Msg("write stopped by context before having all records recorded")

			return recorded, err
		default:
			if err := d.writer.Write(record); err != nil {
				sdk.Logger(ctx).Debug().
					Int("record total", len(records)).
					Int("record recorded", recorded).
					Err(err).
					Send()

				return recorded, err
			}
		}
		recorded++
	}

	return recorded, nil
}

// Teardown gracefully closes connections.
func (d *Destination) Teardown(context.Context) error {
	if d.writer != nil {
		d.writer.stopWrites()
	}

	if d.nc != nil {
		d.nc.Close()
	}

	return nil
}
