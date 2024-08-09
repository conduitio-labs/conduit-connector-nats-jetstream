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
	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	"strings"

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

func (d *Destination) Parameters() config.Parameters {
	return Config{}.Parameters()
}

// Configure parses and initializes the config.
func (d *Destination) Configure(ctx context.Context, cfg config.Config) error {
	parsedCfg, err := ParseConfig(ctx, cfg, NewDestination().Parameters())
	if err != nil {
		return err
	}

	d.config = parsedCfg

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
	conn.SetDisconnectErrHandler(internal.DisconnectErrCallback(ctx, func(*nats.Conn) {}))
	conn.SetReconnectHandler(internal.ReconnectCallback(ctx, func(*nats.Conn) {
		d.writer, err = NewWriter(writerParams{
			nc:            d.nc,
			subject:       d.config.Subject,
			retryWait:     d.config.RetryWait,
			retryAttempts: d.config.RetryAttempts,
		})
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
func (d *Destination) Write(ctx context.Context, records []opencdc.Record) (int, error) {
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
			if err := d.writer.write(ctx, record); err != nil {
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
	if d.nc != nil {
		d.nc.Close()
	}

	return nil
}
