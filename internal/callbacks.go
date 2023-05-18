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

package internal

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

func ErrorHandlerCallback(ctx context.Context) nats.ErrHandler {
	return func(c *nats.Conn, sub *nats.Subscription, err error) {
		sdk.Logger(ctx).
			Error().
			Err(err).
			Str("connection_name", c.Opts.Name).
			Str("cluster_name", c.ConnectedClusterName()).
			Str("server_id", c.ConnectedServerId()).
			Str("server_name", c.ConnectedServerName()).
			Str("subscription", sub.Subject).
			Msg("nats error")
	}
}

func DisconnectErrCallback(ctx context.Context) nats.ConnErrHandler {
	return func(c *nats.Conn, err error) {
		sdk.Logger(ctx).
			Warn().
			Err(err).
			Str("connection_name", c.Opts.Name).
			Str("cluster_name", c.ConnectedClusterName()).
			Str("server_id", c.ConnectedServerId()).
			Str("server_name", c.ConnectedServerName()).
			Msg("disconnected from NATS server")
	}
}

func ReconnectCallback(ctx context.Context, extra nats.ConnHandler) nats.ConnHandler {
	return func(c *nats.Conn) {
		extra(c)
		sdk.Logger(ctx).
			Warn().
			Str("connection_name", c.Opts.Name).
			Str("cluster_name", c.ConnectedClusterName()).
			Str("server_id", c.ConnectedServerId()).
			Str("server_name", c.ConnectedServerName()).
			Msg("reconnected to NATS server")
	}
}

func ClosedCallback(ctx context.Context) nats.ConnHandler {
	return func(c *nats.Conn) {
		sdk.Logger(ctx).
			Warn().
			Str("connection_name", c.Opts.Name).
			Str("cluster_name", c.ConnectedClusterName()).
			Str("server_id", c.ConnectedServerId()).
			Str("server_name", c.ConnectedServerName()).
			Msg("connection has been closed")
	}
}

func DiscoveredServersCallback(ctx context.Context) nats.ConnHandler {
	return func(c *nats.Conn) {
		sdk.Logger(ctx).
			Warn().
			Str("connection_name", c.Opts.Name).
			Str("cluster_name", c.ConnectedClusterName()).
			Str("server_id", c.ConnectedServerId()).
			Str("server_name", c.ConnectedServerName()).
			Strs("servers", c.Servers()).
			Msg("servers have been discovered")
	}
}
