package common

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
			Msg("disconnected from NATS server")
	}
}

func ReconnectCallback(ctx context.Context) nats.ConnHandler {
	return func(c *nats.Conn) {
		sdk.Logger(ctx).
			Warn().
			Str("connection_name", c.Opts.Name).
			Msg("reconnected to NATS server")
	}
}

func ClosedCallback(ctx context.Context) nats.ConnHandler {
	return func(c *nats.Conn) {
		sdk.Logger(ctx).
			Warn().
			Str("connection_name", c.Opts.Name).
			Msg("connection has been closed")
	}
}
