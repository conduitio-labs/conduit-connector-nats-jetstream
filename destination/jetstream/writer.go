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

package jetstream

import (
	"context"
	"fmt"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

// Writer implements a JetStream writer.
// It writes messages asynchronously.
type Writer struct {
	sync.Mutex

	conn        *nats.Conn
	subject     string
	jetstream   nats.JetStreamContext
	publishOpts []nats.PubOpt
}

// WriterParams is an incoming params for the NewWriter function.
type WriterParams struct {
	Conn          *nats.Conn
	Subject       string
	RetryWait     time.Duration
	RetryAttempts int
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, params WriterParams) (*Writer, error) {
	jetstream, err := params.Conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("get jetstream context: %w", err)
	}

	return &Writer{
		conn:        params.Conn,
		subject:     params.Subject,
		jetstream:   jetstream,
		publishOpts: getPublishOptions(params),
	}, nil
}

// Write synchronously writes a record.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	_, err := w.jetstream.Publish(w.subject, record.Payload.After.Bytes(), w.publishOpts...)
	if err != nil {
		return fmt.Errorf("publish sync: %w", err)
	}

	return nil
}

// Close closes the underlying NATS connection.
func (w *Writer) Close(ctx context.Context) error {
	if w.conn != nil {
		w.conn.Close()
	}

	return nil
}

// getPublishOptions returns a NATS publish options based on the provided WriterParams.
func getPublishOptions(params WriterParams) []nats.PubOpt {
	var opts []nats.PubOpt

	if params.RetryWait != 0 {
		opts = append(opts, nats.RetryWait(params.RetryWait))
	}

	if params.RetryAttempts != 0 {
		opts = append(opts, nats.RetryAttempts(params.RetryAttempts))
	}

	return opts
}
