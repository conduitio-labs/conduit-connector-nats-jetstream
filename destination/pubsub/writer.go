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

package pubsub

import (
	"context"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

// Writer implements a PubSub writer.
// It writes messages synchronously. It doesn't support batching/async writing.
type Writer struct {
	conn    *nats.Conn
	subject string
}

// WriterParams is an incoming params for the NewWriter function.
type WriterParams struct {
	Conn    *nats.Conn
	Subject string
}

// NewWriter creates new instance of the Writer.
func NewWriter(ctx context.Context, params WriterParams) (*Writer, error) {
	return &Writer{
		conn:    params.Conn,
		subject: params.Subject,
	}, nil
}

// Write writes directly and synchronously a record to a subject.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	return w.conn.Publish(w.subject, record.Payload.Bytes())
}

// WriteAsync is not implemented for the PubSub Writer.
func (w *Writer) WriteAsync(ctx context.Context, record sdk.Record, ackFunc sdk.AckFunc) (batchIsFull bool, err error) {
	return false, sdk.ErrUnimplemented
}

// Flush is not implemented for the PubSub Writer.
func (w *Writer) Flush(ctx context.Context) error {
	return sdk.ErrUnimplemented
}

// Close closes the underlying NATS connection.
func (w *Writer) Close(ctx context.Context) error {
	if w.conn != nil {
		w.conn.Close()
	}

	return nil
}
