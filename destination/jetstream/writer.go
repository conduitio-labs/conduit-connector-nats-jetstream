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

// message holds a message data and an ack function.
type message struct {
	data    []byte
	ackFunc sdk.AckFunc
}

// Writer implements a JetStream writer.
// It writes messages asynchronously.
type Writer struct {
	sync.Mutex

	conn          *nats.Conn
	subject       string
	jetstream     nats.JetStreamContext
	messages      []*message
	batchSize     int
	retryWait     time.Duration
	retryAttempts int
}

// WriterParams is an incoming params for the NewWriter function.
type WriterParams struct {
	Conn          *nats.Conn
	Subject       string
	BatchSize     int
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
		conn:      params.Conn,
		subject:   params.Subject,
		jetstream: jetstream,
		// pre-allocate enough space for messages and ackFuncs to avoid reallocations
		messages:      make([]*message, 0, params.BatchSize),
		batchSize:     params.BatchSize,
		retryWait:     params.RetryWait,
		retryAttempts: params.RetryAttempts,
	}, nil
}

// Write synchronously writes a record if the w.batchSize if equal to 1.
// If the batch size is greater than 1 the method will return an sdk.ErrUnimplemented.
func (w *Writer) Write(ctx context.Context, record sdk.Record) error {
	if w.batchSize > 1 {
		return sdk.ErrUnimplemented
	}

	opts := w.getPublishOptions()

	_, err := w.jetstream.Publish(w.subject, record.Payload.Bytes(), opts...)
	if err != nil {
		return fmt.Errorf("publish sync: %w", err)
	}

	return nil
}

// WriteAsync writes a record and an ackFunc to internal buffers.
// If the w.batchSize is equal to 1 the method will return an sdk.ErrUnimplemented.
func (w *Writer) WriteAsync(ctx context.Context, record sdk.Record, ackFunc sdk.AckFunc) (batchIsFull bool, err error) {
	if w.batchSize == 1 {
		return false, sdk.ErrUnimplemented
	}

	w.Lock()
	defer w.Unlock()

	w.messages = append(w.messages, &message{
		data:    record.Payload.Bytes(),
		ackFunc: ackFunc,
	})

	return len(w.messages) >= w.batchSize, nil
}

// Flush flushes batched records.
func (w *Writer) Flush(ctx context.Context) error {
	w.Lock()

	messagesBatch := make([]*message, len(w.messages))
	copy(messagesBatch, w.messages)
	w.messages = w.messages[:0]

	w.Unlock()

	opts := w.getPublishOptions()

	// publish all messages from a messages batch asynchronously
	futures := make([]nats.PubAckFuture, len(messagesBatch))
	for i, msg := range messagesBatch {
		future, err := w.jetstream.PublishAsync(w.subject, msg.data, opts...)
		if err != nil {
			return fmt.Errorf("publish async: %w", err)
		}

		futures[i] = future
	}

	// wait until all messages are delivered
	select {
	case <-w.jetstream.PublishAsyncComplete():
		for i, future := range futures {
			sdk.Logger(ctx).Info().Msgf("processed: %d", i)

			select {
			case <-future.Ok():
				if err := messagesBatch[i].ackFunc(nil); err != nil {
					sdk.Logger(ctx).Err(fmt.Errorf("ack func: %w", err))
				}

			case err := <-future.Err():
				sdk.Logger(ctx).Err(
					fmt.Errorf("publish async future for message %d in batch is not OK: %w", i, err),
				)

				if err := messagesBatch[i].ackFunc(err); err != nil {
					sdk.Logger(ctx).Err(fmt.Errorf("ack func: %w", err))
				}
			}
		}

	case <-ctx.Done():
		return ctx.Err()
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

// getPublishOptions returns a NATS publish options based on the Writer's config.
func (w *Writer) getPublishOptions() []nats.PubOpt {
	var opts []nats.PubOpt

	if w.retryWait != 0 {
		opts = append(opts, nats.RetryWait(w.retryWait))
	}

	if w.retryAttempts != 0 {
		opts = append(opts, nats.RetryAttempts(w.retryAttempts))
	}

	return opts
}
