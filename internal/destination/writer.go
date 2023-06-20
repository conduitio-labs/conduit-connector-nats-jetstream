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
	"fmt"
	"sync/atomic"
	"time"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/internal"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

type jetstreamPublisher interface {
	Publish(subj string, data []byte, opts ...nats.PubOpt) (*nats.PubAck, error)
}

// Writer implements a JetStream writer.
// It writes messages asynchronously.
type Writer struct {
	subject     string
	jetstream   jetstreamPublisher
	publishOpts []nats.PubOpt
	canWrite    atomic.Bool
}

// writerParams is an incoming params for the NewWriter function.
type writerParams struct {
	nc            internal.NATSClient
	subject       string
	retryWait     time.Duration
	retryAttempts int
}

// getPublishOptions returns a NATS publish options based on the WriterParams's fields.
func (p writerParams) getPublishOptions() []nats.PubOpt {
	var opts []nats.PubOpt

	if p.retryWait != 0 {
		opts = append(opts, nats.RetryWait(p.retryWait))
	}

	if p.retryAttempts != 0 {
		opts = append(opts, nats.RetryAttempts(p.retryAttempts))
	}

	return opts
}

// NewWriter creates new instance of the Writer.
func NewWriter(params writerParams) (*Writer, error) {
	jetstream, err := params.nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("get jetstream context: %w", err)
	}

	w := &Writer{
		subject:     params.subject,
		jetstream:   jetstream,
		publishOpts: params.getPublishOptions(),
		canWrite:    atomic.Bool{},
	}

	w.startWrites()

	return w, nil
}

// Write synchronously writes a record.
func (w *Writer) Write(record sdk.Record) error {
	// not redundant with Destination.Write
	// writes can become unavailable when processing multiple records
	if !w.canWrite.Load() {
		return errWriteUnavailable
	}

	_, err := w.jetstream.Publish(w.subject, record.Payload.After.Bytes(), w.publishOpts...)
	if err != nil {
		return fmt.Errorf("publish sync: %w", err)
	}

	return nil
}

// stopWrites to the NATS server.
func (w *Writer) stopWrites() {
	w.canWrite.Store(false)
}

// startWrites to the NATS server.
func (w *Writer) startWrites() {
	w.canWrite.Store(true)
}
