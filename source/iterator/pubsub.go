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

package iterator

import (
	"context"
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// PubSubIterator is a iterator for Pub/Sub communication model.
// It receives any new message from NATS.
type PubSubIterator struct {
	conn         *nats.Conn
	messages     chan *nats.Msg
	subscription *nats.Subscription
}

// PubSubIteratorParams contains incoming params for the NewPubSubIterator function.
type PubSubIteratorParams struct {
	Conn       *nats.Conn
	BufferSize int
	Subject    string
}

// NewPubSubIterator creates new instance of the PubSubIterator.
func NewPubSubIterator(ctx context.Context, params PubSubIteratorParams) (*PubSubIterator, error) {
	messages := make(chan *nats.Msg, params.BufferSize)

	subscription, err := params.Conn.ChanSubscribe(params.Subject, messages)
	if err != nil {
		return nil, fmt.Errorf("chan subscribe: %w", err)
	}

	return &PubSubIterator{
		conn:         params.Conn,
		messages:     messages,
		subscription: subscription,
	}, nil
}

// HasNext checks is the iterator has messages.
func (i *PubSubIterator) HasNext(ctx context.Context) bool {
	return len(i.messages) > 0
}

// Next returns the next record from the underlying messages channel.
func (i *PubSubIterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case msg := <-i.messages:
		return i.messageToRecord(msg)

	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

// Stop stops the PubSubIterator, unsubscribes from a subject.
func (i *PubSubIterator) Stop() (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("recovered: %w", err)
		}
	}()

	if i.subscription != nil {
		if err = i.subscription.Unsubscribe(); err != nil {
			return fmt.Errorf("unsubscribe: %w", err)
		}
	}

	close(i.messages)

	if i.conn != nil {
		i.conn.Close()
	}

	return nil
}

// messageToRecord converts a *nats.Msg to a sdk.Record.
func (i *PubSubIterator) messageToRecord(msg *nats.Msg) (sdk.Record, error) {
	position, err := i.getPosition()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get position: %w", err)
	}

	return sdk.Record{
		Position:  position,
		CreatedAt: time.Now(),
		Payload:   sdk.RawData(msg.Data),
	}, nil
}

// getPosition returns the current iterator position.
func (i *PubSubIterator) getPosition() (sdk.Position, error) {
	uuidBytes, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal uuid: %w", err)
	}

	return sdk.Position(uuidBytes), nil
}
