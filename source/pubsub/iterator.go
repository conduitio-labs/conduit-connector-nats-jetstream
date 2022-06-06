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
	"fmt"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

// Iterator is a iterator for Pub/Sub communication model.
// It receives any new message from NATS.
type Iterator struct {
	conn         *nats.Conn
	messages     chan *nats.Msg
	subscription *nats.Subscription
}

// IteratorParams contains incoming params for the NewIterator function.
type IteratorParams struct {
	Conn       *nats.Conn
	BufferSize int
	Subject    string
}

// NewIterator creates new instance of the Iterator.
func NewIterator(ctx context.Context, params IteratorParams) (*Iterator, error) {
	messages := make(chan *nats.Msg, params.BufferSize)

	subscription, err := params.Conn.ChanSubscribe(params.Subject, messages)
	if err != nil {
		return nil, fmt.Errorf("chan subscribe: %w", err)
	}

	return &Iterator{
		conn:         params.Conn,
		messages:     messages,
		subscription: subscription,
	}, nil
}

// HasNext checks is the iterator has messages.
func (i *Iterator) HasNext(ctx context.Context) bool {
	return len(i.messages) > 0
}

// Next returns the next record from the underlying messages channel.
func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case msg := <-i.messages:
		return i.messageToRecord(msg)

	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

// Ack returns sdk.ErrUnimplemented,
// we don't need anything here for Pub/Sub iterator.
func (i *Iterator) Ack(ctx context.Context, position sdk.Position) error {
	return sdk.ErrUnimplemented
}

// Stop stops the Iterator, unsubscribes from a subject.
func (i *Iterator) Stop() (err error) {
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
func (i *Iterator) messageToRecord(msg *nats.Msg) (sdk.Record, error) {
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
func (i *Iterator) getPosition() (sdk.Position, error) {
	uuidBytes, err := uuid.New().MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("marshal uuid: %w", err)
	}

	return sdk.Position(uuidBytes), nil
}
