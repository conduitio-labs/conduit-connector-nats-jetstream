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
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

// heartbeatTimeout is a default heartbeat timeout for push consumers.
const heartbeatTimeout = 2 * time.Second

// Iterator is a iterator for JetStream communication model.
// It receives message from NATS JetStream.
type Iterator struct {
	sync.Mutex

	conn          *nats.Conn
	messages      chan *nats.Msg
	unackMessages []*nats.Msg
	jetstream     nats.JetStreamContext
	consumerInfo  *nats.ConsumerInfo
	subscription  *nats.Subscription
}

// IteratorParams contains incoming params for the NewIterator function.
type IteratorParams struct {
	Conn       *nats.Conn
	BufferSize int
	Durable    string
	Stream     string
	Subject    string
	AckPolicy  nats.AckPolicy
}

// NewIterator creates new instance of the Iterator.
func NewIterator(ctx context.Context, params IteratorParams, sdkPosition sdk.Position) (*Iterator, error) {
	jetstream, err := params.Conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("get jetstream context: %w", err)
	}

	consumerConfig, err := getConsumerConfig(params, sdkPosition)
	if err != nil {
		return nil, fmt.Errorf("get consumer config: %w", err)
	}

	consumerInfo, err := jetstream.AddConsumer(params.Stream, consumerConfig)
	if err != nil {
		return nil, fmt.Errorf("add jetstream consumer: %w", err)
	}

	messages := make(chan *nats.Msg, params.BufferSize)

	subscription, err := jetstream.ChanSubscribe(params.Subject, messages,
		nats.Durable(consumerInfo.Config.Durable),
	)
	if err != nil {
		return nil, fmt.Errorf("chan subscribe: %w", err)
	}

	return &Iterator{
		conn:          params.Conn,
		messages:      messages,
		unackMessages: make([]*nats.Msg, 0),
		jetstream:     jetstream,
		consumerInfo:  consumerInfo,
		subscription:  subscription,
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
		sdkRecord, err := i.messageToRecord(msg)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("convert message to record: %w", err)
		}

		i.Lock()
		i.unackMessages = append(i.unackMessages, msg)
		i.Unlock()

		return sdkRecord, nil

	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	}
}

// Ack acknowledges a message at the given position.
func (i *Iterator) Ack(ctx context.Context, position sdk.Position) error {
	i.Lock()
	defer i.Unlock()

	unackMessage := i.unackMessages[0]

	if err := i.canAck(unackMessage, position); err != nil {
		return fmt.Errorf("message cannot be acknowledged: %w", err)
	}

	if err := unackMessage.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}

	// remove acknowledged message from the slice
	i.unackMessages = i.unackMessages[1:]

	return nil
}

// Stop stops the Iterator, unsubscribes from a subject.
func (i *Iterator) Stop() (err error) {
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

	if i.jetstream != nil {
		err := i.jetstream.DeleteConsumer(
			i.consumerInfo.Stream,
			i.consumerInfo.Name,
		)
		if err != nil {
			return fmt.Errorf("delete consumer: %w", err)
		}
	}

	if i.conn != nil {
		i.conn.Close()
	}

	return nil
}

// getConsumerConfig returns a *nats.ConsumerConfig based on the incoming params and a sdk.Position.
func getConsumerConfig(params IteratorParams, sdkPosition sdk.Position) (*nats.ConsumerConfig, error) {
	position, err := parsePosition(sdkPosition)
	if err != nil {
		return nil, fmt.Errorf("parse position: %w", err)
	}

	var (
		deliveryPolicy = nats.DeliverAllPolicy
		startSeq       uint64
	)

	// if the position has non-zero OptSeq
	// the connector will start consuming from that position
	if position.OptSeq != 0 {
		deliveryPolicy = nats.DeliverByStartSequencePolicy
		// skip a message which was already consumed
		startSeq = position.OptSeq + 1
	}

	return &nats.ConsumerConfig{
		Durable:        params.Durable,
		ReplayPolicy:   nats.ReplayInstantPolicy,
		DeliverSubject: fmt.Sprintf("%s.%s", params.Durable, params.Stream),
		DeliverPolicy:  deliveryPolicy,
		OptStartSeq:    startSeq,
		AckPolicy:      params.AckPolicy,
		FlowControl:    true,
		Heartbeat:      heartbeatTimeout,
	}, nil
}

// canAck checks if the messages can be acknowledged.
func (i *Iterator) canAck(msg *nats.Msg, sdkPosition sdk.Position) error {
	position, err := i.getPosition(msg)
	if err != nil {
		return fmt.Errorf("get position: %w", err)
	}

	if bytes.Compare(position, sdkPosition) != 0 {
		return fmt.Errorf(
			"ack is out-of-order, requested ack for %q, but first unack. Message is %q", sdkPosition, position,
		)
	}

	return nil
}

// messageToRecord converts a *nats.Msg to a sdk.Record.
func (i *Iterator) messageToRecord(msg *nats.Msg) (sdk.Record, error) {
	position, err := i.getPosition(msg)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get position: %w", err)
	}

	return sdk.Record{
		Position:  position,
		CreatedAt: time.Now(),
		Payload:   sdk.RawData(msg.Data),
	}, nil
}

// getPosition returns the current iterator position in the form of sdk.Position.
func (i *Iterator) getPosition(msg *nats.Msg) (sdk.Position, error) {
	metadata, err := msg.Metadata()
	if err != nil {
		return nil, fmt.Errorf("get message metadata: %w", err)
	}

	position := position{
		Durable: i.consumerInfo.Name,
		Stream:  i.consumerInfo.Stream,
		Subject: i.subscription.Subject,
		OptSeq:  metadata.Sequence.Stream,
	}

	sdkPosition, err := position.marshalSDKPosition()
	if err != nil {
		return nil, fmt.Errorf("marshal sdk position: %w", err)
	}

	return sdkPosition, nil
}
