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

package source

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

// fetchSize is always 1 because conduit expects only one Record
const fetchSize = 1

// Iterator is a iterator for JetStream communication model.
// It receives message from NATS JetStream.
type Iterator struct {
	mu sync.RWMutex

	jetstream     nats.JetStreamContext
	unackMessages map[uint64]*nats.Msg
	subscription  *nats.Subscription
	params        IteratorParams
}

// IteratorParams contains incoming params for the NewIterator function.
type IteratorParams struct {
	Conn           *nats.Conn
	BufferSize     int
	Stream         string
	Durable        string
	DeliverSubject string
	Subject        string
	SDKPosition    sdk.Position
	DeliverPolicy  nats.DeliverPolicy
	AckPolicy      nats.AckPolicy
}

// generateConsumerConfig returns a NATS subscribe options based on the IteratorParams's fields.
func (p IteratorParams) getSubscriberOpts(ctx context.Context) ([]nats.SubOpt, error) {
	var opts []nats.SubOpt

	position, err := parsePosition(p.SDKPosition)
	if err != nil {
		return nil, fmt.Errorf("parse position: %w", err)
	}

	// if the position has a non-zero OptSeq
	// the connector will start consuming from that position
	if position.OptSeq != 0 {
		// add 1 to the sequence in order to skip the consumed message at this position
		// and start consuming new messages
		// deliverPolicy in this case will become a DeliverByStartSequencePolicy.
		opts = append(opts, nats.StartSequence(position.OptSeq))
	} else {
		switch p.DeliverPolicy {
		case nats.DeliverAllPolicy:
			opts = append(opts, nats.DeliverAll())
		case nats.DeliverNewPolicy:
			opts = append(opts, nats.DeliverNew())
		}
	}

	switch p.AckPolicy {
	case nats.AckAllPolicy:
		opts = append(opts, nats.AckAll())
	case nats.AckExplicitPolicy:
		opts = append(opts, nats.AckExplicit())
	case nats.AckNonePolicy:
		opts = append(opts, nats.AckNone())
	}

	opts = append(opts,
		nats.Context(ctx),
		nats.PullMaxWaiting(p.BufferSize),
	)

	return opts, nil
}

// NewIterator creates new instance of the Iterator.
func NewIterator(ctx context.Context, params IteratorParams) (*Iterator, error) {
	i := &Iterator{
		mu:     sync.RWMutex{},
		params: params,
	}

	var err error
	// i.messages = make(chan *nats.Msg, i.params.BufferSize)
	i.unackMessages = make(map[uint64]*nats.Msg, i.params.BufferSize)
	i.jetstream, err = i.params.Conn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("get jetstream context: %w", err)
	}

	subscriberOpts, err := i.params.getSubscriberOpts(ctx)
	if err != nil {
		return nil, fmt.Errorf("get consumer options: %w", err)
	}

	// if _, err := i.jetstream.AddStream(&nats.StreamConfig{
	// 	Name:     params.Stream,
	// 	Subjects: []string{params.Subject},
	// 	Storage:  nats.FileStorage,
	// }); err != nil && err != nats.ErrStreamNameAlreadyInUse {
	// 	return nil, fmt.Errorf("creting jetstream stream: %w", err)
	// }

	// if _, err := i.jetstream.AddConsumer(params.Stream, &nats.ConsumerConfig{
	// 	Durable:       params.Durable,
	// 	AckPolicy:     nats.AckExplicitPolicy,
	// 	MaxWaiting:    params.BufferSize,
	// 	MaxAckPending: params.BufferSize,
	// }); err != nil && err != nats.ErrConsumerNameAlreadyInUse {
	// 	return nil, fmt.Errorf("creting jetstream consumer: %w", err)
	// }

	i.subscription, err = i.jetstream.PullSubscribe(i.params.Subject, i.params.Durable, subscriberOpts...)
	if err != nil || i.subscription == nil {
		return nil, fmt.Errorf("pull subscribe: %w", err)
	}

	go i.status(ctx)

	return i, nil
}

// HasNext checks is the iterator has messages.
func (i *Iterator) HasNext(ctx context.Context) bool {
	if !i.params.Conn.IsConnected() && !i.subscription.IsValid() {
		return false
	}

	ci, err := i.subscription.ConsumerInfo()
	if err != nil {
		sdk.Logger(ctx).
			Error().
			Err(err).
			Interface("consumer_info", ci).
			Send()
		return false
	}

	return ci.NumPending > 0
}

// Next returns the next record from the underlying messages channel.
// It also appends messages to a unackMessages slice if the AckPolicy is not equal to AckNonePolicy.
func (i *Iterator) Next(ctx context.Context) (sdk.Record, error) {
	select {
	case <-ctx.Done():
		return sdk.Record{}, ctx.Err()
	default:
		msgs, err := i.subscription.Fetch(fetchSize, nats.Context(ctx))
		if err != nil {
			return sdk.Record{}, sdk.ErrBackoffRetry
		}

		if len(msgs) != fetchSize {
			return sdk.Record{}, sdk.ErrBackoffRetry
		}
		msg := msgs[0]

		sdkRecord, err := i.messageToRecord(msg)
		if err != nil {
			return sdk.Record{},
				errors.Join(
					sdk.ErrMetadataFieldNotFound,
					fmt.Errorf("convert message to record: %w", err),
				)
		}

		position, err := parsePosition(sdkRecord.Position)
		if err != nil {
			return sdk.Record{}, fmt.Errorf("convert record to position: %w", err)
		}

		if i.params.AckPolicy != nats.AckNonePolicy {
			i.mu.Lock()
			i.unackMessages[position.OptSeq] = msg
			i.mu.Unlock()
		}

		return sdkRecord, nil
	}
}

// Ack acknowledges a message at the given position.
func (i *Iterator) Ack(sdkPosition sdk.Position) error {
	// if ack policy is 'none' just return nil here
	if i.params.AckPolicy == nats.AckNonePolicy {
		return nil
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	position, err := parsePosition(sdkPosition)
	if err != nil {
		return fmt.Errorf("could not find record at position: %w", err)
	}

	msg, ok := i.unackMessages[position.OptSeq]
	if !ok {
		return fmt.Errorf("could not find message at position: %d not avaiable to ack", position)
	}

	if err := msg.Ack(); err != nil {
		return fmt.Errorf("ack message: %w", err)
	}

	// remove acknowledged message from the slice
	delete(i.unackMessages, position.OptSeq)

	return nil
}

func (i *Iterator) unAckAll() error {
	// explicity not acking unackedMessages
	for _, msg := range i.unackMessages {
		if err := msg.Nak(); err != nil {
			return fmt.Errorf("not ack (when stopping): %w", err)
		}
	}
	return nil
}

// Stop stops the Iterator, unsubscribes from a subject.
func (i *Iterator) Stop() (err error) {
	if i.subscription != nil {
		// it will delete a consumer belonged to the subscription as well
		if err = i.subscription.Unsubscribe(); err != nil {
			return fmt.Errorf("unsubscribe: %w", err)
		}
	}

	// explicity not acking unackedMessages
	if err := i.unAckAll(); err != nil {
		return fmt.Errorf("not ack (when stopping): %w", err)
	}

	if i.params.Conn != nil {
		i.params.Conn.Close()
	}

	return nil
}

// messageToRecord converts a *nats.Msg to a sdk.Record.
func (i *Iterator) messageToRecord(msg *nats.Msg) (sdk.Record, error) {
	// retrieve a message metadata one more time to grab a metadata.Timestamp
	// and use it for a sdk.Record.Metadata
	metadata, err := msg.Metadata()
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get message metadata: %w", err)
	}

	position, err := i.getMessagePosition(msg, metadata)
	if err != nil {
		return sdk.Record{}, fmt.Errorf("get position: %w", err)
	}

	if metadata.Timestamp.IsZero() {
		metadata.Timestamp = time.Now()
	}

	sdkMetadata := make(sdk.Metadata)
	sdkMetadata.SetCreatedAt(metadata.Timestamp)

	return sdk.Util.Source.NewRecordCreate(position, sdkMetadata, nil, sdk.RawData(msg.Data)), nil
}

// getMessagePosition returns a position of a message in the form of sdk.Position.
func (i *Iterator) getMessagePosition(msg *nats.Msg, metadata *nats.MsgMetadata) (sdk.Position, error) {
	position := position{
		OptSeq: metadata.Sequence.Consumer,
	}

	sdkPosition, err := position.marshalSDKPosition()
	if err != nil {
		return nil, fmt.Errorf("marshal sdk position: %w", err)
	}

	return sdkPosition, nil
}

func (i *Iterator) status(ctx context.Context) {
	t := time.NewTicker(time.Second * 10)

	for {
		<-t.C
		if i.subscription != nil {
			consumerInfo, _ := i.subscription.ConsumerInfo()
			sdk.Logger(ctx).Debug().Interface("consumer_info", consumerInfo).Send()
		}
	}
}
