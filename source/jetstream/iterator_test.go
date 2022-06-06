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
	"reflect"
	"testing"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

func TestIterator_HasNext(t *testing.T) {
	t.Parallel()

	type fields struct {
		messages chan *nats.Msg
	}

	tests := []struct {
		name     string
		fields   fields
		fillFunc func(chan *nats.Msg)
		want     bool
	}{
		{
			name: "true, one message",
			fields: fields{
				messages: make(chan *nats.Msg, 1),
			},
			fillFunc: func(c chan *nats.Msg) {
				c <- &nats.Msg{
					Subject: "foo",
					Data:    []byte("something"),
				}
			},
			want: true,
		},
		{
			name: "false, no messages",
			fields: fields{
				messages: make(chan *nats.Msg, 1),
			},
			fillFunc: nil,
			want:     false,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			i := &Iterator{
				messages: tt.fields.messages,
			}

			if tt.fillFunc != nil {
				tt.fillFunc(i.messages)
			}

			if got := i.HasNext(context.Background()); got != tt.want {
				t.Errorf("Iterator.HasNext() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getConsumerConfig(t *testing.T) {
	t.Parallel()

	type args struct {
		params IteratorParams
	}
	tests := []struct {
		name    string
		args    args
		want    *nats.ConsumerConfig
		wantErr bool
	}{
		{
			name: "success, empty delivery and ack policies",
			args: args{
				params: IteratorParams{
					Durable: "conduit_push_consumer",
					Stream:  "mystream",
				},
			},
			want: &nats.ConsumerConfig{
				Durable:        "conduit_push_consumer",
				ReplayPolicy:   nats.ReplayInstantPolicy,
				DeliverSubject: "conduit_push_consumer.mystream",
				DeliverPolicy:  nats.DeliverAllPolicy,
				OptStartSeq:    0,
				AckPolicy:      nats.AckNonePolicy,
				FlowControl:    true,
				Heartbeat:      2 * time.Second,
			},
		},
		{
			name: "success, custom delivery and ack policies",
			args: args{
				params: IteratorParams{
					Durable:       "conduit_push_consumer",
					Stream:        "mystream",
					DeliverPolicy: nats.DeliverNewPolicy,
					AckPolicy:     nats.AckExplicitPolicy,
				},
			},
			want: &nats.ConsumerConfig{
				Durable:        "conduit_push_consumer",
				ReplayPolicy:   nats.ReplayInstantPolicy,
				DeliverSubject: "conduit_push_consumer.mystream",
				DeliverPolicy:  nats.DeliverNewPolicy,
				OptStartSeq:    0,
				AckPolicy:      nats.AckExplicitPolicy,
				FlowControl:    true,
				Heartbeat:      2 * time.Second,
			},
		},
		{
			name: "success, custom delivery and ack policies and position",
			args: args{
				params: IteratorParams{
					Durable:       "conduit_push_consumer",
					Stream:        "mystream",
					SDKPosition:   sdk.Position(`{"opt_seq": 3}`),
					DeliverPolicy: nats.DeliverAllPolicy,
					AckPolicy:     nats.AckExplicitPolicy,
				},
			},
			want: &nats.ConsumerConfig{
				Durable:        "conduit_push_consumer",
				ReplayPolicy:   nats.ReplayInstantPolicy,
				DeliverSubject: "conduit_push_consumer.mystream",
				DeliverPolicy:  nats.DeliverByStartSequencePolicy,
				OptStartSeq:    3,
				AckPolicy:      nats.AckExplicitPolicy,
				FlowControl:    true,
				Heartbeat:      2 * time.Second,
			},
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := getConsumerConfig(tt.args.params)
			if (err != nil) != tt.wantErr {
				t.Errorf("getConsumerConfig() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("getConsumerConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}
