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
	"testing"

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
			name: "true, multiple messages",
			fields: fields{
				messages: make(chan *nats.Msg, 2),
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

			it := &Iterator{
				messages: tt.fields.messages,
			}

			for i := 0; i < cap(it.messages); i++ {
				if tt.fillFunc != nil {
					tt.fillFunc(it.messages)
				}

				if got := it.HasNext(context.Background()); got != tt.want {
					t.Errorf("Iterator.HasNext() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}
