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
	"reflect"
	"testing"

	"github.com/conduitio-labs/conduit-connector-nats/config"
	"github.com/nats-io/nats.go"
)

func TestParse(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}

	tests := []struct {
		name    string
		args    args
		want    Config
		wantErr bool
	}{
		{
			name: "success, valid StreamName, all other are defaults",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "jetstream",
					ConfigKeyStreamName:     "SuperStream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:    []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject: "foo",
					Mode:    "jetstream",
				},
				StreamName: "SuperStream",
				BufferSize: defaultBufferSize,
				Durable:    defaultConsumerName,
				AckPolicy:  nats.AckExplicitPolicy,
			},
			wantErr: false,
		},
		{
			name: "fail, mode is jetstream, but streamName is empty",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "jetstream",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid StreamName, unallowed symbols",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "jetstream",
					ConfigKeyStreamName:     "sup3r@stream!\\!ame",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid StreamName, length is too long",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "jetstream",
					ConfigKeyStreamName:     "superLongStreamNameWithALotOfsymbolsWithinIt",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, default values",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "jetstream",
					ConfigKeyStreamName:     "stream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:    []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject: "foo",
					Mode:    "jetstream",
				},
				StreamName: "stream",
				BufferSize: defaultBufferSize,
				Durable:    defaultConsumerName,
				AckPolicy:  nats.AckExplicitPolicy,
			},
			wantErr: false,
		},
		{
			name: "success, set buffer size",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "pubsub",
					ConfigKeyBufferSize:     "128",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:    []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject: "foo",
					Mode:    "pubsub",
				},
				BufferSize: 128,
			},
			wantErr: false,
		},
		{
			name: "success, default buffer size",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "pubsub",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:    []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject: "foo",
					Mode:    "pubsub",
				},
				BufferSize: defaultBufferSize,
			},
			wantErr: false,
		},
		{
			name: "fail, invalid buffer size",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "pubsub",
					ConfigKeyBufferSize:     "8",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid buffer size",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "pubsub",
					ConfigKeyBufferSize:     "what",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, all ack policy",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "pubsub",
					ConfigKeyAckPolicy:      "all",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:    []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject: "foo",
					Mode:    "pubsub",
				},
				BufferSize: defaultBufferSize,
				AckPolicy:  nats.AckAllPolicy,
			},
			wantErr: false,
		},
		{
			name: "fail, invalid ack policy",
			args: args{
				cfg: map[string]string{
					config.ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.ConfigKeySubject: "foo",
					config.ConfigKeyMode:    "pubsub",
					ConfigKeyAckPolicy:      "wrong",
				},
			},
			want:    Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
