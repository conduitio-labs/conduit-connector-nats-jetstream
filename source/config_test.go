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
	"strings"
	"testing"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
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
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyStreamName: "SuperStream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				StreamName:    "SuperStream",
				BufferSize:    defaultBufferSize,
				DeliverPolicy: defaultDeliverPolicy,
				AckPolicy:     defaultAckPolicy,
			},
			wantErr: false,
		},
		{
			name: "fail, mode is jetstream, but streamName is empty",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:    "nats://127.0.0.1:1222",
					config.KeySubject: "foo",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid StreamName, unallowed symbols",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyStreamName: "sup3r@stream!\\!ame",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid StreamName, length is too long",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyStreamName: "superLongStreamNameWithALotOfsymbolsWithinIt",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, default values",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyStreamName: "stream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				StreamName:    "stream",
				BufferSize:    defaultBufferSize,
				DeliverPolicy: defaultDeliverPolicy,
				AckPolicy:     defaultAckPolicy,
			},
			wantErr: false,
		},
		{
			name: "success, set buffer size",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyBufferSize: "128",
					ConfigKeyStreamName: "stream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				StreamName:    "stream",
				BufferSize:    128,
				DeliverPolicy: defaultDeliverPolicy,
				AckPolicy:     defaultAckPolicy,
			},
			wantErr: false,
		},
		{
			name: "success, default buffer size",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyStreamName: "stream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				StreamName:    "stream",
				BufferSize:    defaultBufferSize,
				DeliverPolicy: defaultDeliverPolicy,
				AckPolicy:     defaultAckPolicy,
			},
			wantErr: false,
		},
		{
			name: "fail, invalid buffer size",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyBufferSize: "8",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid buffer size",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyBufferSize: "what",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, all ack policy",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyAckPolicy:  "all",
					ConfigKeyStreamName: "stream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				StreamName: "stream",
				BufferSize: defaultBufferSize,
				AckPolicy:  nats.AckAllPolicy,
			},
			wantErr: false,
		},
		{
			name: "success, none ack policy",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyAckPolicy:  "none",
					ConfigKeyStreamName: "stream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				StreamName:    "stream",
				BufferSize:    defaultBufferSize,
				DeliverPolicy: defaultDeliverPolicy,
				AckPolicy:     nats.AckNonePolicy,
			},
			wantErr: false,
		},
		{
			name: "fail, invalid ack policy",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:     "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:  "foo",
					ConfigKeyAckPolicy: "wrong",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, deliver policy new",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:         "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:      "foo",
					ConfigKeyStreamName:    "mystream",
					ConfigKeyDeliverPolicy: "new",
					ConfigKeyAckPolicy:     "explicit",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				StreamName:    "mystream",
				BufferSize:    defaultBufferSize,
				DeliverPolicy: nats.DeliverNewPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
			},
			wantErr: false,
		},
		{
			name: "fail, invalid deliver policy",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:         "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:      "foo",
					ConfigKeyDeliverPolicy: "wrong",
					ConfigKeyAckPolicy:     "explicit",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, custom durable name",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:      "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					config.KeySubject:   "foo",
					ConfigKeyDurable:    "my_super_durable",
					ConfigKeyStreamName: "stream",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				StreamName:    "stream",
				Durable:       "my_super_durable",
				BufferSize:    defaultBufferSize,
				DeliverPolicy: nats.DeliverAllPolicy,
				AckPolicy:     nats.AckExplicitPolicy,
			},
			wantErr: false,
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

			if strings.HasPrefix(got.ConnectionName, config.DefaultConnectionNamePrefix) {
				tt.want.ConnectionName = got.ConnectionName
			}

			if strings.HasPrefix(got.Durable, defaultDurablePrefix) {
				tt.want.Durable = got.Durable
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
