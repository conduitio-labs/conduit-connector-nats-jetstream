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
	"reflect"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
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
			name: "success, all defaults",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:    "nats://localhost:4222",
					config.KeySubject: "foo",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://localhost:4222"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				RetryWait:     defaultRetryWait,
				RetryAttempts: defaultRetryAttempts,
			},
			wantErr: false,
		},
		{
			name: "success, custom retry wait",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:     "nats://localhost:4222",
					config.KeySubject:  "foo",
					ConfigKeyRetryWait: "3s",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://localhost:4222"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				RetryWait:     time.Second * 3,
				RetryAttempts: defaultRetryAttempts,
			},
			wantErr: false,
		},
		{
			name: "success, custom retry attempts",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:         "nats://localhost:4222",
					config.KeySubject:      "foo",
					ConfigKeyRetryAttempts: "5",
				},
			},
			want: Config{
				Config: config.Config{
					URLs:          []string{"nats://localhost:4222"},
					Subject:       "foo",
					MaxReconnects: config.DefaultMaxReconnects,
					ReconnectWait: config.DefaultReconnectWait,
				},
				RetryWait:     defaultRetryWait,
				RetryAttempts: 5,
			},
			wantErr: false,
		},
		{
			name: "fail, invalid retry wait",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:     "nats://localhost:4222",
					config.KeySubject:  "foo",
					ConfigKeyRetryWait: "wrong",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid retry attempts",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:         "nats://localhost:4222",
					config.KeySubject:      "foo",
					ConfigKeyRetryAttempts: "wrong",
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

			tt.want.Config.ConnectionName = got.ConnectionName

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
