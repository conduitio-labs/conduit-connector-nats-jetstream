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
	"context"
	"testing"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
)

func TestDestination_Configure(t *testing.T) {
	t.Parallel()

	type args struct {
		ctx context.Context
		cfg map[string]string
	}

	tests := []struct {
		name        string
		args        args
		expectedErr string
	}{
		{
			name: "success, correct config",
			args: args{
				ctx: context.Background(),
				cfg: map[string]string{
					config.KeyURLs:    "nats://127.0.0.1:4222",
					config.KeySubject: "foo",
				},
			},
		},
		{
			name: "fail, empty config",
			args: args{
				ctx: context.Background(),
				cfg: map[string]string{},
			},
			expectedErr: `parse config: parse common config: validate config: "URLs[0]" value must be a valid url;` +
				` "subject" value must be set`,
		},
		{
			name: "fail, invalid config",
			args: args{
				ctx: context.Background(),
				cfg: map[string]string{
					config.KeyURLs: "nats://127.0.0.1:4222",
				},
			},
			expectedErr: `parse config: parse common config: validate config: "subject" value must be set`,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := &Destination{}
			if err := d.Configure(tt.args.ctx, tt.args.cfg); err != nil {
				if tt.expectedErr == "" {
					t.Errorf("Destination.Configure() unexpected error = %v", err)

					return
				}

				if err.Error() != tt.expectedErr {
					t.Errorf("Destination.Configure() error = %s, wantErr %s", err.Error(), tt.expectedErr)
				}
			}
		})
	}
}
