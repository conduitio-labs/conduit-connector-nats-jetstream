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
	"errors"
	"sync/atomic"
	"testing"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	"github.com/nats-io/nats.go"
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

func TestDestination_Teardown(t *testing.T) {
	t.Parallel()

	type args struct {
		ctx context.Context
	}

	tests := []struct {
		name        string
		args        args
		drainErr    error
		drainCalled bool
		closeCalled bool
	}{
		{
			name: "Teardown works succefully",
			args: args{
				ctx: context.Background(),
			},
			drainErr:    nil,
			drainCalled: true,
			closeCalled: true,
		},
		{
			name: "Drain can fail",
			args: args{
				ctx: context.Background(),
			},
			drainErr:    nats.ErrTimeout,
			drainCalled: true,
			closeCalled: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			nm := &natsMock{
				drainErr: tt.drainErr,
			}
			d := &Destination{
				nc: nm,
				writer: &Writer{
					canWrite: atomic.Bool{},
				},
			}
			d.writer.canWrite.Store(true)
			err := d.Teardown(tt.args.ctx)

			// Asserts

			// writer
			if d.writer.canWrite.Load() != false {
				t.Errorf("Destination.Teardown() can write should be false")
			}

			// nats drain
			if tt.drainCalled != nm.drainCalled {
				t.Errorf("Destination.Teardown() nats Drain method was not called")
			}
			if tt.drainErr != nil && errors.Is(nm.drainErr, err) {
				t.Errorf("Destination.Teardown() expected error = %v", err)
				return
			}

			// nats close
			if tt.closeCalled != nm.closeCalled {
				t.Errorf("Destination.Teardown() nats Close method was not called")
			}
		})
	}
}

type natsMock struct {
	drainErr    error
	drainCalled bool
	closeCalled bool
}

func (m *natsMock) Drain() error {
	m.drainCalled = true
	if m.drainErr != nil {
		return m.drainErr
	}
	return nil
}

func (m *natsMock) JetStream(...nats.JSOpt) (nats.JetStreamContext, error) {
	return nil, nil
}
func (m *natsMock) IsConnected() bool {
	return false
}

func (m *natsMock) Close() {
	m.closeCalled = true
}
