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
	"testing"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

func TestDestination_Configure(t *testing.T) {
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

func TestDestination_Write(t *testing.T) {
	type args struct {
		records       []sdk.Record
		failedWrites  int
		closedContext bool
	}

	tests := []struct {
		name            string
		args            args
		expectedWritten int
		expectedErr     error
	}{
		{
			name: "Write works as expected",
			args: args{
				records: []sdk.Record{
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
				},
			},
			expectedWritten: 1,
		},
		{
			name: "Write can fail",
			args: args{
				failedWrites: 1,
				records: []sdk.Record{
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
				},
			},
			expectedWritten: 0,
			expectedErr:     errors.New("an error"),
		},
		{
			name: "context can be closed",
			args: args{
				closedContext: true,
				records: []sdk.Record{
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
				},
			},
			expectedWritten: 0,
			expectedErr:     context.Canceled,
		},
	}

	for _, tt := range tests {
		ctx := context.Background()

		mockPublisher := &mockJetstreamPublisher{
			failedWrites: tt.args.failedWrites,
			err:          tt.expectedErr,
		}
		d := &Destination{writer: &Writer{
			publisher: mockPublisher,
		}}

		if tt.args.closedContext {
			var cancel context.CancelCauseFunc
			ctx, cancel = context.WithCancelCause(ctx)
			cancel(context.Canceled)
			<-ctx.Done()
		}

		t.Run(tt.name, func(t *testing.T) {
			written, err := d.Write(ctx, tt.args.records)
			if err != nil && tt.expectedErr == nil {
				t.Errorf("Destination.Write() error = %s", err)
			}
			if err != nil && tt.expectedErr != nil && !errors.Is(err, tt.expectedErr) {
				t.Errorf("Destination.Write() error = %s, wantErr %s", err.Error(), tt.expectedErr)
			}
			if written != tt.expectedWritten {
				t.Errorf("Destination.Write() written = %d, wantWritten %d", written, tt.expectedWritten)
			}
		})
	}
}

func TestDestination_Teardown(t *testing.T) {
	type args struct {
		ctx context.Context
	}

	tests := []struct {
		name        string
		args        args
		closeCalled bool
	}{
		{
			name: "Teardown works succefully",
			args: args{
				ctx: context.Background(),
			},
			closeCalled: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			nm := &natsMock{}
			d := &Destination{
				nc: nm,
			}
			err := d.Teardown(tt.args.ctx)

			// Asserts
			if err != nil {
				t.Errorf("Destination.Teardown() unexpected error = %v", err)
			}

			// nats close
			if tt.closeCalled != nm.closeCalled {
				t.Errorf("Destination.Teardown() nats Close method was not called")
			}
		})
	}
}

type natsMock struct {
	closeCalled bool
}

func (m *natsMock) Drain() error {
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

type mockJetstreamPublisher struct {
	totalWrites  int
	failedWrites int
	err          error
}

func (m *mockJetstreamPublisher) Publish(_ string, _ []byte, _ ...nats.PubOpt) (*nats.PubAck, error) {
	m.totalWrites++
	if m.failedWrites != 0 && m.totalWrites <= m.failedWrites {
		return nil, m.err
	}

	return nil, nil
}
