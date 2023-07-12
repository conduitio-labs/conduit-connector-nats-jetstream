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
		cfg           map[string]string
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
				cfg: map[string]string{
					config.KeyURLs:    "nats://127.0.0.1:4222",
					config.KeySubject: "foo",
				},
				records: []sdk.Record{
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
				},
			},
			expectedWritten: 1,
		},
		{
			name: "failed writes can result in partial writes",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:    "nats://127.0.0.1:4222",
					config.KeySubject: "foo",
				},
				records: []sdk.Record{
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
				},
				failedWrites: 1,
			},
			expectedWritten: 2,
		},
		{
			name: "failed writes can result in zero writes",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:    "nats://127.0.0.1:4222",
					config.KeySubject: "foo",
				},
				records: []sdk.Record{
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
				},
				failedWrites: 1,
			},
			expectedWritten: 0,
		},
		{
			name: "context can be closed",
			args: args{
				closedContext: true,
				cfg: map[string]string{
					config.KeyURLs:    "nats://127.0.0.1:4222",
					config.KeySubject: "foo-bar",
				},
				records: []sdk.Record{
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
				},
			},
			expectedWritten: 0,
			expectedErr:     context.Canceled,
		},
		{
			name: "writes can return error and partial rewrites because the amount of attempts",
			args: args{
				cfg: map[string]string{
					config.KeyURLs:         "nats://127.0.0.1:4222",
					config.KeySubject:      "foo-baz",
					ConfigKeyRetryAttempts: "1",
				},
				records: []sdk.Record{
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
					{Payload: sdk.Change{After: make(sdk.RawData, 10)}},
				},
				failedWrites: 4,
			},
			expectedWritten: 0,
			expectedErr:     errWriteUnavailable,
		},
	}

	for _, tt := range tests {
		ctx := context.Background()

		mockPublisher := &mockJetstreamPublisher{
			failedWrites: tt.args.failedWrites,
		}
		d := &Destination{writer: &Writer{
			canWrite:  atomic.Bool{},
			publisher: mockPublisher,
		}}
		if err := d.Configure(ctx, tt.args.cfg); err != nil {
			t.Errorf("Destination.Configure() error = %s", err)
		}
		d.writer.startWrites()

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
				writer: &Writer{
					canWrite: atomic.Bool{},
				},
			}
			d.writer.canWrite.Store(true)
			err := d.Teardown(tt.args.ctx)

			// Asserts
			if err != nil {
				t.Errorf("Destination.Teardown() unexpected error = %v", err)
			}

			// writer
			if d.writer.canWrite.Load() != false {
				t.Errorf("Destination.Teardown() can write should be false")
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
}

func (m *mockJetstreamPublisher) Publish(_ string, _ []byte, _ ...nats.PubOpt) (*nats.PubAck, error) {
	m.totalWrites++
	if m.failedWrites != 0 && m.totalWrites <= m.failedWrites {
		return nil, errWriteUnavailable
	}

	return nil, nil
}
