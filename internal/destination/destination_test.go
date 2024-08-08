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
	"github.com/conduitio/conduit-commons/opencdc"
	"github.com/matryer/is"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestDestination_Configure(t *testing.T) {
	type args struct {
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
				cfg: map[string]string{
					"urls":    "nats://127.0.0.1:4222",
					"subject": "foo",
				},
			},
		},
		{
			name: "fail, negative retry wait",
			args: args{
				cfg: map[string]string{
					"urls":      "nats://127.0.0.1:4222",
					"subject":   "foo",
					"retryWait": "-5s",
				},
			},
			expectedErr: "RetryWait can't be a negative value",
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)
			d := &Destination{}

			err := d.Configure(context.Background(), tt.args.cfg)
			if tt.expectedErr == "" {
				is.NoErr(err)
			} else {
				is.True(err != nil)
				is.Equal(err.Error(), tt.expectedErr)
			}
		})
	}
}

func TestDestination_Write(t *testing.T) {
	type args struct {
		records       []opencdc.Record
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
				records: []opencdc.Record{
					{Payload: opencdc.Change{After: make(opencdc.RawData, 10)}},
				},
			},
			expectedWritten: 1,
		},
		{
			name: "Write can fail",
			args: args{
				failedWrites: 1,
				records: []opencdc.Record{
					{Payload: opencdc.Change{After: make(opencdc.RawData, 10)}},
				},
			},
			expectedWritten: 0,
			expectedErr:     errors.New("an error"),
		},
		{
			name: "context can be closed",
			args: args{
				closedContext: true,
				records: []opencdc.Record{
					{Payload: opencdc.Change{After: make(opencdc.RawData, 10)}},
					{Payload: opencdc.Change{After: make(opencdc.RawData, 10)}},
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
