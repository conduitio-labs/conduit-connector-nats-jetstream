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
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	"github.com/conduitio-labs/conduit-connector-nats-jetstream/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/nats-io/nats.go"
)

func TestSource_Open(t *testing.T) {
	t.Parallel()

	stream, subject := "mystreamone", "foo_source_one"

	source, err := createTestJetStream(t, stream, subject)
	if err != nil {
		t.Fatalf("create test jetstream: %v", err)

		return
	}

	if err := source.Teardown(context.Background()); err != nil {
		t.Fatalf("teardown source: %v", err)
	}
}

func TestSource_Read_JetStream_oneMessage(t *testing.T) {
	t.Parallel()

	stream, subject := "mystreamreadone", "foo_one"

	source, err := createTestJetStream(t, stream, subject)
	if err != nil {
		t.Fatalf("create test jetstream: %v", err)

		return
	}

	t.Cleanup(func() {
		if err := source.Teardown(context.Background()); err != nil {
			t.Fatalf("teardown source: %v", err)
		}
	})

	testConn, err := test.GetTestConnection()
	if err != nil {
		t.Fatalf("get test connection: %v", err)

		return
	}

	err = testConn.Publish(subject, []byte(`{"level": "info"}`))
	if err != nil {
		t.Fatalf("publish message: %v", err)

		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	var record sdk.Record
	for {
		record, err = source.Read(ctx)
		if err != nil {
			if errors.Is(err, sdk.ErrBackoffRetry) {
				continue
			}
			t.Fatalf("read message: %v", err)

			return
		}

		break
	}

	if !bytes.Equal(record.Payload.After.Bytes(), []byte(`{"level": "info"}`)) {
		t.Fatalf("Source.Read = %v, want %v", record.Payload.After.Bytes(), []byte(`{"level": "info"}`))

		return
	}
}

func TestSource_Read_JetStream_backoffRetry(t *testing.T) {
	t.Parallel()

	stream, subject := "mystreamtwo", "foo_two"

	source, err := createTestJetStream(t, stream, subject)
	if err != nil {
		t.Fatalf("create test jetstream: %v", err)

		return
	}

	t.Cleanup(func() {
		if err := source.Teardown(context.Background()); err != nil {
			t.Fatalf("teardown source: %v", err)
		}
	})

	_, err = source.Read(context.Background())
	if err == nil {
		t.Fatal("Source.Read expected backoff retry error, got nil")

		return
	}

	if err != nil && !errors.Is(err, sdk.ErrBackoffRetry) {
		t.Fatalf("read message: %v", err)

		return
	}
}

func createTestJetStream(t *testing.T, stream, subject string) (sdk.Source, error) {
	source := NewSource()
	err := source.Configure(context.Background(), map[string]string{
		config.KeyURLs:      test.TestURL,
		config.KeySubject:   subject,
		ConfigKeyStreamName: stream,
	})
	if err != nil {
		return nil, fmt.Errorf("configure source: %v", err)
	}

	testConn, err := test.GetTestConnection()
	if err != nil {
		return nil, fmt.Errorf("get test connection: %v", err)
	}

	js, err := testConn.JetStream()
	if err != nil {
		return nil, fmt.Errorf("create jetstream context: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     stream,
		Subjects: []string{subject},
	})
	if err != nil {
		return nil, fmt.Errorf("add stream: %v", err)
	}

	err = source.Open(context.Background(), sdk.Position(nil))
	if err != nil {
		return nil, fmt.Errorf("open source: %v", err)
	}

	return source, nil
}
