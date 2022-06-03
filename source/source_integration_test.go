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
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/conduitio-labs/conduit-connector-nats/config"
	"github.com/conduitio-labs/conduit-connector-nats/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

func TestSource_Open(t *testing.T) {
	t.Parallel()

	source := NewSource()
	err := source.Configure(context.Background(), map[string]string{
		config.ConfigKeyURLs:           test.TestURL,
		config.ConfigKeySubject:        "foo",
		config.ConfigKeyMode:           "pubsub",
		config.ConfigKeyConnectionName: "super_connection",
	})
	if err != nil {
		t.Fatalf("configure source: %v", err)

		return
	}

	err = source.Open(context.Background(), sdk.Position(nil))
	if err != nil {
		t.Fatalf("open source: %v", err)

		return
	}

	err = source.Teardown(context.Background())
	if err != nil {
		t.Fatalf("teardown source: %v", err)

		return
	}
}

func TestSource_Read_PubSub(t *testing.T) {
	t.Parallel()

	source := NewSource()
	err := source.Configure(context.Background(), map[string]string{
		config.ConfigKeyURLs:    test.TestURL,
		config.ConfigKeySubject: "foo",
		config.ConfigKeyMode:    "pubsub",
	})
	if err != nil {
		t.Fatalf("configure source: %v", err)

		return
	}

	err = source.Open(context.Background(), sdk.Position(nil))
	if err != nil {
		t.Fatalf("open source: %v", err)

		return
	}

	t.Cleanup(func() {
		if err := source.Teardown(context.Background()); err != nil {
			t.Fatalf("teardown source: %v", err)
		}
	})

	t.Run("success, one message", func(t *testing.T) {
		t.Parallel()

		testConn, err := test.GetTestConnection()
		if err != nil {
			t.Fatalf("get test connection: %v", err)

			return
		}

		err = testConn.Publish("foo", []byte(`{"level": "info"}`))
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

		if !reflect.DeepEqual(record.Payload.Bytes(), []byte(`{"level": "info"}`)) {
			t.Fatalf("Source.Read = %v, want %v", record.Payload.Bytes(), []byte(`{"level": "info"}`))

			return
		}
	})

	t.Run("success, many messages", func(t *testing.T) {
		t.Parallel()

		testConn, err := test.GetTestConnection()
		if err != nil {
			t.Fatalf("get test connection: %v", err)

			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		records := make([]sdk.Record, 0)
		for i := 0; i < 1024; i++ {
			err = testConn.Publish("foo", []byte(`{"level": "info"}`))
			if err != nil {
				t.Fatalf("publish message: %v", err)

				return
			}

			record, err := source.Read(ctx)
			if err != nil {
				if errors.Is(err, sdk.ErrBackoffRetry) {
					i--

					continue
				}
				t.Fatalf("read message: %v", err)

				return
			}

			records = append(records, record)
		}

		if len(records) != 1024 {
			t.Fatalf("len(records) = %d, expected = %d", len(records), 1024)

			return
		}
	})

	t.Run("success, no messages, backof retry", func(t *testing.T) {
		t.Parallel()

		_, err := source.Read(context.Background())
		if err != nil && !errors.Is(err, sdk.ErrBackoffRetry) {
			t.Fatalf("read message: %v", err)

			return
		}
	})
}
