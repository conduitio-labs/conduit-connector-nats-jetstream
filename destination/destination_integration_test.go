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

	config "github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	test "github.com/conduitio-labs/conduit-connector-nats-jetstream/test"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/matryer/is"
)

func TestDestination_Open_Success(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_success",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(context.Background(), map[string]string{
		config.KeyURLs:    test.TestURL,
		config.KeySubject: "foo_destination_success",
	})
	is.NoErr(err)

	err = destination.Open(context.Background())
	is.NoErr(err)

	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Open_Fail(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_fail",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(context.Background(), map[string]string{
		config.KeyURLs:    "nats://localhost:6666",
		config.KeySubject: "foo_destination_fail",
	})
	is.NoErr(err)

	err = destination.Open(context.Background())
	is.True(err != nil)

	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_Write(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_write_jetstream",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(context.Background(), map[string]string{
		config.KeyURLs:     test.TestURL,
		config.KeySubject:  "foo_destination_write_jetstream",
		ConfigKeyBatchSize: "1",
	})
	is.NoErr(err)

	err = destination.Open(context.Background())
	is.NoErr(err)

	err = destination.Write(context.Background(), sdk.Record{
		Payload: sdk.RawData([]byte("hello")),
	})
	is.NoErr(err)

	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_WriteAsync_MessagesCountAreEqualToBatchSize(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_write_async_jetstream_100_messages_100_batch_size",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(context.Background(), map[string]string{
		config.KeyURLs:     test.TestURL,
		config.KeySubject:  "foo_destination_write_async_jetstream_100_messages_100_batch_size",
		ConfigKeyBatchSize: "100",
	})
	is.NoErr(err)

	err = destination.Open(context.Background())
	is.NoErr(err)

	for i := 0; i < 100; i++ {
		err = destination.WriteAsync(context.Background(), sdk.Record{
			Payload: sdk.RawData([]byte("hello")),
		}, func(err error) error {
			return err
		})
		is.NoErr(err)
	}

	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_WriteAsync_MessagesCountAreNotEqualToBatchSize(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_write_async_jetstream_100_messages_50_batch_size",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(context.Background(), map[string]string{
		config.KeyURLs:     test.TestURL,
		config.KeySubject:  "foo_destination_write_async_jetstream_100_messages_50_batch_size",
		ConfigKeyBatchSize: "50",
	})
	is.NoErr(err)

	err = destination.Open(context.Background())
	is.NoErr(err)

	for i := 0; i < 100; i++ {
		err = destination.WriteAsync(context.Background(), sdk.Record{
			Payload: sdk.RawData([]byte("hello")),
		}, func(err error) error {
			return err
		})
		is.NoErr(err)
	}

	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

func TestDestination_WriteAsync_MessagesCountOneBatchSizeOne(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_write_async_jetstream_1_message_1_batch_size",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(context.Background(), map[string]string{
		config.KeyURLs:     test.TestURL,
		config.KeySubject:  "foo_destination_write_async_jetstream_1_message_1_batch_size",
		ConfigKeyBatchSize: "1",
	})
	is.NoErr(err)

	err = destination.Open(context.Background())
	is.NoErr(err)

	record := sdk.Record{
		Payload: sdk.RawData([]byte("hello")),
	}

	err = destination.WriteAsync(context.Background(), record, func(err error) error {
		return err
	})
	is.Equal(err, sdk.ErrUnimplemented)

	err = destination.Teardown(context.Background())
	is.NoErr(err)
}
