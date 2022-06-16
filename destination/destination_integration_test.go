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

func TestDestination_Open(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination",
	})
	is.NoErr(err)

	t.Run("success, jetstream", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		destination := NewDestination()

		err := destination.Configure(context.Background(), map[string]string{
			config.ConfigKeyURLs:    test.TestURL,
			config.ConfigKeySubject: "foo_destination",
		})
		is.NoErr(err)

		err = destination.Open(context.Background())
		is.NoErr(err)

		err = destination.Teardown(context.Background())
		is.NoErr(err)
	})

	t.Run("fail, url is pointed to a non-existent server", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		destination := NewDestination()

		err := destination.Configure(context.Background(), map[string]string{
			config.ConfigKeyURLs:    "nats://localhost:6666",
			config.ConfigKeySubject: "foo_destination",
		})
		is.NoErr(err)

		err = destination.Open(context.Background())
		is.True(err != nil)

		err = destination.Teardown(context.Background())
		is.NoErr(err)
	})
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

	t.Run("success, jetstream sync write, 1 message, batch size is 1", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		destination := NewDestination()

		err := destination.Configure(context.Background(), map[string]string{
			config.ConfigKeyURLs:    test.TestURL,
			config.ConfigKeySubject: "foo_destination_write_jetstream",
			ConfigKeyBatchSize:      "1",
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
	})
}

func TestDestination_WriteAsync(t *testing.T) {
	t.Parallel()

	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_write_async_jetstream",
	})
	is.NoErr(err)

	t.Run("success, jetstream async write, 100 messages, batch size is 100", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		destination := NewDestination()

		err := destination.Configure(context.Background(), map[string]string{
			config.ConfigKeyURLs:    test.TestURL,
			config.ConfigKeySubject: "foo_destination_write_async_jetstream",
			ConfigKeyBatchSize:      "100",
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
	})

	t.Run("success, jetstream async write, 100 messages, batch size is 50", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		destination := NewDestination()

		err := destination.Configure(context.Background(), map[string]string{
			config.ConfigKeyURLs:    test.TestURL,
			config.ConfigKeySubject: "foo_destination_write_async_jetstream",
			ConfigKeyBatchSize:      "50",
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
	})

	t.Run("fail, jetstream try async write, 1 message, batch is 1", func(t *testing.T) {
		t.Parallel()

		is := is.New(t)

		destination := NewDestination()

		err := destination.Configure(context.Background(), map[string]string{
			config.ConfigKeyURLs:    test.TestURL,
			config.ConfigKeySubject: "foo_destination_write_async_jetstream",
			ConfigKeyBatchSize:      "1",
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
	})
}
