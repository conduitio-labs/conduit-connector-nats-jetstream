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

//go:build integration

package destination

import (
	"context"
	"github.com/conduitio/conduit-commons/opencdc"
	"testing"

	test "github.com/conduitio-labs/conduit-connector-nats-jetstream/test"
	"github.com/matryer/is"
)

func TestDestination_Open_Success(t *testing.T) {
	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_success",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(context.Background(), map[string]string{
		"urls":    test.TestURL,
		"subject": "foo_destination_success",
	})
	is.NoErr(err)

	err = destination.Open(context.Background())
	is.NoErr(err)

	err = destination.Teardown(context.Background())
	is.NoErr(err)
}

// TestDestination_Open_Fail should fail because of the wrong URL.
func TestDestination_Open_Fail(t *testing.T) {
	is := is.New(t)

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_fail",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(context.Background(), map[string]string{
		"urls":    "nats://localhost:6666",
		"subject": "foo_destination_fail",
	})
	is.NoErr(err)

	err = destination.Open(context.Background())
	is.True(err != nil)
}

func TestIntegrationDestination_Write(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()

	conn, err := test.GetTestConnection()
	is.NoErr(err)

	err = test.CreateTestStream(conn, t.Name(), []string{
		"foo_destination_write_jetstream",
	})
	is.NoErr(err)

	destination := NewDestination()

	err = destination.Configure(ctx, map[string]string{
		"urls":    test.TestURL,
		"subject": "foo_destination_write_jetstream",
	})
	is.NoErr(err)

	err = destination.Open(ctx)
	is.NoErr(err)

	var written int
	written, err = destination.Write(ctx, []opencdc.Record{
		{
			Payload: opencdc.Change{
				After: opencdc.RawData([]byte("hello")),
			},
		},
	})
	is.NoErr(err)
	is.Equal(written, 1)

	err = destination.Teardown(ctx)
	is.NoErr(err)
}
