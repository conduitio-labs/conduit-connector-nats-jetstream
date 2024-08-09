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

package nats

import (
	"testing"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/test"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/matryer/is"
	"go.uber.org/goleak"
)

type driver struct {
	sdk.ConfigurableAcceptanceTestDriver
}

func (d driver) GenerateRecord(t *testing.T, operation opencdc.Operation) opencdc.Record {
	record := d.ConfigurableAcceptanceTestDriver.GenerateRecord(t, operation)
	// we don't need key for NATS JetStream
	record.Key = nil

	return record
}

//nolint:paralleltest // we don't need the paralleltest here
func TestAcceptance(t *testing.T) {
	sourceCfg := map[string]string{
		"urls":   test.TestURL,
		"stream": "test",
	}
	destCfg := map[string]string{
		"urls":    test.TestURL,
		"subject": "test",
	}

	sdk.AcceptanceTest(t, driver{
		ConfigurableAcceptanceTestDriver: sdk.ConfigurableAcceptanceTestDriver{
			Config: sdk.ConfigurableAcceptanceTestDriverConfig{
				Connector:         Connector,
				SourceConfig:      sourceCfg,
				DestinationConfig: destCfg,
				BeforeTest:        beforeTest(sourceCfg, destCfg),
				Skip:              []string{},
				GoleakOptions: []goleak.Option{
					// nats.go spawns a separate goroutine to process flush requests
					// and we have no chance to stop it using the library's API
					goleak.IgnoreTopFunction("github.com/nats-io/nats%2ego.(*Conn).flusher"),
					goleak.IgnoreTopFunction("sync.runtime_notifyListWait"),
					goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
				},
			},
		},
	})
}

// beforeTest creates new stream before each test.
func beforeTest(sourceCfg, destCfg map[string]string) func(t *testing.T) {
	return func(t *testing.T) {
		is := is.New(t)

		conn, err := test.GetTestConnection()
		is.NoErr(err)

		streamName := sourceCfg["stream"] + "-" + uuid.New().String()
		subject := t.Name() + uuid.New().String()

		err = test.CreateTestStream(conn, streamName, []string{subject})
		is.NoErr(err)

		sourceCfg["subject"] = subject
		destCfg["subject"] = subject
	}
}
