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
	"github.com/nats-io/nats.go"
	"testing"

	commonscfg "github.com/conduitio/conduit-commons/config"
	"github.com/matryer/is"
)

func TestParse_Durable_Default(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	rawCfg := commonscfg.Config{
		"urls":    "nats://127.0.0.1:1222",
		"subject": "test-subject",
		"stream":  "test-stream",
	}

	parsed, err := ParseConfig(ctx, rawCfg, NewSource().Parameters())
	is.NoErr(err)
	is.True(parsed.Durable != "")
}

func TestParse_Durable_Custom(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	rawCfg := commonscfg.Config{
		"urls":    "nats://127.0.0.1:1222",
		"subject": "test-subject",
		"stream":  "test-stream",
		"durable": "foobar",
	}

	parsed, err := ParseConfig(ctx, rawCfg, NewSource().Parameters())
	is.NoErr(err)
	is.Equal(rawCfg["durable"], parsed.Durable)
}

func TestParse_DeliverSubject_Default(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	rawCfg := commonscfg.Config{
		"urls":    "nats://127.0.0.1:1222",
		"subject": "test-subject",
		"stream":  "test-stream",
	}

	parsed, err := ParseConfig(ctx, rawCfg, NewSource().Parameters())
	is.NoErr(err)
	is.True(parsed.DeliverSubject != "")
}

func TestParse_DeliverSubject_Custom(t *testing.T) {
	is := is.New(t)
	ctx := context.Background()
	rawCfg := commonscfg.Config{
		"urls":           "nats://127.0.0.1:1222",
		"subject":        "test-subject",
		"stream":         "test-stream",
		"deliverSubject": "foobar",
	}

	parsed, err := ParseConfig(ctx, rawCfg, NewSource().Parameters())
	is.NoErr(err)
	is.Equal(rawCfg["deliverSubject"], parsed.DeliverSubject)
}

func TestParse_AckPolicy(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  nats.AckPolicy
	}{
		{
			name: "default (explicit)",
			want: nats.AckExplicitPolicy,
		},
		{
			name:  "none",
			input: "none",
			want:  nats.AckNonePolicy,
		},
		{
			name:  "explicit",
			input: "explicit",
			want:  nats.AckExplicitPolicy,
		},
		{
			name:  "all",
			input: "all",
			want:  nats.AckAllPolicy,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			rawCfg := commonscfg.Config{
				"urls":      "nats://127.0.0.1:1222",
				"subject":   "test-subject",
				"stream":    "test-stream",
				"ackPolicy": tc.input,
			}

			parsed, err := ParseConfig(ctx, rawCfg, NewSource().Parameters())
			is.NoErr(err)
			is.Equal(tc.want, parsed.NATSAckPolicy())
		})
	}
}

func TestParse_DeliverPolicy(t *testing.T) {
	testCases := []struct {
		name  string
		input string
		want  nats.DeliverPolicy
	}{
		{
			name: "default (all)",
			want: nats.DeliverAllPolicy,
		},
		{
			name:  "all",
			input: "all",
			want:  nats.DeliverAllPolicy,
		},
		{
			name:  "new",
			input: "new",
			want:  nats.DeliverNewPolicy,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			is := is.New(t)
			ctx := context.Background()
			rawCfg := commonscfg.Config{
				"urls":          "nats://127.0.0.1:1222",
				"subject":       "test-subject",
				"stream":        "test-stream",
				"deliverPolicy": tc.input,
			}

			parsed, err := ParseConfig(ctx, rawCfg, NewSource().Parameters())
			is.NoErr(err)
			is.Equal(tc.want, parsed.NATSDeliverPolicy())
		})
	}
}
