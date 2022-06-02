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

package iterator

import (
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

func TestPubSubIterator_messageToRecord(t *testing.T) {
	t.Parallel()

	type args struct {
		msg *nats.Msg
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
		want    sdk.Record
	}{
		{
			name: "success",
			args: args{
				msg: &nats.Msg{
					Subject: "foo",
					Data:    []byte("sample"),
				},
			},
			wantErr: false,
			want: sdk.Record{
				Payload: sdk.RawData([]byte("sample")),
			},
		},
		{
			name: "success, nil data",
			args: args{
				msg: &nats.Msg{
					Subject: "foo",
				},
			},
			wantErr: false,
			want: sdk.Record{
				Payload: sdk.RawData(nil),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			i := &PubSubIterator{}

			got, err := i.messageToRecord(tt.args.msg)
			if (err != nil) != tt.wantErr {
				t.Errorf("PubSubIterator.messageToRecord() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			// we don't care about time
			tt.want.CreatedAt = got.CreatedAt

			// check if the position is a valid UUID
			_, err = uuid.FromBytes(got.Position)
			if err != nil {
				t.Errorf("uuid.ParseBytes() = %v", err)

				return
			}

			tt.want.Position = got.Position

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("PubSubIterator.messageToRecord() = %v, want %v", got, tt.want)
			}
		})
	}
}
