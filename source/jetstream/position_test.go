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

package jetstream

import (
	"reflect"
	"testing"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

func Test_position_marshalPosition(t *testing.T) {
	t.Parallel()

	type fields struct {
		Durable string
		Stream  string
		Subject string
		OptSeq  uint64
	}
	tests := []struct {
		name    string
		fields  fields
		want    sdk.Position
		wantErr bool
	}{
		{
			name: "success, all fields",
			fields: fields{
				Durable: "conduit_push_consumer",
				Stream:  "FOO_STREAM",
				Subject: "foo_subject",
				OptSeq:  32,
			},
			want: sdk.Position(
				`{"durable":"conduit_push_consumer","stream":"FOO_STREAM","subject":"foo_subject","opt_seq":32}`,
			),
			wantErr: false,
		},
		{
			name:   "success, empty",
			fields: fields{},
			want: sdk.Position(
				`{"durable":"","stream":"","subject":"","opt_seq":0}`,
			),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := position{
				Durable: tt.fields.Durable,
				Stream:  tt.fields.Stream,
				Subject: tt.fields.Subject,
				OptSeq:  tt.fields.OptSeq,
			}

			got, err := p.marshalSDKPosition()
			if (err != nil) != tt.wantErr {
				t.Errorf("position.marshalPosition() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("position.marshalPosition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_parsePosition(t *testing.T) {
	t.Parallel()

	type args struct {
		sdkPosition sdk.Position
	}
	tests := []struct {
		name    string
		args    args
		want    position
		wantErr bool
	}{
		{
			name: "success, all fields",
			args: args{
				sdkPosition: sdk.Position([]byte(
					`{"durable":"conduit_push_consumer","stream":"FOO_STREAM","subject":"foo_subject","opt_seq":32}`,
				)),
			},
			want: position{
				Durable: "conduit_push_consumer",
				Stream:  "FOO_STREAM",
				Subject: "foo_subject",
				OptSeq:  32,
			},
			wantErr: false,
		},
		{
			name: "success, empty",
			args: args{
				sdkPosition: sdk.Position([]byte(
					`{"durable":"","stream":"","subject":"","opt_seq":0}`,
				)),
			},
			want: position{
				Durable: "",
				Stream:  "",
				Subject: "",
				OptSeq:  0,
			},
			wantErr: false,
		},
		{
			name: "success, position is nil",
			args: args{
				sdkPosition: sdk.Position(nil),
			},
			want: position{
				Durable: "",
				Stream:  "",
				Subject: "",
				OptSeq:  0,
			},
			wantErr: false,
		},
		{
			name: "fail, wrong field type",
			args: args{
				sdkPosition: sdk.Position([]byte(
					`{"durable":"conduit_push_consumer","stream":"FOO_STREAM","subject":"foo_subject","opt_seq":"32"}`,
				)),
			},
			want:    position{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parsePosition(tt.args.sdkPosition)
			if (err != nil) != tt.wantErr {
				t.Errorf("parsePosition() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parsePosition() = %v, want %v", got, tt.want)
			}
		})
	}
}
