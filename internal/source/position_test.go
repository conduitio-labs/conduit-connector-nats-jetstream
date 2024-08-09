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
	"github.com/conduitio/conduit-commons/opencdc"
	"reflect"
	"testing"
)

func Test_position_marshalPosition(t *testing.T) {
	tests := []struct {
		name    string
		fields  position
		want    opencdc.Position
		wantErr bool
	}{
		{
			name: "success, all fields",
			fields: position{
				OptSeq: 32,
			},
			want: opencdc.Position(
				`{"opt_seq":32}`,
			),
			wantErr: false,
		},
		{
			name:   "success, empty",
			fields: position{},
			want: opencdc.Position(
				`{"opt_seq":0}`,
			),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			p := position{
				OptSeq: tt.fields.OptSeq,
			}

			got, err := p.marshalSDKPosition()
			if (err != nil) != tt.wantErr {
				t.Errorf("position.marshalPosition() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("position.marshalPosition() = %v, want %v", string(got), string(tt.want))
			}
		})
	}
}

func Test_parsePosition(t *testing.T) {
	type args struct {
		sdkPosition opencdc.Position
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
				sdkPosition: opencdc.Position([]byte(
					`{"opt_seq":32}`,
				)),
			},
			want: position{
				OptSeq: 32,
			},
			wantErr: false,
		},
		{
			name: "success, empty",
			args: args{
				sdkPosition: opencdc.Position([]byte(
					`{}`,
				)),
			},
			want: position{
				OptSeq: 0,
			},
			wantErr: false,
		},
		{
			name: "success, position is nil",
			args: args{
				sdkPosition: opencdc.Position(nil),
			},
			want: position{
				OptSeq: 0,
			},
			wantErr: false,
		},
		{
			name: "fail, wrong field type",
			args: args{
				sdkPosition: opencdc.Position([]byte(
					`{"opt_seq":"32"}`,
				)),
			},
			want:    position{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
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
