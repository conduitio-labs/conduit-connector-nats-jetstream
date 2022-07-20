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

package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestParse(t *testing.T) {
	t.Parallel()

	type args struct {
		cfg map[string]string
	}

	tests := []struct {
		name    string
		args    args
		want    Config
		wantErr bool
	}{
		{
			name: "success, only required fields provided, many connection URLs",
			args: args{
				cfg: map[string]string{
					KeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					KeySubject: "foo",
				},
			},
			want: Config{
				URLs:    []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
				Subject: "foo",
			},
			wantErr: false,
		},
		{
			name: "success, only required fields provided, one connection URL",
			args: args{
				cfg: map[string]string{
					KeyURLs:    "nats://127.0.0.1:1222",
					KeySubject: "foo",
				},
			},
			want: Config{
				URLs:    []string{"nats://127.0.0.1:1222"},
				Subject: "foo",
			},
			wantErr: false,
		},
		{
			name: "success, url with token",
			args: args{
				cfg: map[string]string{
					KeyURLs:    "nats://token:127.0.0.1:1222",
					KeySubject: "foo",
				},
			},
			want: Config{
				URLs:    []string{"nats://token:127.0.0.1:1222"},
				Subject: "foo",
			},
			wantErr: false,
		},
		{
			name: "success, url with user/password",
			args: args{
				cfg: map[string]string{
					KeyURLs:    "nats://admin:admin@127.0.0.1:1222",
					KeySubject: "foo",
				},
			},
			want: Config{
				URLs:    []string{"nats://admin:admin@127.0.0.1:1222"},
				Subject: "foo",
			},
			wantErr: false,
		},
		{
			name: "fail, required field (subject) is missing",
			args: args{
				cfg: map[string]string{
					KeyURLs: "nats://localhost:1222",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid url",
			args: args{
				cfg: map[string]string{
					KeyURLs:    "notaurl",
					KeySubject: "foo",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, tls.clientCertPath without tls.clientPrivateKeyPath",
			args: args{
				cfg: map[string]string{
					KeyURLs:              "nats://127.0.0.1:1222",
					KeySubject:           "foo",
					KeyTLSClientCertPath: "./config.go",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, nkey pair",
			args: args{
				cfg: map[string]string{
					KeyURLs:     "nats://127.0.0.1:1222",
					KeySubject:  "foo",
					KeyNKeyPath: "./config.go",
				},
			},
			want: Config{
				URLs:     []string{"nats://127.0.0.1:1222"},
				Subject:  "foo",
				NKeyPath: "./config.go",
			},
			wantErr: false,
		},
		{
			name: "success, credentials file",
			args: args{
				cfg: map[string]string{
					KeyURLs:                "nats://127.0.0.1:1222",
					KeySubject:             "foo",
					KeyCredentialsFilePath: "./config.go",
				},
			},
			want: Config{
				URLs:                []string{"nats://127.0.0.1:1222"},
				Subject:             "foo",
				CredentialsFilePath: "./config.go",
			},
			wantErr: false,
		},
		{
			name: "success, custom connection name",
			args: args{
				cfg: map[string]string{
					KeyURLs:           "nats://127.0.0.1:1222",
					KeySubject:        "foo",
					KeyConnectionName: "my_super_connection",
				},
			},
			want: Config{
				URLs:           []string{"nats://127.0.0.1:1222"},
				Subject:        "foo",
				ConnectionName: "my_super_connection",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		tt := tt

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Parse(tt.args.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parse() error = %v, wantErr %v", err, tt.wantErr)

				return
			}

			if strings.HasPrefix(got.ConnectionName, DefaultConnectionNamePrefix) {
				tt.want.ConnectionName = got.ConnectionName
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
