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
					ConfigKeyURLs:    "nats://127.0.0.1:1222,nats://127.0.0.1:1223,nats://127.0.0.1:1224",
					ConfigKeySubject: "foo",
					ConfigKeyMode:    "pubsub",
				},
			},
			want: Config{
				URLs:    []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223", "nats://127.0.0.1:1224"},
				Subject: "foo",
				Mode:    "pubsub",
			},
			wantErr: false,
		},
		{
			name: "success, only required fields provided, one connection URL",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:    "nats://127.0.0.1:1222",
					ConfigKeySubject: "foo",
					ConfigKeyMode:    "pubsub",
				},
			},
			want: Config{
				URLs:    []string{"nats://127.0.0.1:1222"},
				Subject: "foo",
				Mode:    "pubsub",
			},
			wantErr: false,
		},
		{
			name: "success, url with token",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:    "nats://token:127.0.0.1:1222",
					ConfigKeySubject: "foo",
					ConfigKeyMode:    "pubsub",
				},
			},
			want: Config{
				URLs:    []string{"nats://token:127.0.0.1:1222"},
				Subject: "foo",
				Mode:    "pubsub",
			},
			wantErr: false,
		},
		{
			name: "success, url with user/password",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:    "nats://admin:admin@127.0.0.1:1222",
					ConfigKeySubject: "foo",
					ConfigKeyMode:    "pubsub",
				},
			},
			want: Config{
				URLs:    []string{"nats://admin:admin@127.0.0.1:1222"},
				Subject: "foo",
				Mode:    "pubsub",
			},
			wantErr: false,
		},
		{
			name: "fail, required field (subject) is missing",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs: "nats://localhost:1222",
					ConfigKeyMode: "pubsub",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, invalid url",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:    "notaurl",
					ConfigKeySubject: "foo",
					ConfigKeyMode:    "pubsub",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, mode is jetstream",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:    "nats://127.0.0.1:1222",
					ConfigKeySubject: "foo",
					ConfigKeyMode:    "jetstream",
				},
			},
			want: Config{
				URLs:    []string{"nats://127.0.0.1:1222"},
				Subject: "foo",
				Mode:    "jetstream",
			},
			wantErr: false,
		},
		{
			name: "fail, mode is unknown",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:    "nats://127.0.0.1:1222",
					ConfigKeySubject: "foo",
					ConfigKeyMode:    "reply",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "fail, tlsClientCertPath without tlsClientPrivateKeyPath",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:              "nats://127.0.0.1:1222",
					ConfigKeySubject:           "foo",
					ConfigKeyMode:              "pubsub",
					ConfigKeyTLSClientCertPath: "./config.go",
				},
			},
			want:    Config{},
			wantErr: true,
		},
		{
			name: "success, nkey pair",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:     "nats://127.0.0.1:1222",
					ConfigKeySubject:  "foo",
					ConfigKeyMode:     "pubsub",
					ConfigKeyNKeyPath: "./config.go",
				},
			},
			want: Config{
				URLs:     []string{"nats://127.0.0.1:1222"},
				Subject:  "foo",
				Mode:     "pubsub",
				NKeyPath: "./config.go",
			},
			wantErr: false,
		},
		{
			name: "success, credentials file",
			args: args{
				cfg: map[string]string{
					ConfigKeyURLs:                "nats://127.0.0.1:1222",
					ConfigKeySubject:             "foo",
					ConfigKeyMode:                "pubsub",
					ConfigKeyCredentialsFilePath: "./config.go",
				},
			},
			want: Config{
				URLs:                []string{"nats://127.0.0.1:1222"},
				Subject:             "foo",
				Mode:                "pubsub",
				CredentialsFilePath: "./config.go",
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parse() = %v, want %v", got, tt.want)
			}
		})
	}
}
