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
	"testing"

	"github.com/matryer/is"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{
			name: "success, only required fields provided, many connection URLs",
			cfg: Config{
				URLs:    []string{"nats://127.0.0.1:1222", "nats://127.0.0.1:1223"},
				Subject: "foo",
			},
			wantErr: false,
		},
		{
			name: "success, only required fields provided, one connection URL",
			cfg: Config{
				URLs:    []string{"nats://127.0.0.1:1222"},
				Subject: "foo",
			},
			wantErr: false,
		},
		{
			name: "success, url with token",
			cfg: Config{
				URLs:    []string{"nats://token:127.0.0.1:1222"},
				Subject: "foo",
			},
			wantErr: false,
		},
		{
			name: "success, url with user/password",
			cfg: Config{
				URLs:    []string{"nats://admin:admin@127.0.0.1:1222"},
				Subject: "foo",
			},
			wantErr: false,
		},

		{
			name: "fail, invalid url",
			cfg: Config{
				URLs:    []string{"foo"},
				Subject: "foo",
			},
			wantErr: true,
		},
		{
			name: "fail, empty url",
			cfg: Config{
				URLs:    []string{""},
				Subject: "foo",
			},
			wantErr: true,
		},
		{
			name: "fail, tls.clientCertPath without tls.clientPrivateKeyPath",
			cfg: Config{
				URLs:    []string{"nats://127.0.0.1:1222"},
				Subject: "foo",
				ConfigTLS: ConfigTLS{
					TLSClientCertPath: "./client-cert-path",
				},
			},
			wantErr: true,
		},
		{
			name: "fail, tls.clientPrivateKeyPath without tls.clientCertPath",
			cfg: Config{
				URLs:    []string{"nats://127.0.0.1:1222"},
				Subject: "foo",
				ConfigTLS: ConfigTLS{
					TLSClientPrivateKeyPath: "./private-key-path",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			is := is.New(t)

			err := tt.cfg.Validate()
			if tt.wantErr {
				is.True(err != nil)
			} else {
				is.NoErr(err)
			}
		})
	}
}
