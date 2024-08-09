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

//go:generate paramgen -output=paramgen.go Config

package destination

import (
	"context"
	"errors"
	"time"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	commonscfg "github.com/conduitio/conduit-commons/config"
	sdk "github.com/conduitio/conduit-connector-sdk"
)

var errNegativeRetryWait = errors.New("RetryWait can't be a negative value")

// Config holds destination specific configurable values.
type Config struct {
	config.Config

	// RetryWait is the retry wait time after a failure to send a message.
	RetryWait time.Duration `json:"retryWait" default:"5s"`
	// RetryAttempts is the number of attempts to send a message after a failure.
	RetryAttempts int `json:"retryAttempts" validate:"greater-than=0" default:"3"`
}

func ParseConfig(ctx context.Context, cfg commonscfg.Config, parameters commonscfg.Parameters) (Config, error) {
	parsedCfg := Config{
		Config: config.Config{
			// todo get connector ID from ctx
			ConnectionName: "connector-id",
		},
	}

	err := sdk.Util.ParseConfig(ctx, cfg, &parsedCfg, parameters)
	if err != nil {
		return Config{}, err
	}

	err = parsedCfg.Validate()
	if err != nil {
		return Config{}, err
	}

	return parsedCfg, nil
}

func (c *Config) Validate() error {
	var errs []error

	if err := c.Config.Validate(); err != nil {
		errs = append(errs, err)
	}

	if c.RetryWait < 0 {
		errs = append(errs, errNegativeRetryWait)
	}

	return errors.Join(errs...)
}
