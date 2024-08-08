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

package destination

import (
	"errors"
	"time"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
)

var (
	errNegativeRetryWait = errors.New("RetryWait can't be a negative value")
)

// Config holds destination specific configurable values.
type Config struct {
	config.Config

	RetryWait     time.Duration `json:"retryWait" default:"5s"`
	RetryAttempts int           `json:"retryAttempts" validate:"greater-than=0" default:"3"`
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
