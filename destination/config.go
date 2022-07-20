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
	"fmt"
	"strconv"
	"time"

	"github.com/conduitio-labs/conduit-connector-nats-jetstream/config"
	"github.com/conduitio-labs/conduit-connector-nats-jetstream/validator"
)

const (
	// defaultBatchSize is the default batch size,
	// it's equal to 1 which means that each message will be published synchronously.
	// Otherwise messages will be published asynchronously.
	defaultBatchSize = 1
	// defaultRetryWait is the default retry wait time when ErrNoResponders is encountered.
	defaultRetryWait = time.Second * 5
	// defaultRetryAttempts is the retry number of attempts when ErrNoResponders is encountered.
	defaultRetryAttempts = 3
)

const (
	// ConfigKeyBatchSize is a config name for a batch size.
	ConfigKeyBatchSize = "batchSize"
	// ConfigKeyRetryWait is a config name for a retry wait duration.
	ConfigKeyRetryWait = "retryWait"
	// ConfigKeyRetryAttempts is a config name for a retry attempts count.
	ConfigKeyRetryAttempts = "retryAttempts"
)

// Config holds destination specific configurable values.
type Config struct {
	config.Config

	// BatchSize is a message batch size used with JetStream mode for async message writes.
	// If it's equal to 1 messages will be published synchronously.
	BatchSize     int           `key:"batchSize" validate:"min=1"`
	RetryWait     time.Duration `key:"retryWait"`
	RetryAttempts int           `key:"retryAttempts"`
}

// Parse maps the incoming map to the Config and validates it.
func Parse(cfg map[string]string) (Config, error) {
	common, err := config.Parse(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("parse common config: %w", err)
	}

	destinationConfig := Config{
		Config: common,
	}

	if err := destinationConfig.parseFields(cfg); err != nil {
		return Config{}, fmt.Errorf("parse fields: %w", err)
	}

	if err := validator.Validate(&destinationConfig); err != nil {
		return Config{}, fmt.Errorf("validate destination config: %w", err)
	}

	return destinationConfig, nil
}

// parseFields parses non-string fields and set default values for empty fields.
func (c *Config) parseFields(cfg map[string]string) error {
	c.BatchSize = defaultBatchSize
	if cfg[ConfigKeyBatchSize] != "" {
		batchSize, err := strconv.Atoi(cfg[ConfigKeyBatchSize])
		if err != nil {
			return fmt.Errorf("parse %q: %w", ConfigKeyBatchSize, err)
		}

		c.BatchSize = batchSize
	}

	c.RetryWait = defaultRetryWait
	if cfg[ConfigKeyRetryWait] != "" {
		retryWait, err := time.ParseDuration(cfg[ConfigKeyRetryWait])
		if err != nil {
			return fmt.Errorf("parse %q: %w", ConfigKeyRetryWait, err)
		}

		c.RetryWait = retryWait
	}

	c.RetryAttempts = defaultRetryAttempts
	if cfg[ConfigKeyRetryAttempts] != "" {
		retryAttempts, err := strconv.Atoi(cfg[ConfigKeyRetryAttempts])
		if err != nil {
			return fmt.Errorf("parse %q: %w", ConfigKeyRetryAttempts, err)
		}

		c.RetryAttempts = retryAttempts
	}

	return nil
}
