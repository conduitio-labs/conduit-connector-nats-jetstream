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
	"encoding/json"
	"fmt"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

// position defines a position model for the JetStream iterator.
type position struct {
	// OptSeq is a position of a message in a stream.
	OptSeq uint64 `json:"opt_seq"`
}

// marshalPosition marshals the underlying position into a sdk.Position as JSON bytes.
func (p position) marshalSDKPosition() (sdk.Position, error) {
	positionBytes, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal position: %w", err)
	}

	return sdk.Position(positionBytes), nil
}

// parsePosition converts an sdk.Position into a position.
func parsePosition(sdkPosition sdk.Position) (position, error) {
	var p position

	if sdkPosition == nil {
		return p, nil
	}

	if err := json.Unmarshal(sdkPosition, &p); err != nil {
		return position{}, fmt.Errorf("unmarshal sdk.Position into Position: %w", err)
	}

	return p, nil
}
