// Copyright 2021-2026 The Connect Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connectwebsocket

import (
	"encoding/binary"
	"errors"
	"io"
)

const envelopeLen = 5

func writeEnvelope(writer io.Writer, flag uint8, payload []byte) error {
	if uint64(len(payload)) > 4294967295 {
		return errors.New("connectwebsocket: payload too large")
	}
	var head [envelopeLen]byte
	head[0] = flag
	//nolint:gosec // envelope length is bounded
	binary.BigEndian.PutUint32(head[1:5], uint32(len(payload)))
	if _, err := writer.Write(head[:]); err != nil {
		return err
	}
	if len(payload) > 0 {
		if _, err := writer.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

func readEnvelope(reader io.Reader) (uint8, []byte, error) {
	var head [envelopeLen]byte
	if _, err := io.ReadFull(reader, head[:]); err != nil {
		return 0, nil, err
	}
	flag := head[0]
	length := binary.BigEndian.Uint32(head[1:5])
	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(reader, payload); err != nil {
			return 0, nil, err
		}
	}
	return flag, payload, nil
}
