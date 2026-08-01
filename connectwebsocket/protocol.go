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
	"sync"
)

const (
	envelopeLen = 5
	maxPooledBufferSize = 64 * 1024 // 64 KiB
)

var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, maxPooledBufferSize)
		return &b
	},
}

func getBuffer(size int) ([]byte, *[]byte) {
	if size <= maxPooledBufferSize {
		p := bufferPool.Get().(*[]byte)
		return (*p)[:size], p
	}
	b := make([]byte, size)
	return b, nil
}

func putBuffer(p *[]byte) {
	if p != nil {
		bufferPool.Put(p)
	}
}

func writeEnvelope(writer io.Writer, flag uint8, payload []byte) error {
	if uint64(len(payload)) > 4294967295 {
		return errors.New("connectwebsocket: payload too large")
	}
	totalLen := envelopeLen + len(payload)
	buf, poolPtr := getBuffer(totalLen)
	defer putBuffer(poolPtr)

	buf[0] = flag
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(payload)))
	copy(buf[envelopeLen:], payload)
	_, err := writer.Write(buf)
	return err
}

func readEnvelope(reader io.Reader) (uint8, []byte, error) {
	var head [envelopeLen]byte
	if _, err := io.ReadFull(reader, head[:]); err != nil {
		return 0, nil, err
	}
	flag := head[0]
	length := binary.BigEndian.Uint32(head[1:5])
	if length == 0 {
		return flag, nil, nil
	}
	buf, poolPtr := getBuffer(int(length))
	if _, err := io.ReadFull(reader, buf); err != nil {
		putBuffer(poolPtr)
		return 0, nil, err
	}
	payload := make([]byte, length)
	copy(payload, buf)
	putBuffer(poolPtr)
	return flag, payload, nil
}
