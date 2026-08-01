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
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/internal/connectprotocol"
)

// Envelope flag constants.
const (
	flagEnvelopeData       uint8 = 0x00
	flagEnvelopeCompressed uint8 = connectprotocol.FlagEnvelopeCompressed
	flagEnvelopeEndStream  uint8 = connectprotocol.FlagEnvelopeEndStream
	flagEnvelopeHeaders    uint8 = 0x04
)

func errorForWire(err error) *connect.Error {
	var cerr *connect.Error
	if errors.As(err, &cerr) {
		if cerr.IsRemote() {
			return connect.Errorf(connect.CodeInternal, "").WithCause(err)
		}
		return cerr
	}
	switch {
	case errors.Is(err, context.Canceled):
		return connect.Errorf(connect.CodeCanceled, "").WithCause(err)
	case errors.Is(err, context.DeadlineExceeded):
		return connect.Errorf(connect.CodeDeadlineExceeded, "").WithCause(err)
	}
	return connect.Errorf(connect.CodeUnknown, "").WithCause(err)
}

func marshalHeaders(header http.Header) ([]byte, error) {
	return connectprotocol.MarshalHeaders(header)
}

func unmarshalHeaders(data []byte) (http.Header, error) {
	return connectprotocol.UnmarshalHeaders(data)
}

func marshalEndStream(err *connect.Error, trailers http.Header) ([]byte, error) {
	var errVal error
	if err != nil {
		errVal = err
	}
	return connectprotocol.MarshalEndStream(errVal, trailers)
}

func unmarshalEndStream(data []byte) (*connect.Error, http.Header, error) {
	return connectprotocol.UnmarshalEndStream(data)
}
