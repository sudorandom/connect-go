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
	"strings"
	"sync"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/internal/connectprotocol"
	framev1 "connectrpc.com/connect/v2/internal/gen/connect/frame/v1"
)

var headersProtoPool = sync.Pool{
	New: func() any {
		return &framev1.Headers{}
	},
}

var headerFieldPool = sync.Pool{
	New: func() any {
		return &framev1.Header{}
	},
}

func getHeaderField(key string, values []string) *framev1.Header {
	h := headerFieldPool.Get().(*framev1.Header)
	h.Key = key
	h.Values = values
	return h
}

func putHeaderField(h *framev1.Header) {
	h.Key = ""
	h.Values = nil
	headerFieldPool.Put(h)
}

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

func headersToProto(h http.Header) (*framev1.Headers, func()) {
	headersProto := headersProtoPool.Get().(*framev1.Headers)
	if cap(headersProto.Fields) < len(h) {
		headersProto.Fields = make([]*framev1.Header, 0, len(h))
	} else {
		headersProto.Fields = headersProto.Fields[:0]
	}

	for k, vs := range h {
		headersProto.Fields = append(headersProto.Fields, getHeaderField(k, vs))
	}

	cleanup := func() {
		for _, f := range headersProto.Fields {
			putHeaderField(f)
		}
		headersProto.Fields = headersProto.Fields[:0]
		headersProtoPool.Put(headersProto)
	}

	return headersProto, cleanup
}

func protoToHeaders(protoHeaders *framev1.Headers) http.Header {
	headers := make(http.Header)
	for _, field := range protoHeaders.Fields {
		if strings.HasPrefix(field.Key, ":") {
			headers[field.Key] = field.Values
		} else {
			headers[http.CanonicalHeaderKey(field.Key)] = field.Values
		}
	}
	return headers
}

func marshalEndStream(err *connect.Error, trailers http.Header) ([]byte, error) {
	return connectprotocol.MarshalEndStream(err, trailers)
}

func unmarshalEndStream(data []byte) (*connect.Error, http.Header, error) {
	return connectprotocol.UnmarshalEndStream(data)
}
