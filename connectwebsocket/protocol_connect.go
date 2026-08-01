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

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	framev1 "connectrpc.com/connect/v2/internal/gen/connect/frame/v1"
	"google.golang.org/protobuf/types/known/anypb"
)

// Envelope flag constants.
const (
	flagEnvelopeData       uint8 = 0x00
	flagEnvelopeCompressed uint8 = 0x01
	flagEnvelopeEndStream  uint8 = 0x02
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

func headersToProto(h http.Header) *framev1.Headers {
	fields := make([]*framev1.Header, 0, len(h))
	for k, vs := range h {
		fields = append(fields, &framev1.Header{
			Key:    k,
			Values: vs,
		})
	}
	return &framev1.Headers{Fields: fields}
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

func endStreamToProto(err *connect.Error, trailers http.Header) *framev1.EndStream {
	out := &framev1.EndStream{}
	if err != nil {
		var details []*framev1.ErrorDetail
		for _, detail := range err.Details() {
			anyDetail := connectproto.ErrorDetailToAny(detail)
			details = append(details, &framev1.ErrorDetail{
				Type:  anyDetail.TypeUrl,
				Value: anyDetail.Value,
			})
		}
		out.Error = &framev1.Error{
			Code:    err.Code().String(),
			Message: err.Message(),
			Details: details,
		}
	}
	if len(trailers) > 0 {
		out.Metadata = headersToProto(trailers)
	}
	return out
}

func protoToEndStream(protoEndStream *framev1.EndStream) (*connect.Error, http.Header) {
	var err *connect.Error
	if protoEndStream.Error != nil {
		var code connect.Code
		_ = code.UnmarshalText([]byte(protoEndStream.Error.Code))
		if code == 0 {
			code = connect.CodeUnknown
		}
		cerr := connect.NewError(code, protoEndStream.Error.Message)
		for _, detail := range protoEndStream.Error.Details {
			typeURL := detail.Type
			if !strings.Contains(typeURL, "/") {
				typeURL = "type.googleapis.com/" + typeURL
			}
			anyDetail := &anypb.Any{
				TypeUrl: typeURL,
				Value:   detail.Value,
			}
			if errDetail, err := connectproto.NewErrorDetail(anyDetail); err == nil {
				cerr = cerr.WithDetail(errDetail)
			}
		}
		err = cerr.WithRemote()
	}
	var trailers http.Header
	if protoEndStream.Metadata != nil {
		trailers = protoToHeaders(protoEndStream.Metadata)
	}
	return err, trailers
}
