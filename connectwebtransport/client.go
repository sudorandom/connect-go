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

package connectwebtransport

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect/v2"
	framev1 "connectrpc.com/connect/v2/internal/gen/connect/frame/v1"
	"github.com/quic-go/webtransport-go"
	"google.golang.org/protobuf/proto"
)

type clientStream struct {
	spec     connect.Spec
	stream   *webtransport.Stream
	callInfo *connect.CallInfo
	opts     transportOptions
	codec    connect.Codec
	ctx      context.Context

	sendHeadersOnce sync.Once
	sendHeadersErr  error

	closeSendOnce sync.Once

	recvHeadersOnce sync.Once
	recvHeadersErr  error

	rxEnd bool
}

func (cs *clientStream) SendHeaders() error {
	cs.sendHeadersOnce.Do(func() {
		cs.sendHeadersErr = cs.sendHeaders()
	})
	return cs.sendHeadersErr
}

func (cs *clientStream) sendHeaders() error {
	header := make(http.Header)
	// Copy user headers
	if cs.callInfo != nil && cs.callInfo.RequestHeader() != nil {
		maps.Insert(header, cs.callInfo.RequestHeader().All())
	}
	// Add pseudo-headers
	header.Set(":path", cs.spec.Procedure)
	header.Set("Content-Type", "application/connect+"+cs.codec.Name())
	if cs.opts.sendCompressor != "" && cs.opts.sendCompressor != connect.CompressionNameIdentity {
		header.Set("Connect-Content-Encoding", cs.opts.sendCompressor)
	}
	if cs.opts.acceptCompression != "" {
		header.Set("Connect-Accept-Encoding", cs.opts.acceptCompression)
	}
	if deadline, ok := cs.ctx.Deadline(); ok {
		if ms := time.Until(deadline).Milliseconds(); ms > 0 {
			header.Set("Connect-Timeout-Ms", strconv.FormatInt(ms, 10))
		}
	}

	protoHeaders := headersToProto(header)
	data, err := proto.Marshal(protoHeaders)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal headers: %v", err)
	}

	return writeEnvelope(cs.stream, flagEnvelopeHeaders, data)
}

func (cs *clientStream) Send(msg any) error {
	if err := cs.SendHeaders(); err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := cs.codec.MarshalWrite(cs.ctx, &buf, msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal request: %v", err)
	}
	payload := buf.Bytes()

	flag := flagEnvelopeData
	var compressor connect.Compressor
	if cs.opts.sendCompressor != "" && cs.opts.sendCompressor != connect.CompressionNameIdentity {
		compressor = cs.opts.compressors[cs.opts.sendCompressor]
	}

	if compressor != nil {
		var buf bytes.Buffer
		compWriter, err := compressor.Compress(&buf)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to compress request: %v", err)
		}
		if _, err := compWriter.Write(payload); err != nil {
			_ = compWriter.Close()
			return connect.Errorf(connect.CodeInternal, "failed to write compressed request: %v", err)
		}
		if err := compWriter.Close(); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to flush compressed request: %v", err)
		}
		payload = buf.Bytes()
		flag = flagEnvelopeCompressed
	}

	if cs.opts.sendMaxBytes > 0 && len(payload) > cs.opts.sendMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds send limit %d", len(payload), cs.opts.sendMaxBytes)
	}

	return writeEnvelope(cs.stream, flag, payload)
}

func (cs *clientStream) CloseSend() error {
	var err error
	cs.closeSendOnce.Do(func() {
		err = cs.stream.Close()
	})
	return err
}

func (cs *clientStream) Receive(msg any) error {
	// Send headers if we haven't already
	if err := cs.SendHeaders(); err != nil {
		return err
	}

	// Receive response headers first
	cs.recvHeadersOnce.Do(func() {
		cs.recvHeadersErr = cs.recvHeaders()
	})
	if cs.recvHeadersErr != nil {
		return cs.recvHeadersErr
	}

	if cs.rxEnd {
		return io.EOF
	}

	flag, payload, err := readEnvelope(cs.stream)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return connect.Errorf(connect.CodeInternal, "protocol error: unexpected EOF before end stream envelope")
		}
		return err
	}

	if flag == flagEnvelopeHeaders {
		// Unexpected headers frame after response headers
		return connect.Errorf(connect.CodeInternal, "protocol error: unexpected headers frame")
	}

	if flag == flagEnvelopeEndStream {
		cs.rxEnd = true
		cerr, trailers, err := unmarshalEndStream(payload)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to unmarshal end stream envelope: %v", err)
		}
		if cs.callInfo != nil && cs.callInfo.ResponseTrailer() != nil {
			for k, vs := range trailers {
				cs.callInfo.ResponseTrailer().SetValues(k, vs)
			}
		}
		if cerr != nil {
			return cerr
		}
		return io.EOF
	}

	if flag != flagEnvelopeData && flag != flagEnvelopeCompressed {
		return connect.Errorf(connect.CodeInternal, "protocol error: unknown frame flag: 0x%x", flag)
	}

	if cs.opts.readMaxBytes > 0 && len(payload) > cs.opts.readMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds read limit %d", len(payload), cs.opts.readMaxBytes)
	}

	if flag == flagEnvelopeCompressed {
		encoding := cs.callInfo.ResponseHeader().Get("Connect-Content-Encoding")
		compressor := cs.opts.compressors[encoding]
		if compressor == nil {
			return connect.Errorf(connect.CodeInternal, "protocol error: compressed message without Connect-Content-Encoding")
		}
		rc, err := compressor.Decompress(bytes.NewReader(payload))
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to decompress response: %v", err)
		}
		decompressed, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to read decompressed response: %v", err)
		}
		payload = decompressed
	}

	if err := cs.codec.UnmarshalRead(cs.ctx, bytes.NewReader(payload), msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal response: %v", err)
	}

	if cs.spec.StreamType == connect.StreamTypeUnary {
		if err := cs.readUnaryEndStream(); err != nil {
			return err
		}
	}
	return nil
}

func (cs *clientStream) recvHeaders() error {
	flag, payload, err := readEnvelope(cs.stream)
	if err != nil {
		return err
	}
	if flag != flagEnvelopeHeaders {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected headers frame first, got 0x%x", flag)
	}
	var protoHeaders framev1.Headers
	if err := proto.Unmarshal(payload, &protoHeaders); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal response headers: %v", err)
	}
	headers := protoToHeaders(&protoHeaders)
	if cs.callInfo != nil && cs.callInfo.ResponseHeader() != nil {
		for k, vs := range headers {
			// Skip pseudokeys
			if strings.HasPrefix(k, ":") {
				continue
			}
			cs.callInfo.ResponseHeader().SetValues(k, vs)
		}
	}
	return nil
}

func (cs *clientStream) readUnaryEndStream() error {
	nextFlag, nextPayload, nextErr := readEnvelope(cs.stream)
	if nextErr != nil {
		return nextErr
	}
	if nextFlag != flagEnvelopeEndStream {
		return connect.Errorf(connect.CodeInternal, "protocol error: expected end stream envelope after unary response message, got 0x%x", nextFlag)
	}
	cs.rxEnd = true
	cerr, trailers, err := unmarshalEndStream(nextPayload)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal end stream envelope: %v", err)
	}
	if cs.callInfo != nil && cs.callInfo.ResponseTrailer() != nil {
		for k, vs := range trailers {
			cs.callInfo.ResponseTrailer().SetValues(k, vs)
		}
	}
	if cerr != nil {
		return cerr
	}
	return nil
}

func (cs *clientStream) Close() error {
	_ = cs.CloseSend()
	cs.stream.CancelRead(0)
	return nil
}
