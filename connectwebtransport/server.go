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
	"connectrpc.com/connect/v2/connectproto"
	"connectrpc.com/connect/v2/internal/connectprotocol"
	framev1 "connectrpc.com/connect/v2/internal/gen/connect/frame/v1"
	"github.com/quic-go/webtransport-go"
	"google.golang.org/protobuf/proto"
)

type Handler struct {
	server *connect.Server
	opts   serverOptions
}

// NewHandler returns a new Handler that serves RPCs over WebTransport.
func NewHandler(server *connect.Server, opts ...Option) *Handler {
	sOpts := serverOptions{
		codecs:      make(map[string]connect.Codec),
		compressors: make(map[string]connect.Compressor),
	}
	// Defaults
	sOpts.codecs[connect.CodecNameProto] = connectproto.NewBinaryCodec()
	sOpts.codecs[connect.CodecNameJSON] = connectproto.NewJSONCodec()
	compMap, compNames := connectprotocol.DefaultCompressors()
	sOpts.compressors = compMap
	sOpts.compressorNames = compNames

	for _, opt := range opts {
		opt.applyServer(&sOpts)
	}
	sOpts.acceptCompression = strings.Join(sOpts.compressorNames, ",")

	return &Handler{
		server: server,
		opts:   sOpts,
	}
}

func (h *Handler) HandleSession(ctx context.Context, session *webtransport.Session) {
	for {
		stream, err := session.AcceptStream(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			continue
		}
		go h.handleStream(ctx, session, stream)
	}
}

func (h *Handler) handleStream(ctx context.Context, session *webtransport.Session, stream *webtransport.Stream) {
	defer func() {
		_ = stream.Close()
		stream.CancelRead(0)
	}()

	// Read request headers
	flag, payload, err := readEnvelope(stream)
	if err != nil {
		return
	}
	if flag != flagEnvelopeHeaders {
		return
	}

	var protoHeaders framev1.Headers
	if err := proto.Unmarshal(payload, &protoHeaders); err != nil {
		return
	}
	headers := protoToHeaders(&protoHeaders)

	procedure := ""
	if vs := headers[":path"]; len(vs) > 0 {
		procedure = vs[0]
	}
	if procedure == "" {
		return
	}

	if timeoutStr := headers.Get("Connect-Timeout-Ms"); timeoutStr != "" {
		if ms, err := strconv.ParseInt(timeoutStr, 10, 64); err == nil && ms > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, time.Duration(ms)*time.Millisecond)
			defer cancel()
		}
	}

	contentType := headers.Get("Content-Type")
	codecName := connect.CodecNameProto
	if strings.Contains(contentType, "json") {
		codecName = connect.CodecNameJSON
	} else if strings.Contains(contentType, "proto") {
		codecName = connect.CodecNameProto
	}

	codec := h.opts.codecs[codecName]
	if codec == nil {
		// Fallback to default codecs or return unimplemented
		for _, c := range h.opts.codecs {
			if c.Name() == codecName {
				codec = c
				break
			}
		}
		if codec == nil {
			// If not found, use first registered codec
			for _, c := range h.opts.codecs {
				codec = c
				break
			}
		}
	}

	callInfo := &connect.CallInfo{
		PeerAddr: session.RemoteAddr().String(),
		Protocol: "webtransport",
		Codec:    codec.Name(),
	}
	for k, vs := range headers {
		if !strings.HasPrefix(k, ":") {
			callInfo.RequestHeader().SetValues(k, vs)
		}
	}

	sStream := &serverStream{
		stream:   stream,
		callInfo: callInfo,
		opts:     h.opts,
		codec:    codec,
		ctx:      ctx,
	}

	callErr := h.server.Call(ctx, procedure, callInfo, sStream)

	// Ensure headers are sent
	_ = sStream.SendHeaders()

	var endErr *connect.Error
	if callErr != nil {
		endErr = errorForWire(callErr)
	}
	trailers := make(http.Header)
	maps.Insert(trailers, callInfo.ResponseTrailer().All())
	endPayload, marshalErr := marshalEndStream(endErr, trailers)
	if marshalErr != nil {
		endPayload = nil
	}
	_ = writeEnvelope(stream, flagEnvelopeEndStream, endPayload)
}

type serverStream struct {
	stream   *webtransport.Stream
	callInfo *connect.CallInfo
	opts     serverOptions
	codec    connect.Codec
	ctx      context.Context

	sendHeadersOnce sync.Once
	sendHeadersErr  error

	responseCompressor connect.Compressor
}

func (ss *serverStream) Receive(msg any) error {
	flag, payload, err := readEnvelope(ss.stream)
	if err != nil {
		return err
	}

	if flag == flagEnvelopeHeaders {
		return connect.Errorf(connect.CodeInternal, "protocol error: unexpected headers envelope in request body")
	}

	if flag == flagEnvelopeEndStream {
		return io.EOF
	}

	if flag != flagEnvelopeData && flag != flagEnvelopeCompressed {
		return connect.Errorf(connect.CodeInternal, "protocol error: unknown frame flag: 0x%x", flag)
	}

	if ss.opts.readMaxBytes > 0 && len(payload) > ss.opts.readMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds read limit %d", len(payload), ss.opts.readMaxBytes)
	}

	if flag == flagEnvelopeCompressed {
		encoding := ss.callInfo.RequestHeader().Get("Connect-Content-Encoding")
		compressor := ss.opts.compressors[encoding]
		if compressor == nil {
			return connect.Errorf(connect.CodeInternal, "protocol error: compressed message without Connect-Content-Encoding")
		}
		rc, err := compressor.Decompress(bytes.NewReader(payload))
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to decompress request: %v", err)
		}
		decompressed, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to read decompressed request: %v", err)
		}
		payload = decompressed
	}

	if err := ss.codec.UnmarshalRead(ss.ctx, bytes.NewReader(payload), msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to unmarshal request: %v", err)
	}
	return nil
}

func (ss *serverStream) SendHeaders() error {
	ss.sendHeadersOnce.Do(func() {
		ss.sendHeadersErr = ss.sendHeaders()
	})
	return ss.sendHeadersErr
}

func (ss *serverStream) sendHeaders() error {
	header := make(http.Header)
	if ss.callInfo.ResponseHeader() != nil {
		maps.Insert(header, ss.callInfo.ResponseHeader().All())
	}
	header.Set("Content-Type", "application/connect+"+ss.codec.Name())

	encoding, compressor := connectprotocol.NegotiateCompression(
		ss.callInfo.RequestHeader().Get("Connect-Accept-Encoding"),
		ss.opts.compressors,
	)
	if compressor != nil {
		ss.responseCompressor = compressor
		header.Set("Connect-Content-Encoding", encoding)
	}
	if ss.opts.acceptCompression != "" {
		header.Set("Connect-Accept-Encoding", ss.opts.acceptCompression)
	}

	protoHeaders := headersToProto(header)
	data, err := proto.Marshal(protoHeaders)
	if err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal response headers: %v", err)
	}

	return writeEnvelope(ss.stream, flagEnvelopeHeaders, data)
}

func (ss *serverStream) Send(msg any) error {
	if err := ss.SendHeaders(); err != nil {
		return err
	}

	var buf bytes.Buffer
	if err := ss.codec.MarshalWrite(ss.ctx, &buf, msg); err != nil {
		return connect.Errorf(connect.CodeInternal, "failed to marshal response: %v", err)
	}
	payload := buf.Bytes()

	flag := flagEnvelopeData
	if ss.responseCompressor != nil {
		var buf bytes.Buffer
		compWriter, err := ss.responseCompressor.Compress(&buf)
		if err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to compress response: %v", err)
		}
		if _, err := compWriter.Write(payload); err != nil {
			_ = compWriter.Close()
			return connect.Errorf(connect.CodeInternal, "failed to write compressed response: %v", err)
		}
		if err := compWriter.Close(); err != nil {
			return connect.Errorf(connect.CodeInternal, "failed to flush compressed response: %v", err)
		}
		payload = buf.Bytes()
		flag = flagEnvelopeCompressed
	}

	if ss.opts.sendMaxBytes > 0 && len(payload) > ss.opts.sendMaxBytes {
		return connect.Errorf(connect.CodeResourceExhausted, "message size %d exceeds send limit %d", len(payload), ss.opts.sendMaxBytes)
	}

	return writeEnvelope(ss.stream, flag, payload)
}
