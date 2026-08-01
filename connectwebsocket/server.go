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
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"maps"
	"net/http"
	"strings"
	"sync"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	framev1 "connectrpc.com/connect/v2/internal/gen/connect/frame/v1"
	"github.com/coder/websocket"
	"google.golang.org/protobuf/proto"
)

type Handler struct {
	server *connect.Server
}

func NewHandler(server *connect.Server) *Handler {
	return &Handler{
		server: server,
	}
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // For demo
	})
	if err != nil {
		return
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	ctx := r.Context()
	log.Printf("websocket connection opened from %s", r.RemoteAddr)
	
	muxer := &wsMuxer{
		conn:    conn,
		handler: h,
		streams: make(map[uint32]*serverStream),
	}
	muxer.loop(ctx)
	log.Printf("websocket connection closed from %s", r.RemoteAddr)
}

type wsMuxer struct {
	conn    *websocket.Conn
	handler *Handler

	mu      sync.Mutex
	streams map[uint32]*serverStream
}

func (m *wsMuxer) loop(ctx context.Context) {
	for {
		typ, payload, err := m.conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageBinary {
			continue // ignore text frames
		}

		var frame framev1.WebSocketFrame
		if err := proto.Unmarshal(payload, &frame); err != nil {
			continue
		}

		m.mu.Lock()
		stream, ok := m.streams[frame.StreamId]
		if !ok {
			// New stream
			stream = &serverStream{
				streamID: frame.StreamId,
				muxer:    m,
				recvChan: make(chan *framev1.WebSocketFrame, 16),
			}
			m.streams[frame.StreamId] = stream
			go m.handler.handleStream(ctx, stream)
		}
		m.mu.Unlock()

		stream.recvChan <- &frame
	}
}

func (m *wsMuxer) writeFrame(ctx context.Context, frame *framev1.WebSocketFrame) error {
	payload, err := proto.Marshal(frame)
	if err != nil {
		return err
	}
	
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.conn.Write(ctx, websocket.MessageBinary, payload)
}

func (h *Handler) handleStream(ctx context.Context, stream *serverStream) {
	// 1. Wait for Headers frame
	select {
	case <-ctx.Done():
		return
	case frame := <-stream.recvChan:
		headersPayload, ok := frame.Payload.(*framev1.WebSocketFrame_Headers)
		if !ok {
			return
		}
		headers := protoToHeaders(headersPayload.Headers)
		
		procedure := ""
		if vs := headers[":path"]; len(vs) > 0 {
			procedure = vs[0]
		}
		if procedure == "" {
			return
		}

		contentType := ""
		if vs := headers["Content-Type"]; len(vs) > 0 {
			contentType = vs[0]
		}
		var codec connect.Codec
		if strings.Contains(contentType, "json") {
			codec = connectproto.NewJSONCodec()
		} else {
			codec = connectproto.NewBinaryCodec()
		}
		
		callInfo := &connect.CallInfo{
			PeerAddr: "websocket-peer",
			Protocol: "websocket",
			Codec:    codec.Name(),
		}
		for k, vs := range headers {
			if !strings.HasPrefix(k, ":") {
				callInfo.RequestHeader().SetValues(k, vs)
			}
		}

		stream.callInfo = callInfo
		stream.codec = codec
		stream.ctx = ctx
		
		callErr := h.server.Call(ctx, procedure, callInfo, stream)

		// Send EndStream
		_ = stream.SendHeaders()
		var endErr *connect.Error
		if callErr != nil {
			endErr = errorForWire(callErr)
		}
		trailers := make(http.Header)
		maps.Insert(trailers, callInfo.ResponseTrailer().All())
		end := endStreamToProto(endErr, trailers)
		
		_ = stream.muxer.writeFrame(ctx, &framev1.WebSocketFrame{
			StreamId: stream.streamID,
			Payload: &framev1.WebSocketFrame_EndStream{
				EndStream: end,
			},
		})
	}
}

type serverStream struct {
	streamID uint32
	muxer    *wsMuxer
	recvChan chan *framev1.WebSocketFrame
	
	callInfo *connect.CallInfo
	codec    connect.Codec
	ctx      context.Context
	
	sendHeadersOnce sync.Once
}

func (s *serverStream) Receive(msg any) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case frame, ok := <-s.recvChan:
		if !ok {
			return errors.New("stream closed")
		}
		if _, ok := frame.Payload.(*framev1.WebSocketFrame_EndStream); ok {
			return io.EOF
		}
		dataPayload, ok := frame.Payload.(*framev1.WebSocketFrame_Data)
		if !ok {
			return errors.New("expected data payload")
		}
		
		if err := s.codec.UnmarshalRead(s.ctx, bytes.NewReader(dataPayload.Data), msg); err != nil {
			return err
		}
		return nil
	}
}

func (s *serverStream) SendHeaders() error {
	s.sendHeadersOnce.Do(func() {
		header := make(http.Header)
		if s.callInfo.ResponseHeader() != nil {
			maps.Insert(header, s.callInfo.ResponseHeader().All())
		}
		header.Set("Content-Type", "application/connect+"+s.codec.Name())
		
		protoHeaders := headersToProto(header)
		_ = s.muxer.writeFrame(s.ctx, &framev1.WebSocketFrame{
			StreamId: s.streamID,
			Payload: &framev1.WebSocketFrame_Headers{
				Headers: protoHeaders,
			},
		})
	})
	return nil
}

func (s *serverStream) Send(msg any) error {
	if err := s.SendHeaders(); err != nil {
		return err
	}
	
	var buf bytes.Buffer
	if err := s.codec.MarshalWrite(s.ctx, &buf, msg); err != nil {
		return err
	}
	
	return s.muxer.writeFrame(s.ctx, &framev1.WebSocketFrame{
		StreamId: s.streamID,
		Payload: &framev1.WebSocketFrame_Data{
			Data: buf.Bytes(),
		},
	})
}
