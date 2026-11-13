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

package connecthttp_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/internal/assert"
	pingv1 "connectrpc.com/connect/v2/internal/gen/connect/ping/v1"
	"connectrpc.com/connect/v2/internal/gen/connect/ping/v1/pingv1connect"
	"connectrpc.com/connect/v2/internal/memhttp"
	"connectrpc.com/connect/v2/internal/memhttp/memhttptest"
	"google.golang.org/protobuf/proto"
)

// httpQueryMethod is the HTTP QUERY method from RFC 10008.
const httpQueryMethod = "QUERY"

type recordedRequest struct {
	method string
	header http.Header
	body   []byte
}

// requestRecorder wraps a handler and records every request it serves.
type requestRecorder struct {
	next http.Handler

	mu       sync.Mutex
	requests []recordedRequest
}

func (r *requestRecorder) ServeHTTP(responseWriter http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	request.Body = io.NopCloser(bytes.NewReader(body))
	r.mu.Lock()
	r.requests = append(r.requests, recordedRequest{
		method: request.Method,
		header: request.Header.Clone(),
		body:   body,
	})
	r.mu.Unlock()
	r.next.ServeHTTP(responseWriter, request)
}

func (r *requestRecorder) take() []recordedRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	requests := r.requests
	r.requests = nil
	return requests
}

// queryResponse is an HTTP response with its body already read.
type queryResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// queryPingServer echoes pings and records the HTTP method handlers observe.
type queryPingServer struct {
	pingv1connect.UnimplementedPingServiceHandler

	mu      sync.Mutex
	methods []string
}

func (s *queryPingServer) Ping(ctx context.Context, request *pingv1.PingRequest) (*pingv1.PingResponse, error) {
	serverInfo, _ := connecthttp.ServerInfoForContext(ctx)
	s.mu.Lock()
	s.methods = append(s.methods, serverInfo.HTTPMethod())
	s.mu.Unlock()
	if request.GetText() == "fail" {
		return nil, connect.NewError(connect.CodeFailedPrecondition, "ping failed")
	}
	return &pingv1.PingResponse{Number: request.GetNumber(), Text: request.GetText()}, nil
}

func (s *queryPingServer) Fail(context.Context, *pingv1.FailRequest) (*pingv1.FailResponse, error) {
	return &pingv1.FailResponse{}, nil
}

func (s *queryPingServer) Sum(context.Context, pingv1connect.PingServiceSumServerStream) (*pingv1.SumResponse, error) {
	return &pingv1.SumResponse{}, nil
}

func (s *queryPingServer) CountUp(context.Context, *pingv1.CountUpRequest, pingv1connect.PingServiceCountUpServerStream) error {
	return nil
}

func newQueryTestServer(t *testing.T, handler *queryPingServer, serverOptions ...connecthttp.Option) (*memhttp.Server, *requestRecorder) {
	t.Helper()
	mux := http.NewServeMux()
	srv := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(srv, handler)
	connecthttp.Mount(mux, srv, serverOptions...)
	recorder := &requestRecorder{next: mux}
	return memhttptest.NewServer(t, recorder), recorder
}

func TestClientHTTPQuery(t *testing.T) {
	t.Parallel()
	newClient := func(server *memhttp.Server, options ...connecthttp.Option) pingv1connect.PingServiceClient {
		return pingv1connect.NewPingServiceClient(connect.NewClient(
			connecthttp.NewTransport(server.Client(), server.URL(), options...),
		))
	}
	ctx := t.Context()

	t.Run("off_uses_post", func(t *testing.T) {
		t.Parallel()
		server, recorder := newQueryTestServer(t, &queryPingServer{})
		_, err := newClient(server).Ping(ctx, &pingv1.PingRequest{Text: "off"})
		assert.Nil(t, err)
		requests := recorder.take()
		assert.Equal(t, len(requests), 1)
		assert.Equal(t, requests[0].method, http.MethodPost)
	})
	t.Run("on_matches_post_request", func(t *testing.T) {
		t.Parallel()
		server, recorder := newQueryTestServer(t, &queryPingServer{})
		// Both calls share the recorder, so run them in sequence and compare.
		client := newClient(server, connecthttp.WithHTTPQuery())
		response, err := client.Ping(ctx, &pingv1.PingRequest{Number: 42, Text: "query"})
		assert.Nil(t, err)
		assert.Equal(t, response.GetText(), "query")
		assert.Equal(t, response.GetNumber(), 42)
		queryRequests := recorder.take()
		assert.Equal(t, queryRequests[len(queryRequests)-1].method, httpQueryMethod)
		queryRequest := queryRequests[len(queryRequests)-1]

		_, err = newClient(server).Ping(ctx, &pingv1.PingRequest{Number: 42, Text: "query"})
		assert.Nil(t, err)
		postRequests := recorder.take()
		postRequest := postRequests[len(postRequests)-1]
		assert.Equal(t, postRequest.method, http.MethodPost)
		assert.Equal(t, queryRequest.body, postRequest.body)
		for _, name := range []string{"Content-Type", "Connect-Protocol-Version", "Accept-Encoding"} {
			assert.Equal(t, queryRequest.header.Get(name), postRequest.header.Get(name))
		}
		assert.Equal(t, queryRequest.header.Get("Content-Type"), "application/proto")
		assert.Equal(t, queryRequest.header.Get("Connect-Protocol-Version"), "1")
	})
	t.Run("on_ineligible_and_streaming_use_post", func(t *testing.T) {
		t.Parallel()
		server, recorder := newQueryTestServer(t, &queryPingServer{})
		client := newClient(server, connecthttp.WithHTTPQuery())
		_, err := client.Fail(ctx, &pingv1.FailRequest{})
		assert.Nil(t, err)
		stream, err := client.Sum(ctx)
		assert.Nil(t, err)
		assert.Nil(t, stream.Send(&pingv1.SumRequest{Number: 1}))
		_, err = stream.CloseAndReceive()
		assert.Nil(t, err)
		countUp, err := client.CountUp(ctx, &pingv1.CountUpRequest{Number: 1})
		assert.Nil(t, err)
		_, _ = countUp.Receive()
		assert.Nil(t, countUp.Close())
		for _, request := range recorder.take() {
			assert.NotEqual(t, request.method, httpQueryMethod)
			assert.Equal(t, request.method, http.MethodPost)
		}
	})
	t.Run("json_codec", func(t *testing.T) {
		t.Parallel()
		server, _ := newQueryTestServer(t, &queryPingServer{})
		client := newClient(server, connecthttp.WithHTTPQuery(), connecthttp.WithSendCodec("json"))
		response, err := client.Ping(ctx, &pingv1.PingRequest{Text: "json"})
		assert.Nil(t, err)
		assert.Equal(t, response.GetText(), "json")
	})
	t.Run("compression", func(t *testing.T) {
		t.Parallel()
		server, recorder := newQueryTestServer(t, &queryPingServer{})
		client := newClient(server,
			connecthttp.WithHTTPQuery(),
			connecthttp.WithSendCompression("gzip"),
			connecthttp.WithCompressMinBytes(1),
		)
		text := strings.Repeat("compress me ", 32)
		response, err := client.Ping(ctx, &pingv1.PingRequest{Text: text})
		assert.Nil(t, err)
		assert.Equal(t, response.GetText(), text)
		requests := recorder.take()
		last := requests[len(requests)-1]
		assert.Equal(t, last.method, httpQueryMethod)
		assert.Equal(t, last.header.Get("Content-Encoding"), "gzip")
	})
}

func TestClientHTTPQueryWithGet(t *testing.T) {
	t.Parallel()
	server, recorder := newQueryTestServer(t, &queryPingServer{})
	call := func(t *testing.T, text string, options ...connecthttp.Option) string {
		t.Helper()
		client := pingv1connect.NewPingServiceClient(connect.NewClient(connecthttp.NewTransport(
			server.Client(),
			server.URL(),
			append([]connecthttp.Option{connecthttp.WithHTTPGet(), connecthttp.WithHTTPQuery()}, options...)...,
		)))
		response, err := client.Ping(t.Context(), &pingv1.PingRequest{Text: text})
		assert.Nil(t, err)
		assert.Equal(t, response.GetText(), text)
		requests := recorder.take()
		assert.Equal(t, len(requests), 1)
		return requests[0].method
	}
	// Without a limit, GET wins.
	assert.Equal(t, call(t, "small"), http.MethodGet)
	// Within the limit, GET wins.
	assert.Equal(t, call(t, "small", connecthttp.WithHTTPGetMaxURLSize(4096, true)), http.MethodGet)
	// Over the limit, the fallback is QUERY rather than POST.
	large := strings.Repeat(".", 256)
	assert.Equal(t, call(t, large, connecthttp.WithHTTPGetMaxURLSize(64, true)), httpQueryMethod)
	// Even with compression, which GET tries first.
	assert.Equal(t, call(t, large,
		connecthttp.WithHTTPGetMaxURLSize(1, true),
		connecthttp.WithSendCompression("gzip"),
	), httpQueryMethod)
}

func TestClientHTTPQueryRejected(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var methods []string
	server := memhttptest.NewServer(t, http.HandlerFunc(func(responseWriter http.ResponseWriter, request *http.Request) {
		mu.Lock()
		methods = append(methods, request.Method)
		mu.Unlock()
		responseWriter.Header().Set("Allow", "GET, POST")
		responseWriter.WriteHeader(http.StatusMethodNotAllowed)
	}))
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connecthttp.NewTransport(
		server.Client(),
		server.URL(),
		connecthttp.WithHTTPQuery(),
	)))
	_, err := client.Ping(t.Context(), &pingv1.PingRequest{})
	assert.NotNil(t, err)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, methods, []string{httpQueryMethod})
}

func TestClientHTTPQueryInfo(t *testing.T) {
	t.Parallel()
	handler := &queryPingServer{}
	server, _ := newQueryTestServer(t, handler)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connecthttp.NewTransport(
		server.Client(), server.URL(), connecthttp.WithHTTPQuery(),
	)))
	ctx, _ := connect.NewClientContext(t.Context())
	_, err := client.Ping(ctx, &pingv1.PingRequest{})
	assert.Nil(t, err)
	clientInfo, ok := connecthttp.ClientInfoForContext(ctx)
	assert.True(t, ok)
	assert.Equal(t, clientInfo.HTTPMethod(), httpQueryMethod)
	handler.mu.Lock()
	defer handler.mu.Unlock()
	assert.Equal(t, handler.methods, []string{httpQueryMethod})
}

func TestHandlerHTTPQuery(t *testing.T) {
	t.Parallel()
	handler := &queryPingServer{}
	server, _ := newQueryTestServer(t, handler)
	httpClient := server.Client()
	const pingProcedure = pingv1connect.PingServicePingProcedure
	do := func(t *testing.T, method, procedure, contentType string, body []byte, header ...string) queryResponse {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, server.URL()+procedure, bytes.NewReader(body))
		assert.Nil(t, err)
		if contentType != "" {
			request.Header.Set("Content-Type", contentType)
		}
		for i := 0; i+1 < len(header); i += 2 {
			request.Header.Set(header[i], header[i+1])
		}
		response, err := httpClient.Do(request)
		assert.Nil(t, err)
		defer response.Body.Close()
		responseBody, err := io.ReadAll(response.Body)
		assert.Nil(t, err)
		return queryResponse{StatusCode: response.StatusCode, Header: response.Header, Body: responseBody}
	}
	gzipped := func(data []byte) []byte {
		var buffer bytes.Buffer
		writer := gzip.NewWriter(&buffer)
		_, err := writer.Write(data)
		assert.Nil(t, err)
		assert.Nil(t, writer.Close())
		return buffer.Bytes()
	}
	protoBody, err := proto.Marshal(&pingv1.PingRequest{Number: 7, Text: "hello"})
	assert.Nil(t, err)
	jsonBody := []byte(`{"number":"7","text":"hello"}`)

	for _, test := range []struct {
		name        string
		contentType string
		body        []byte
		encoding    string
	}{
		{name: "proto", contentType: "application/proto", body: protoBody},
		{name: "json", contentType: "application/json", body: jsonBody},
		{name: "proto_gzip", contentType: "application/proto", body: gzipped(protoBody), encoding: "gzip"},
		{name: "json_gzip", contentType: "application/json", body: gzipped(jsonBody), encoding: "gzip"},
	} {
		t.Run("query_"+test.name, func(t *testing.T) {
			t.Parallel()
			var header []string
			if test.encoding != "" {
				header = append(header, "Content-Encoding", test.encoding)
			}
			response := do(t, httpQueryMethod, pingProcedure, test.contentType, test.body, header...)
			assert.Equal(t, response.StatusCode, http.StatusOK)
			assert.Equal(t, response.Header.Get("Content-Type"), test.contentType)
			assert.Equal(t, response.Header.Values("Vary"), []string{"Accept-Encoding"})
		})
	}
	t.Run("query_empty_body", func(t *testing.T) {
		t.Parallel()
		response := do(t, httpQueryMethod, pingProcedure, "application/proto", nil)
		assert.Equal(t, response.StatusCode, http.StatusOK)
	})
	t.Run("query_ignores_uri_query", func(t *testing.T) {
		t.Parallel()
		response := do(t, httpQueryMethod, pingProcedure+"?encoding=bogus&message=bogus", "application/proto", protoBody)
		assert.Equal(t, response.StatusCode, http.StatusOK)
	})
	t.Run("query_error_format", func(t *testing.T) {
		t.Parallel()
		response := do(t, httpQueryMethod, pingProcedure, "application/json", []byte(`{"text":"fail"}`))
		assert.Equal(t, response.StatusCode, http.StatusBadRequest)
		assert.Equal(t, response.Header.Get("Content-Type"), "application/json")
		assert.Equal(t, string(response.Body), `{"code":"failed_precondition","message":"ping failed"}`)
	})
	t.Run("query_bad_version_header", func(t *testing.T) {
		t.Parallel()
		response := do(t, httpQueryMethod, pingProcedure, "application/json", jsonBody, "Connect-Protocol-Version", "2")
		assert.Equal(t, response.StatusCode, http.StatusBadRequest)
	})
	t.Run("query_ineligible_and_streaming", func(t *testing.T) {
		t.Parallel()
		for _, procedure := range []string{
			pingv1connect.PingServiceFailProcedure,
			pingv1connect.PingServiceSumProcedure,
			pingv1connect.PingServiceCountUpProcedure,
			pingv1connect.PingServiceCumSumProcedure,
		} {
			response := do(t, httpQueryMethod, procedure, "application/json", []byte("{}"))
			assert.Equal(t, response.StatusCode, http.StatusMethodNotAllowed)
			assert.Equal(t, response.Header.Get("Allow"), http.MethodPost)
		}
	})
	t.Run("allow_lists_query_when_accepted", func(t *testing.T) {
		t.Parallel()
		response := do(t, http.MethodPut, pingProcedure, "application/json", nil)
		assert.Equal(t, response.StatusCode, http.StatusMethodNotAllowed)
		assert.Equal(t, response.Header.Get("Allow"), "GET, POST, QUERY")
	})
	t.Run("unsupported_content_type", func(t *testing.T) {
		t.Parallel()
		for _, contentType := range []string{"application/x-custom", "application/grpc", "application/grpc-web+proto", "application/connect+json", ""} {
			response := do(t, httpQueryMethod, pingProcedure, contentType, []byte("{}"))
			assert.Equal(t, response.StatusCode, http.StatusUnsupportedMediaType)
			assert.Equal(t, response.Header.Get("Accept-Query"), "application/json, application/proto")
			assert.Equal(t, response.Header.Get("Accept-Post"), "")
		}
	})
	t.Run("post_unsupported_content_type_unchanged", func(t *testing.T) {
		t.Parallel()
		response := do(t, http.MethodPost, pingProcedure, "application/x-custom", []byte("{}"))
		assert.Equal(t, response.StatusCode, http.StatusUnsupportedMediaType)
		assert.Equal(t, response.Header.Get("Accept-Query"), "")
		assert.NotEqual(t, response.Header.Get("Accept-Post"), "")
	})
	t.Run("handler_observes_query", func(t *testing.T) {
		t.Parallel()
		before := func() int {
			handler.mu.Lock()
			defer handler.mu.Unlock()
			return len(handler.methods)
		}()
		response := do(t, httpQueryMethod, pingProcedure, "application/json", jsonBody)
		assert.Equal(t, response.StatusCode, http.StatusOK)
		handler.mu.Lock()
		defer handler.mu.Unlock()
		assert.True(t, len(handler.methods) > before)
		assert.True(t, slices.Contains(handler.methods, httpQueryMethod))
	})
}

func TestHandlerHTTPQueryDisabled(t *testing.T) {
	t.Parallel()
	server, _ := newQueryTestServer(t, &queryPingServer{}, connecthttp.WithoutHTTPQuery())
	const pingProcedure = pingv1connect.PingServicePingProcedure
	do := func(t *testing.T, method, target string, body string) queryResponse {
		t.Helper()
		request, err := http.NewRequestWithContext(t.Context(), method, server.URL()+target, strings.NewReader(body))
		assert.Nil(t, err)
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		assert.Nil(t, err)
		defer response.Body.Close()
		return queryResponse{StatusCode: response.StatusCode, Header: response.Header}
	}
	response := do(t, httpQueryMethod, pingProcedure, "{}")
	assert.Equal(t, response.StatusCode, http.StatusMethodNotAllowed)
	assert.Equal(t, response.Header.Get("Allow"), "GET, POST")
	assert.Equal(t, do(t, http.MethodPost, pingProcedure, "{}").StatusCode, http.StatusOK)
	assert.Equal(t, do(t, http.MethodGet, pingProcedure+"?encoding=json&message=%7B%7D", "").StatusCode, http.StatusOK)
}

func TestHTTPQueryNotModified(t *testing.T) {
	t.Parallel()
	const etag = `"etag-1"`
	mux := http.NewServeMux()
	srv := connect.NewServer()
	pingv1connect.RegisterPingServiceHandler(srv, &notModifiedPingServer{etag: etag})
	connecthttp.Mount(mux, srv)
	server := memhttptest.NewServer(t, mux)
	client := pingv1connect.NewPingServiceClient(connect.NewClient(connecthttp.NewTransport(
		server.Client(), server.URL(), connecthttp.WithHTTPQuery(),
	)))
	// unconditional request
	ctx, info := connect.NewClientContext(t.Context())
	_, err := client.Ping(ctx, &pingv1.PingRequest{})
	assert.Nil(t, err)
	assert.Equal(t, info.ResponseHeader().Get("Etag"), etag)
	assert.Equal(t, info.ResponseHeader().Values("Vary"), []string{"Accept-Encoding"})
	httpInfo, _ := info.TransportInfo.(*connecthttp.ClientInfo)
	assert.Equal(t, httpInfo.HTTPMethod(), httpQueryMethod)

	// conditional request
	condCtx, condInfo := connect.NewClientContext(t.Context())
	condInfo.RequestHeader().Set("If-None-Match", etag)
	_, err = client.Ping(condCtx, &pingv1.PingRequest{})
	assert.NotNil(t, err)
	assert.True(t, connecthttp.IsNotModifiedError(err))
	condHTTPInfo, _ := condInfo.TransportInfo.(*connecthttp.ClientInfo)
	assert.Equal(t, condHTTPInfo.HTTPMethod(), httpQueryMethod)
}
