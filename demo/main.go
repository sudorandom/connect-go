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

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	connect "connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connecthttp"
	"connectrpc.com/connect/v2/connectwebtransport"
	"connectrpc.com/connect/v2/connectwebsocket"
	elizav1 "connectrpc.com/connect/v2/internal/gen/connect/eliza/v1"
	"connectrpc.com/connect/v2/internal/gen/connect/eliza/v1/elizav1connect"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"
)

type elizaServer struct {
	elizav1connect.UnimplementedElizaServiceHandler
}

func (elizaServer) Say(ctx context.Context, req *elizav1.SayRequest) (*elizav1.SayResponse, error) {
	log.Printf("[Go Eliza] Say: %s", req.Sentence)
	return &elizav1.SayResponse{
		Sentence: "Go Eliza says: " + req.Sentence,
	}, nil
}

func (elizaServer) Converse(ctx context.Context, stream elizav1connect.ElizaServiceConverseServerStream) error {
	for {
		req, err := stream.Receive()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		log.Printf("[Go Eliza] Converse: %s", req.Sentence)
		if err := stream.Send(&elizav1.ConverseResponse{
			Sentence: "Go Eliza says: " + req.Sentence,
		}); err != nil {
			return err
		}
	}
}

func (elizaServer) Introduce(ctx context.Context, req *elizav1.IntroduceRequest, stream elizav1connect.ElizaServiceIntroduceServerStream) error {
	log.Printf("[Go Eliza] Introduce: %s", req.Name)
	sentences := []string{
		"Hi " + req.Name + ", I'm Eliza over WebTransport!",
		"I am running on a Go backend using the connectwebtransport package.",
		"How can I help you today?",
	}
	for _, s := range sentences {
		if err := stream.Send(&elizav1.IntroduceResponse{Sentence: s}); err != nil {
			return err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func main() {
	certFile := "localhost.pem"
	keyFile := "localhost-key.pem"

	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("failed to load keypair: %v", err)
	}

	// 1. Build connect handler
	connectServer := connect.NewServer()
	elizav1connect.RegisterElizaServiceHandler(connectServer, elizaServer{})
	webtransportHandler := connectwebtransport.NewHandler(connectServer)
	websocketHandler := connectwebsocket.NewHandler(connectServer)

	// 2. Setup WebTransport H3 server & TCP HTTP/2 Server
	mux := http.NewServeMux()
	connecthttp.Mount(mux, connectServer)
	
	mux.Handle("/websocket", websocketHandler)

	// Handle standard Connect HTTP traffic (unary/gRPC/etc) with CORS
	corsHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Headers", "*")
		w.Header().Set("Access-Control-Allow-Methods", "*")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		
		mux.ServeHTTP(w, r)
	})

	wtServer := &webtransport.Server{
		H3: &http3.Server{
			Addr:    ":4433",
			Handler: corsHandler,
			TLSConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{http3.NextProtoH3},
			},
			EnableDatagrams: true,
		},
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	webtransport.ConfigureHTTP3Server(wtServer.H3)

	mux.HandleFunc("/webtransport", func(w http.ResponseWriter, r *http.Request) {
		session, err := wtServer.Upgrade(w, r)
		if err != nil {
			log.Printf("WebTransport upgrade failed: %v", err)
			return
		}
		log.Println("WebTransport upgrade succeeded, handling session")
		webtransportHandler.HandleSession(r.Context(), session)
	})

	// Run WebTransport server on UDP :4433
	go func() {
		log.Println("UDP WebTransport/H3 server listening on https://localhost:4433/webtransport")
		udpAddr, err := net.ResolveUDPAddr("udp", ":4433")
		if err != nil {
			log.Fatalf("failed to resolve UDP: %v", err)
		}
		conn, err := net.ListenUDP("udp", udpAddr)
		if err != nil {
			log.Fatalf("failed to listen UDP: %v", err)
		}
		if err := wtServer.Serve(conn); err != nil {
			log.Fatalf("WebTransport server failed: %v", err)
		}
	}()

	withH3Headers := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			rw.Header().Add("Alt-Svc", `h3=":4433"; ma=2592000`)
			next.ServeHTTP(rw, r)
		})
	}

	// Run TCP HTTP/2 Server on TCP :4433
	log.Println("TCP HTTP/2 frontend server listening on https://localhost:4433")
	server := &http.Server{
		Addr:    ":4433",
		Handler: withH3Headers(corsHandler),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"h2", "http/1.1"},
		},
	}
	if err := server.ListenAndServeTLS("", ""); err != nil {
		log.Fatalf("HTTP server failed: %v", err)
	}
}
