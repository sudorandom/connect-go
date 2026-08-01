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
	"compress/gzip"
	"context"
	"errors"
	"io"
	"strings"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/quic-go/webtransport-go"
)

// Option configures NewTransport and NewHandler.
type Option interface {
	applyTransport(*transportOptions)
	applyServer(*serverOptions)
}

type transportOptions struct {
	codecs            map[string]connect.Codec
	sendCodecName     string
	sendCodec         connect.Codec
	sendCompressor    string
	compressors       map[string]connect.Compressor
	compressorNames   []string
	acceptCompression string
	readMaxBytes      int
	sendMaxBytes      int
}

type serverOptions struct {
	codecs            map[string]connect.Codec
	compressors       map[string]connect.Compressor
	compressorNames   []string
	acceptCompression string
	readMaxBytes      int
	sendMaxBytes      int
}

type optionFunc func(*transportOptions, *serverOptions)

func (f optionFunc) applyTransport(opts *transportOptions) { f(opts, nil) }
func (f optionFunc) applyServer(opts *serverOptions)       { f(nil, opts) }

// WithSendCodec selects the codec (by name) used for outgoing client requests.
func WithSendCodec(name string) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.sendCodecName = name
		}
	})
}

// WithCodecs registers connect.Codec values.
func WithCodecs(codecs ...connect.Codec) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			for _, c := range codecs {
				topts.codecs[c.Name()] = c
			}
		}
		if sopts != nil {
			for _, c := range codecs {
				sopts.codecs[c.Name()] = c
			}
		}
	})
}

// WithSendCompressor selects the compressor (by name) to apply to outgoing client payloads.
func WithSendCompressor(name string) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.sendCompressor = name
		}
	})
}

// WithCompressors registers connect.Compressor values.
func WithCompressors(compressors ...connect.Compressor) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			for _, c := range compressors {
				topts.compressors[c.Name()] = c
				topts.compressorNames = append(topts.compressorNames, c.Name())
			}
		}
		if sopts != nil {
			for _, c := range compressors {
				sopts.compressors[c.Name()] = c
				sopts.compressorNames = append(sopts.compressorNames, c.Name())
			}
		}
	})
}

// WithReadMaxBytes limits the size of a message that can be read.
func WithReadMaxBytes(maxBytes int) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.readMaxBytes = maxBytes
		}
		if sopts != nil {
			sopts.readMaxBytes = maxBytes
		}
	})
}

// WithSendMaxBytes limits the size of a message that can be sent.
func WithSendMaxBytes(maxBytes int) Option {
	return optionFunc(func(topts *transportOptions, sopts *serverOptions) {
		if topts != nil {
			topts.sendMaxBytes = maxBytes
		}
		if sopts != nil {
			sopts.sendMaxBytes = maxBytes
		}
	})
}

// Default gzip compressor.
type gzipCompressor struct{}

func (gzipCompressor) Name() string { return connect.CompressionNameGzip }
func (gzipCompressor) Compress(w io.Writer) (io.WriteCloser, error) {
	return gzip.NewWriter(w), nil
}
func (gzipCompressor) Decompress(r io.Reader) (io.ReadCloser, error) {
	return gzip.NewReader(r)
}

func defaultCompressors() (map[string]connect.Compressor, []string) {
	c := gzipCompressor{}
	return map[string]connect.Compressor{c.Name(): c}, []string{c.Name()}
}

type transport struct {
	session *webtransport.Session
	opts    transportOptions
}

// NewTransport returns a connect.Transport that dispatches RPCs over WebTransport.
func NewTransport(session *webtransport.Session, opts ...Option) connect.Transport {
	tOpts := transportOptions{
		codecs:         make(map[string]connect.Codec),
		compressors:    make(map[string]connect.Compressor),
		sendCodecName:  connect.CodecNameProto,
		sendCompressor: connect.CompressionNameIdentity,
	}
	// Defaults
	tOpts.codecs[connect.CodecNameProto] = connectproto.NewBinaryCodec()
	tOpts.codecs[connect.CodecNameJSON] = connectproto.NewJSONCodec()
	compMap, compNames := defaultCompressors()
	tOpts.compressors = compMap
	tOpts.compressorNames = compNames

	for _, opt := range opts {
		opt.applyTransport(&tOpts)
	}
	tOpts.sendCodec = tOpts.codecs[tOpts.sendCodecName]
	tOpts.acceptCompression = strings.Join(tOpts.compressorNames, ",")

	return &transport{
		session: session,
		opts:    tOpts,
	}
}

func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	if t.session == nil {
		return nil, errors.New("connectwebtransport: nil session")
	}
	stream, err := t.session.OpenStreamSync(ctx)
	if err != nil {
		return nil, err
	}

	callInfo, _ := connect.CallInfoForClientContext(ctx)
	if callInfo != nil {
		callInfo.Protocol = "webtransport"
	}

	clientStreamVal := &clientStream{
		spec:     spec,
		stream:   stream,
		callInfo: callInfo,
		opts:     t.opts,
		codec:    t.opts.sendCodec,
		ctx:      ctx,
	}
	if clientStreamVal.codec == nil {
		clientStreamVal.codec = t.opts.codecs[connect.CodecNameProto]
	}
	return clientStreamVal, nil
}
