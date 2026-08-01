# connectwebtransport

This package implements a Connect transport for running RPC calls over **WebTransport** (via HTTP/3 / QUIC).

## 🚀 Usage

WebTransport is establishing a single persistent connection (session) over HTTP/3, and then multiplexing individual RPC calls over dedicated streams inside that session.

### Server Example

```go
connectServer := connect.NewServer()
pingv1connect.RegisterPingServiceHandler(connectServer, pingServer{})

// Wrap the Connect server in a WebTransport handler
webtransportHandler := connectwebtransport.NewHandler(connectServer)

// Set up standard HTTP/3 server
wtServer := &webtransport.Server{
    H3: &http3.Server{
        Addr:      ":4433",
        TLSConfig: tlsConfig,
    },
}

http.HandleFunc("/webtransport", func(w http.ResponseWriter, r *http.Request) {
    session, err := wtServer.Upgrade(w, r)
    if err != nil {
        return
    }
    webtransportHandler.HandleSession(r.Context(), session)
})
```

### Client Example

```go
dialer := &webtransport.Dialer{
    TLSClientConfig: tlsConfig,
}

_, session, err := dialer.Dial(ctx, "https://localhost:4433/webtransport", nil)
if err != nil {
    log.Fatal(err)
}

// Wrap the session in a WebTransport transport
wtTransport := connectwebtransport.NewTransport(session)
client := pingv1connect.NewPingServiceClient(connect.NewClient(wtTransport))

// Invoke RPCs normally
resp, err := client.Ping(ctx, &pingv1.PingRequest{Text: "hello"})
```

---

## 🛠️ Implementation Details

This package uses the **Connect streaming protocol framing methodology** to carry messages and metadata over raw WebTransport streams:

1. **Stream Mapping**: Each RPC call is allocated a dedicated bidirectional WebTransport stream.
2. **Connect Envelope Framing**: All data written to the stream is prefixed with the standard Connect 5-byte envelope header: `[1-byte flag] [4-byte big-endian length]`.
3. **Header Frame (Flag `0x04`)**: Sent exactly once at the beginning of both request and response streams containing JSON-encoded metadata (e.g., `:path`, `Content-Type`).
4. **Data Frames (Flags `0x00`/`0x01`)**: Carry the serialized and optionally compressed message payloads (uncompressed: `0x00`, compressed: `0x01`).
5. **End of Stream (EOS) Frame (Flag `0x02`)**: Sent by the server at the end of the response containing JSON-encoded trailers and remote execution errors, matching the Connect streaming protocol's End-Stream layout.

---

## 📐 Architectural Alternatives Considered

During design, several architectural alternatives were evaluated:

### 1. New WebTransport Session (Handshake) per RPC Call
Instead of sharing one session, the client could establish a new WebTransport session (HTTP/3 CONNECT request) for every RPC call.
* **Why we rejected it**: Highly inefficient. Initiating a WebTransport session requires performing an HTTP/3 upgrade request and verifying security origins, adding high RTT (Round Trip Time) and CPU overhead. Sharing a session allows opening new streams in **0 RTT** with negligible resource cost.

### 2. Multiplexing RPCs over a Single WebTransport Stream
Instead of opening a new stream per RPC, we could keep a single bidirectional stream open and multiplex multiple concurrent RPC calls over it.
* **Why we rejected it**: Multiplexing multiple calls over a single stream would require us to build a custom multiplexing layer (assigning request IDs to frames and matching responses to requests). This would effectively reinvent HTTP/2 on top of WebTransport. Mapping each RPC to a dedicated WebTransport stream lets QUIC handle flow control, stream scheduling, and loss recovery natively.

### 3. Connect directly over standard HTTP/3
Instead of WebTransport, we could just run Connect over HTTP/3 using standard HTTP clients/servers.
* **Why we rejected it**: This is already supported by the core `connecthttp` package using standard Go HTTP/3 libraries. WebTransport is distinct because it is designed specifically for web/browser clients to establish low-latency, bidirectional connections that allow raw stream manipulation and unreliable datagrams, bypassing browser HTTP sandboxing limits.

---

## 🎮 Running the Eliza Demo

We have a unified Eliza demo app that showcases the Go `connectwebtransport` server working with the TypeScript `@connectrpc/connect-web` WebTransport client.

### 1. Build the TypeScript Workspace
Ensure you have cloned `connectrpc/connect-es` to `../connect-es` and built the workspace:
```bash
cd ../connect-es
npx turbo run build
```

### 2. Regenerate Local Certificates
Because the default certificates in the `connect-es` repository are signed by the authors' local CA, you must generate a fresh set signed by your own local `mkcert` CA so your browser can trust them:
```bash
cd ../connect-es/packages/example
mkcert localhost 127.0.0.1 ::1
```

### 3. Start the Demo Server
Run the unified Go HTTP/2 TCP + HTTP/3 WebTransport server:
```bash
cd ../connect-go-v2
go run ./connectwebtransport/demo/main.go
```
The server will start listening on `https://localhost:4433`.

### 4. Configure Browser Trust

Because WebTransport uses UDP (QUIC) and does not show typical TCP certificate warning pages, you must perform these browser-specific configurations to allow the connection:

#### 🦊 Firefox
1. Navigate to the TCP HTTPS page: **`https://localhost:4433/`**
2. Firefox will show a security warning. Click **Advanced -> Accept the Risk and Continue**.
3. Firefox will now cache this exception and allow the WebTransport UDP connection.

#### 🌐 Chrome, Edge, and Brave
1. Ensure you have installed your local root CA by running `mkcert -install`.
2. Chromium-based browsers require a flag to allow locally trusted root CAs for WebTransport (otherwise they only accept publicly known roots):
   * Open **`chrome://flags/#webtransport-developer-mode`**
   * Set **WebTransport Developer Mode** to **Enabled**
   * Relaunch the browser.
3. Open **`https://localhost:4433`** and chat with Eliza over WebTransport!
