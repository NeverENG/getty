/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package benchmark

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

import (
	getty "github.com/AlexStocks/getty/transport"
	gettylog "github.com/AlexStocks/getty/util"
)

const (
	// maxPacketLen mirrors getty's unexported fragment size: WriteBytes above
	// it switches from a single read-locked send to the write-locked
	// fragmenting loop, so benchmarks cover both sides of that boundary.
	maxPacketLen = 16 * 1024
	// wsPath is the websocket endpoint both sides of the ws echo agree on.
	wsPath = "/bench-echo"
	// replyBuffer bounds the outstanding echo replies a client can have; keep
	// every in-flight value well below it so the reply listener never blocks
	// the session read loop.
	replyBuffer = 4096
	// dialTimeout bounds how long a benchmark waits for a client pool.
	dialTimeout = 10 * time.Second
	// replyTimeout turns "the peer never echoed" into a failure instead of a
	// hang.
	replyTimeout = 30 * time.Second
	// maxMsgLen must exceed every echoed payload: session.handleTCPPackage and
	// handleWSPackage drop any pkg larger than maxMsgLen, whose default is 4KB,
	// so a 16KB echo would silently never come back.
	maxMsgLen = 2 << 20
)

// sizes spans both sides of maxPacketLen.
var sizes = []int{64, 1 << 10, maxPacketLen, maxPacketLen + 1, 64 << 10, 1 << 20}

// Echo payload sizes per transport. ws stays under the 4KB default maxMsgLen: a
// ws *client* has gorilla's read limit fixed from the default maxMsgLen while
// the session is built, before NewSessionCallback can raise it, so a larger
// echo is rejected with "read limit exceeded" no matter what the callback does.
var (
	tcpEchoSizes = []int{64, 1 << 10, maxPacketLen, 64 << 10}
	wsEchoSizes  = []int{64, 1 << 10, 3 << 10}
)

func echoSizes(transport string) []int {
	if transport == "ws" {
		return wsEchoSizes
	}
	return tcpEchoSizes
}

// silenceLogs keeps getty's logging out of the numbers. gettyTCPConn.Send logs
// on every call, and the libraries also log benign teardown events (a closed
// http.Server), which would otherwise dominate the measurement and garble the
// output. Failures surface through b.Fatal and the dial/reply timeouts instead.
var silenceLogsOnce sync.Once

func silenceLogs() {
	silenceLogsOnce.Do(func() {
		if err := gettylog.SetLoggerLevel(gettylog.LoggerLevelFatal); err != nil {
			panic(err)
		}
		_ = gettylog.SetLoggerCallerDisable()
	})
}

func benchPayload(n int) []byte {
	p := make([]byte, n)
	for i := range p {
		p[i] = byte(i)
	}
	return p
}

// sizeName keeps the maxPacketLen boundary distinguishable from maxPacketLen+1.
func sizeName(n int) string {
	switch {
	case n == maxPacketLen+1:
		return "16K+1"
	case n >= 1<<20:
		return fmt.Sprintf("%dM", n>>20)
	case n >= 1<<10:
		return fmt.Sprintf("%dK", n>>10)
	default:
		return strconv.Itoa(n)
	}
}

// nopCodec is the cheapest codec there is: Read never completes a pkg, so a
// session using it only drains its peer, and Write passes a []byte straight
// through. The write-path benchmarks use it so the codec is not part of what
// they measure.
type nopCodec struct{}

func (nopCodec) Read(getty.Session, []byte) (any, int, error) { return nil, 0, nil }

func (nopCodec) Write(_ getty.Session, pkg any) ([]byte, error) {
	body, ok := pkg.([]byte)
	if !ok {
		return nil, fmt.Errorf("nopCodec: unexpected pkg type %T", pkg)
	}
	return body, nil
}

// udpCodec unwraps the UDPContext a udp session must be written with.
type udpCodec struct{}

func (udpCodec) Read(getty.Session, []byte) (any, int, error) { return nil, 0, nil }

func (udpCodec) Write(_ getty.Session, pkg any) ([]byte, error) {
	ctx, ok := pkg.(getty.UDPContext)
	if !ok {
		return nil, fmt.Errorf("udpCodec: unexpected pkg type %T", pkg)
	}
	body, ok := ctx.Pkg.([]byte)
	if !ok {
		return nil, fmt.Errorf("udpCodec: unexpected UDPContext.Pkg type %T", ctx.Pkg)
	}
	return body, nil
}

// lengthPrefixedCodec frames each pkg as a 4-byte big-endian length plus body.
// It is the smallest codec a getty user would write, so the echo benchmarks
// measure getty rather than a serialization library.
type lengthPrefixedCodec struct{}

func (lengthPrefixedCodec) Read(_ getty.Session, data []byte) (any, int, error) {
	if len(data) < 4 {
		return nil, 0, nil
	}
	n := int(binary.BigEndian.Uint32(data[:4]))
	if n < 0 || len(data) < 4+n {
		return nil, 0, nil
	}
	body := make([]byte, n)
	copy(body, data[4:4+n])
	return body, 4 + n, nil
}

func (lengthPrefixedCodec) Write(_ getty.Session, pkg any) ([]byte, error) {
	body, ok := pkg.([]byte)
	if !ok {
		return nil, fmt.Errorf("lengthPrefixedCodec: unexpected pkg type %T", pkg)
	}
	frame := make([]byte, 4+len(body))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
	copy(frame[4:], body)
	return frame, nil
}

// jsonPkg is what the JSON codec benchmark round-trips.
type jsonPkg struct {
	Body []byte `json:"body"`
}

// jsonCodec keeps the same length framing and adds a JSON encode/decode inside
// it.
type jsonCodec struct{}

func (jsonCodec) Read(ss getty.Session, data []byte) (any, int, error) {
	body, n, err := lengthPrefixedCodec{}.Read(ss, data)
	if body == nil || err != nil {
		return nil, n, err
	}
	var pkg jsonPkg
	if err := json.Unmarshal(body.([]byte), &pkg); err != nil {
		return nil, 0, err
	}
	return &pkg, n, nil
}

func (jsonCodec) Write(_ getty.Session, pkg any) ([]byte, error) {
	body, err := json.Marshal(pkg)
	if err != nil {
		return nil, err
	}
	return lengthPrefixedCodec{}.Write(nil, body)
}

// discardListener does nothing: the sessions that use it only send.
type discardListener struct{}

func (discardListener) OnOpen(getty.Session) error   { return nil }
func (discardListener) OnClose(getty.Session)        {}
func (discardListener) OnError(getty.Session, error) {}
func (discardListener) OnCron(getty.Session)         {}
func (discardListener) OnMessage(getty.Session, any) {}

// echoListener writes every decoded pkg straight back, so a benchmark measures
// the full write-codec-conn-read-codec round trip on both sides.
type echoListener struct{}

func (echoListener) OnOpen(getty.Session) error   { return nil }
func (echoListener) OnClose(getty.Session)        {}
func (echoListener) OnError(getty.Session, error) {}
func (echoListener) OnCron(getty.Session)         {}
func (echoListener) OnMessage(ss getty.Session, pkg any) {
	_, _, _ = ss.WritePkg(pkg, 0)
}

// replyListener signals the benchmark goroutine once per echoed pkg.
type replyListener struct{ replies chan<- struct{} }

func (replyListener) OnOpen(getty.Session) error   { return nil }
func (replyListener) OnClose(getty.Session)        {}
func (replyListener) OnError(getty.Session, error) {}
func (replyListener) OnCron(getty.Session)         {}
func (l replyListener) OnMessage(getty.Session, any) {
	l.replies <- struct{}{}
}

// drainListener is a raw TCP listener that throws away everything it receives.
// It stands in for the peer in the write-path benchmarks: they measure a getty
// client session writing to a socket, and the peer must neither apply
// backpressure of its own nor add getty work to the measurement.
func drainListener(tb testing.TB) (string, func()) {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("net.Listen: %v", err)
	}

	var (
		conns    sync.Map
		acceptWG sync.WaitGroup
	)
	acceptWG.Add(1)
	go func() {
		defer acceptWG.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conns.Store(conn, struct{}{})
			go func(c net.Conn) {
				_, _ = io.Copy(io.Discard, c)
			}(conn)
		}
	}()

	return ln.Addr().String(), func() {
		_ = ln.Close()
		// The accept goroutine must be gone before the range below, otherwise it
		// can store a connection that nothing will ever close.
		acceptWG.Wait()
		conns.Range(func(k, _ any) bool {
			_ = k.(net.Conn).Close()
			return true
		})
	}
}

// newWriteSession returns a getty TCP client session whose peer discards every
// byte, so WriteBytes/WritePkg/Send can be measured against a real socket.
// compress == getty.CompressNone leaves the connection raw: getty only installs
// its flate/snappy codec when SetCompressType is called, and passing
// CompressNone there would install flate at NoCompression level instead.
func newWriteSession(b *testing.B, codec getty.ReadWriter, compress getty.CompressType) getty.Session {
	b.Helper()
	silenceLogs()

	addr, closeDrain := drainListener(b)
	ready := make(chan getty.Session, 1)
	cli := getty.NewTCPClient(getty.WithServerAddress(addr), getty.WithConnectionNumber(1))
	go cli.RunEventLoop(func(ss getty.Session) error {
		ss.SetPkgHandler(codec)
		ss.SetEventListener(discardListener{})
		ss.SetReadTimeout(time.Minute)
		ss.SetWriteTimeout(time.Minute)
		if compress != getty.CompressNone {
			// Must happen before any IO on the connection.
			ss.SetCompressType(compress)
		}
		ready <- ss
		return nil
	})
	b.Cleanup(func() {
		cli.Close()
		closeDrain()
	})

	select {
	case ss := <-ready:
		return ss
	case <-time.After(dialTimeout):
		b.Fatal("client session did not come up")
		return nil
	}
}

// benchEcho is a live getty client/server echo pair over loopback.
type benchEcho struct {
	srv        getty.StreamServer
	cli        getty.Client
	sessions   []getty.Session
	replies    chan struct{}
	replyTimer *time.Timer
}

// newBenchEcho starts a server and a client pool of conns sessions, all sharing
// codec.
func newBenchEcho(b *testing.B, transport string, codec getty.ReadWriter, compress getty.CompressType, conns int) *benchEcho {
	b.Helper()
	silenceLogs()

	e := &benchEcho{
		replies:    make(chan struct{}, replyBuffer),
		replyTimer: time.NewTimer(replyTimeout),
	}
	// Registered before anything can fail, so a dial timeout or an unsupported
	// transport still tears down whatever was already started instead of leaving
	// a server and its event loop behind for the rest of the run.
	b.Cleanup(e.close)

	switch transport {
	case "tcp":
		e.srv = getty.NewTCPServer(getty.WithLocalAddress("127.0.0.1:0")).(getty.StreamServer)
	case "ws":
		e.srv = getty.NewWSServer(
			getty.WithLocalAddress("127.0.0.1:0"),
			getty.WithWebsocketServerPath(wsPath),
		).(getty.StreamServer)
	default:
		b.Fatalf("unknown transport %q", transport)
	}
	e.srv.RunEventLoop(func(ss getty.Session) error {
		setHandler(ss, codec, echoListener{}, compress)
		return nil
	})

	addr := e.srv.Listener().Addr().String()
	switch transport {
	case "tcp":
		e.cli = getty.NewTCPClient(getty.WithServerAddress(addr), getty.WithConnectionNumber(conns))
	case "ws":
		e.cli = getty.NewWSClient(
			getty.WithServerAddress("ws://"+addr+wsPath),
			getty.WithConnectionNumber(conns),
		)
	}
	ready := make(chan getty.Session, conns)
	// RunEventLoop blocks until the pool is dialled; run it in the background
	// so a server that never accepts cannot pin the benchmark setup forever.
	go e.cli.RunEventLoop(func(ss getty.Session) error {
		setHandler(ss, codec, replyListener{replies: e.replies}, compress)
		ready <- ss
		return nil
	})

	timeout := time.After(dialTimeout)
	for i := 0; i < conns; i++ {
		select {
		case ss := <-ready:
			e.sessions = append(e.sessions, ss)
		case <-timeout:
			b.Fatalf("%s: only %d of %d client sessions came up", transport, i, conns)
		}
	}

	// Drain anything sent during the handshake so the first measured iteration
	// starts from an empty reply queue.
	for drained := false; !drained; {
		select {
		case <-e.replies:
		default:
			drained = true
		}
	}

	return e
}

func setHandler(ss getty.Session, codec getty.ReadWriter, listener getty.EventListener, compress getty.CompressType) {
	ss.SetPkgHandler(codec)
	ss.SetEventListener(listener)
	ss.SetMaxMsgLen(maxMsgLen)
	ss.SetReadTimeout(time.Minute)
	ss.SetWriteTimeout(time.Minute)
	if compress != getty.CompressNone {
		ss.SetCompressType(compress)
	}
}

// waitReply blocks until the peer echoed one pkg, and fails the benchmark
// instead of hanging forever if it never does.
//
// It reuses one timer rather than calling time.After: waitReply runs inside the
// measured loop, and a fresh timer per echoed pkg would put its allocation and
// its runtime timer churn into ns/op and allocs/op.
func (e *benchEcho) waitReply(b *testing.B) {
	b.Helper()
	if !e.replyTimer.Stop() {
		select {
		case <-e.replyTimer.C:
		default:
		}
	}
	e.replyTimer.Reset(replyTimeout)

	select {
	case <-e.replies:
	case <-e.replyTimer.C:
		b.Fatal("no echo came back: the session dropped the pkg (maxMsgLen) or the peer died")
	}
}

// close is idempotent and tolerates a partially built pair, because it can run
// after any early failure in newBenchEcho.
func (e *benchEcho) close() {
	if e.cli != nil {
		e.cli.Close()
	}
	if e.srv != nil {
		e.srv.Close()
	}
	// Give the session goroutines a chance to leave the timer wheel before the
	// next benchmark builds its own pair.
	for _, ss := range e.sessions {
		if ss != nil {
			ss.Close()
		}
	}
}
