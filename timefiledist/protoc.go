/*
Copyright (c) 2018 Simon Schmidt

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/


package timefiledist

import (
	"github.com/valyala/fastrpc"
	"github.com/valyala/fasthttp"
	"github.com/vmihailenco/msgpack"
	"bufio"
	"net"
	"fmt"
	"io"
	"bytes"
	
	"sync"
)

const (
	SniffHeader = "TFDist"
	ProtocolVersion byte = 2
)

var empty_ = new(bytes.Buffer)

type Message struct{
	_extensible struct{}
	Type    []byte
	Id      []byte
	Exp     uint64
	Ok      bool
	Payload []byte
	dec *msgpack.Decoder
}

func (m *Message) SetPayload(payload []byte) {
	m.Ok,m.Payload = true,append(m.Payload[:0],payload...)
}
func (m *Message) SetError(e error) {
	m.Ok,m.Payload = false,append(m.Payload[:0],e.Error()...)
}

func (m *Message) GetError() error {
	if m.Ok { return nil }
	return fmt.Errorf("remote:%s",m.Payload)
}
func (m *Message) AssignResp(o *Message) {
	m.Ok = o.Ok
	m.Payload = append(m.Payload[:0],o.Payload...)
}

// ConcurrencyLimitError must set the response
// to 'concurrency limit exceeded' error.
func (m *Message) ConcurrencyLimitError(concurrency int) {
	m.SetError(fmt.Errorf("ConcurrencyLimitError(%d)",concurrency))
}

// Init must prepare ctx for reading the next request.
func (m *Message) Init(conn net.Conn, logger fasthttp.Logger) {}

func (m *Message) getDecoder(r io.Reader) *msgpack.Decoder {
	if m.dec==nil {
		m.dec = msgpack.NewDecoder(r)
	} else {
		m.dec.Reset(r)
	}
	return m.dec
}

// ReadRequest must read request from br.
func (m *Message) ReadRequest(br *bufio.Reader) error {
	dec := m.getDecoder(br)
	defer dec.Reset(empty_) /* Unwire the stream, help the GC. */
	return dec.DecodeMulti(&m.Type,&m.Id,&m.Exp,&m.Payload)
}

// WriteResponse must write response to bw.
func (m *Message) WriteResponse(bw *bufio.Writer) error {
	enc := msgpack.NewEncoder(bw)
	return enc.EncodeMulti( m.Ok , m.Payload)
}

// ReadResponse must read response from br.
func (m *Message) ReadResponse(br *bufio.Reader) error {
	dec := m.getDecoder(br)
	defer dec.Reset(empty_) /* Unwire the stream, help the GC. */
	return dec.DecodeMulti(&m.Ok ,&m.Payload)
}

// WriteRequest must write request to bw.
func (m *Message) WriteRequest(bw *bufio.Writer) error {
	enc := msgpack.NewEncoder(bw)
	return enc.EncodeMulti( m.Type, m.Id, m.Exp, m.Payload)
}

var (
	_ fastrpc.RequestWriter  = (*Message)(nil)
	_ fastrpc.ResponseReader = (*Message)(nil)
	_ fastrpc.HandlerCtx     = (*Message)(nil)
)

var messagePool = sync.Pool{ New: func() interface{} { return new(Message) } }

func NewHandlerCtx() fastrpc.HandlerCtx { return messagePool.Get().(fastrpc.HandlerCtx) }
func NewResponse() fastrpc.ResponseReader { return messagePool.Get().(fastrpc.ResponseReader) }

func AcquireMessage() *Message { return messagePool.Get().(*Message) }
func (m *Message) ReleaseMessage() { messagePool.Put(m) }

func dial(addr string) (net.Conn, error) { return net.Dial("tcp",addr) }
func MakeConnection(cli *fastrpc.Client) {
	cli.SniffHeader = SniffHeader
	cli.ProtocolVersion = ProtocolVersion
	cli.NewResponse = NewResponse
	cli.CompressType = fastrpc.CompressNone
	cli.Dial = dial
}
func MakeServer(srv *fastrpc.Server) {
	srv.SniffHeader = SniffHeader
	srv.ProtocolVersion = ProtocolVersion
	srv.NewHandlerCtx = NewHandlerCtx
	srv.CompressType = fastrpc.CompressNone
}

