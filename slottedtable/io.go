/*
Copyright (c) 2020 Simon Schmidt

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


package slottedtable

import "io"
import "bytes"
import "github.com/vmihailenco/msgpack"

import "errors"

var ErrHashError = errors.New("Hash-Error")

type reader struct {
	bytes.Reader
	buffer []byte
}
func newReader(buf []byte) *reader {
	r := new(reader)
	r.Reset(buf)
	return r
}
func (r *reader) Reset(buf []byte) {
	r.Reader.Reset(buf)
	r.buffer = buf
}
func (r *reader) GetData(lng int) ([]byte,error) {
	size := int64(lng)
	end,err := r.Seek(size,io.SeekCurrent)
	if err!=nil { return nil,err }
	return r.buffer[end-size:end],nil
}

type bytea []byte
func (b *bytea) DecodeMsgpack(dec *msgpack.Decoder) error {
	var t []byte
	var e error
	var l int
	r,ok := dec.Buffered().(*reader)
	if !ok {
		t,e = dec.DecodeBytes()
	} else {
		l,e = dec.DecodeBytesLen()
		if e!=nil { return e }
		t,e = r.GetData(l)
	}
	*b = bytea(t)
	return e
}

func readRecord(dec *msgpack.Decoder) (int64,[]byte,error) {
	var rec bytea
	var chksum uint64
	
	i,err := dec.DecodeInt64()
	if err!=nil { return 0,nil,err }
	if i==0 { return 0,nil,io.EOF }
	err = dec.Decode(&rec)
	if err!=nil { return 0,nil,err }
	chksum,err = dec.DecodeUint64()
	if err!=nil { return 0,nil,err }
	if hashBuf(rec)!=chksum { return 0,nil,ErrHashError }
	
	return i,rec,err
}
func measure(buf []byte) (pos int64,lastid int64,err error) {
	
	reader := newReader(buf)
	dec := msgpack.NewDecoder(reader)
	
	for {
		id2,_,err2 := readRecord(dec)
		if err2==ErrHashError { continue }
		if err2!=nil { break }
		pos,_ = reader.Seek(0,io.SeekCurrent)
		lastid,err = id2,err2
	}
	return
}

