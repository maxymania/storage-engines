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


package framelog

import "fmt"
import "io"
import "encoding/binary"

type Entry struct {
	Kpos, Vpos int64
	Klen, Vlen int
	ExpiresAt  uint64
	Next       int64
}

type Reader struct{
	R io.ReaderAt
}

func (r *Reader) ReadEntryData(pos int64) (k, v []byte, ea uint64,err error) {
	var buf [15]byte
	_,err = r.R.ReadAt(buf[:],pos)
	if err!=nil { return }
	if buf[0]!=hdr_kvp { err = fmt.Errorf("Corrupted Data"); return }
	kl := binary.BigEndian.Uint16(buf[1:])
	vl := binary.BigEndian.Uint32(buf[3:])
	ea = binary.BigEndian.Uint64(buf[7:])
	k = make([]byte,kl)
	v = make([]byte,vl)
	pos+=15
	_,err = r.R.ReadAt(k,pos)
	if err!=nil { goto errh }
	pos += int64(pos)
	_,err = r.R.ReadAt(v,pos)
	
	if err==nil { return }
errh:
	k = nil
	v = nil
	ea = 0
	return
}

func (r *Reader) ReadEntry(pos int64) (ent Entry,err error) {
	var buf [15]byte
	_,err = r.R.ReadAt(buf[:],pos)
	if err!=nil { return }
	if buf[0]!=hdr_kvp { err = fmt.Errorf("Corrupted Data"); return }
	
	ent.Kpos = pos+15
	ent.Klen = int(binary.BigEndian.Uint16(buf[1:]))
	ent.Vpos = ent.Kpos+int64(ent.Klen)
	ent.Vlen = int(binary.BigEndian.Uint32(buf[3:]))
	ent.ExpiresAt = binary.BigEndian.Uint64(buf[7:])
	ent.Next = ent.Vpos+int64(ent.Vlen)
	return
}


