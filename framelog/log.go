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

import "encoding/binary"

const (
	hdr_kvp uint8 = 0x47
)

type kvPair struct{
	Hdr       uint8
	Key       uint16
	Value     uint32
	ExpiresAt uint64
}


type Writer struct{
	Cw CountWriter
	Damage error
}

func (w *Writer) Insert(k, v []byte, ea uint64) int64 {
	kvp := kvPair{
		Hdr: hdr_kvp,
		Key: uint16(len(k)),
		Value: uint32(len(v)),
		ExpiresAt: ea,
	}
	pos := w.Cw.Count
	binary.Write(&(w.Cw),binary.BigEndian,kvp)
	_,e := w.Cw.Write(k)
	if e!=nil { w.Damage = e }
	_,e = w.Cw.Write(v)
	if e!=nil { w.Damage = e }
	return pos
}

