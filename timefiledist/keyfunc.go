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

import "encoding/binary"
import farm "github.com/dgryski/go-farm"
import "github.com/byte-mug/golibs/bufferex"

func EncodeKey(b []byte) bufferex.Binary {
	r := bufferex.AllocBinary(len(b)+8)
	copy(r.Bytes()[8:],b)
	binary.BigEndian.PutUint64(r.Bytes(),farm.Fingerprint64(r.Bytes()[8:]))
	return r
}

func (m *Message) SetKey(k []byte) {
	b := EncodeKey(k)
	defer b.Free()
	m.Id = append(m.Id[:0],b.Bytes()...)
}
func (m *Message) GetKeyHash() uint64 {
	id := m.Id
	if len(id)<8 { return 0 }
	return binary.BigEndian.Uint64(id)
}

