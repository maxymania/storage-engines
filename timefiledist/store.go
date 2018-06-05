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

import timefile "github.com/maxymania/storage-engines/timefile2"
import leveldb "github.com/maxymania/storage-engines/leveldbx"
import "encoding/binary"
import "github.com/vmihailenco/msgpack"
import "io"
import "github.com/syndtr/goleveldb/leveldb/util"

import "github.com/byte-mug/golibs/bufferex"

func u2b(u uint64) []byte {
	v := make([]byte,8)
	binary.BigEndian.PutUint64(v,u)
	return v
}
func b2u(b []byte) uint64 {
	if len(b)<8 { return 0 }
	return binary.BigEndian.Uint64(b)
}

type reader struct{
	b bufferex.Binary
}
func (r *reader) SetValue(f io.ReaderAt, off int64, lng int32) error {
	r.b = bufferex.AllocBinary(int(lng))
	_,err := f.ReadAt(r.b.Bytes(),off)
	return err
}

type Modifier func(b *bufferex.Binary,e uint64)

func defaultModifier (b *bufferex.Binary,e uint64) {}


type HeadStorage struct{
	Store *timefile.Store
	DB    *leveldb.DB
	Mod   Modifier
	LS    *Landscape
	LHash uint64
}
func (h *HeadStorage) work(hl Notify) {
	
	/* Check, if we have a range, that overflows. */
	cut := hl.Start>hl.Limit
	
	var r *util.Range
	
	/* In this case, we have a range without overflow. */
	if hl.Start<hl.Limit {
		r = &util.Range{u2b(hl.Start),u2b(hl.Limit)}
	}
	
	i := h.Store.DB.NewIterator(r,nil)
	defer i.Release()
	
	if !i.First() { return }
	
	var p []pair
	for {
		K1 := i.Key()
		if cut && b2u(K1)>=hl.Limit {
			/* Skip the gap between Limit and Start. */
			if !i.Seek(u2b(hl.Start)) { break }
			K1 = i.Key()
		}
		V1 := i.Value()
		if b2u(V1)<current { /* Expired! Next! */; continue }
		v := make([]byte,16)
		copy(v[:8],V1)
		binary.BigEndian.PutUint64(v[8:],h.LHash)
		p = append(p,pair{K1,v})
		if len(p) > (1<<12) {
			hl.C.sendPairs(p)
			p = p[:0]
		}
	}
	if len(p) != 0 {
		hl.C.sendPairs(p)
	}
}
func (h *HeadStorage) Worker() {
	for hl := range h.LS.Ntfr {
		go h.work(hl)
	}
}


func (h *HeadStorage) Handle(m *Message) *Message {
	var p []pair
	switch string(m.Type) {
	case "lookup":
		v1,_ := h.DB.Get(m.Id,nil)
		if len(v1)>=16 {
			data,_ := msgpack.Marshal(binary.BigEndian.Uint64(v1))
			m.SetPayload(data)
		}
	case "lookup|read":
		v1,_ := h.DB.Get(m.Id,nil)
		v2,_ := h.Store.DB.Get(m.Id,nil)
		if len(v2)!=0 {
			var b reader
			err := h.Store.Get(m.Id, &b)
			defer b.b.Free()
			if err!=nil {
				m.SetError(err)
			} else {
				h.Mod(&b.b,m.Exp)
				m.SetPayload(b.b.Bytes())
				return m
			}
		}
		if len(v1)>=16 {
			data,_ := msgpack.Marshal(false,binary.BigEndian.Uint64(v1))
			m.SetPayload(data)
		}
	case "read":
		var b reader
		err := h.Store.Get(m.Id, &b)
		defer b.b.Free()
		if err!=nil {
			m.SetError(err)
		} else {
			h.Mod(&b.b,m.Exp)
			m.SetPayload(b.b.Bytes())
		}
	case "put":
		u := m.GetKeyHash()
		err := h.Store.Insert(m.Id,m.Payload,m.Exp)
		if err!=nil {
			m.SetError(err)
		} else {
			m.SetPayload(nil)
			n := h.LS.find(u)
			if n!=nil {
				v2,_ := h.Store.DB.Get(m.Id,nil)
				v := make([]byte,16)
				copy(v[:8],v2)
				binary.BigEndian.PutUint64(v[8:],h.LHash)
				n.Value.(*Client).sendPairs([]pair{{m.Id,v}})
			}
		}
	case "index":
		err := msgpack.Unmarshal(m.Payload,&p)
		if err!=nil { p = nil }
		
		if len(p)>1 {
			i := 0
			b := new(leveldb.Batch)
			for _,pair := range p {
				b.Put(pair.Key,pair.Value)
				i++
				if i > (1<<12) {
					h.DB.Write(b,nil)
					b.Reset()
					i=0
				}
			}
			if i > 0 {
				h.DB.Write(b,nil)
			}
		} else {
			for _,pair := range p { h.DB.Put(pair.Key,pair.Value,nil) }
		}
	default:
		return nil
	}
	return m
}

