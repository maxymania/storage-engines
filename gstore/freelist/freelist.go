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


package freelist

import "bytes"
import "github.com/vmihailenco/msgpack"
import "github.com/maxymania/storage-engines/gstore/blockmgr"
import "math"
import "errors"

var errOverflow = errors.New("overflow")
var errNoFreeBlocks = errors.New("noFreeBlocks")

func plen(u uint64) int {
	if u <= math.MaxInt8 { return 1 }
	if u <= math.MaxUint8 { return 2 }
	if u <= math.MaxUint16 { return 3 }
	if u <= math.MaxUint32 { return 5 }
	return 9
}

type bwriter struct{
	targ []byte
	pos  int
}
func (b *bwriter) rest() int {
	return len(b.targ)-b.pos
}
func (b *bwriter) Write(p []byte) (int,error) {
	if len(p)>=b.rest() { return 0,errOverflow }
	copy(b.targ[b.pos:],p)
	b.pos += len(p)
	return len(p),nil
}
func (b *bwriter) WriteString(p string) (int,error) {
	if len(p)>=b.rest() { return 0,errOverflow }
	copy(b.targ[b.pos:],p)
	b.pos += len(p)
	return len(p),nil
}
func (b *bwriter) WriteByte(p byte) error {
	if 0>=b.rest() { return errOverflow }
	b.targ[b.pos] = p
	b.pos++
	return nil
}

type blkSet map[uint64]bool

type alloclist struct{
	bmr     blockmgr.BlockManager
	blksize int
	exclude blkSet
	ba,bf   uint64
	free    []uint64 // ready to use
	dump    []uint64 // ready to recycle
}

func (a *alloclist) addTail(offset uint64,lng int) {
	nf := make([]uint64,lng)
	for i := range nf {
		nf[i] = offset
		offset++
	}
	a.free = append(a.free,nf...)
}

func (a *alloclist) alloc() (uint64,error) {
restart:
	if len(a.free)==0 {
		if a.ba==0 && a.bf==0 {
			o,l,e := a.bmr.GrowStep()
			if e!=nil { return 0,e }
			a.addTail(o,l)
			goto restart
		}
		if a.ba==0 {
			a.bf,a.ba = a.ba,a.bf
		}
		blk,e := a.bmr.ReadBlock(a.ba)
		if e!=nil { return 0,e }
		var nba uint64
		var nf []uint64
		dec := msgpack.NewDecoder(bytes.NewReader(blk))
		e = dec.DecodeMulti(&nba,&nf)
		a.bmr.DiscardRead(blk)
		if e!=nil { return 0,e }
		a.free = append(a.free,nf...)
		a.ba = nba
		goto restart
	}
	ptr := a.free[0]
	a.free = a.free[1:]
	if a.exclude[ptr] {
		a.dump = append(a.dump,ptr)
		goto restart
	}
	return ptr,nil
}

func (a *alloclist) putblock(u uint64) {
	a.dump = append(a.dump,u)
}

func encodes(enc *msgpack.Encoder,u []uint64) (err error) {
	err = enc.EncodeArrayLen(len(u))
	if err!=nil { return }
	for _,uu := range u {
		err = enc.EncodeUint(uu)
		if err!=nil { return }
	}
	return
}

func (a *alloclist) writefrees() error {
restart:
	if len(a.free)==0 { return nil }
	if a.exclude[a.free[0]] {
		for i,n := range a.free {
			if !a.exclude[n] { a.free[0],a.free[i] = a.free[i],a.free[0] ; goto done }
		}
	} else { goto done }
	
	// XXX is this a good solution?
	{
		o,l,e := a.bmr.GrowStep()
		if e!=nil { return e }
		a.addTail(o,l)
		goto restart
	}
	// return errNoFreeBlocks
	
done:
	BZ := a.blksize-18
	h := 0
	snt := a.free[0]
	rest := a.free[1:]
	l := len(rest)
	for i,d := range rest {
		h += plen(d)
		if h>BZ { l = i; break }
	}
	blk,err := a.bmr.Allocate(snt)
	if err!=nil { return err }
	enc := msgpack.NewEncoder(&bwriter{targ:blk})
	err = enc.EncodeUint(a.ba)
	if err!=nil { return err }
	err = encodes(enc,rest[:l])
	if err!=nil { return err }
	err = a.bmr.WriteBack(snt,blk)
	if err!=nil { return err }
	a.ba = snt
	a.free = rest[l:]
	goto restart
}

func (a *alloclist) writeback() error {
	BZ := a.blksize-18
	var rb []uint64
	var bb [][]uint64
	for {
		if len(a.dump)==0 { break }
		h := 0
		blk,err := a.alloc()
		if err!=nil { return err }
		for i,d := range a.dump {
			h += plen(d)
			if h>BZ {
				bb = append(bb,a.dump[:i])
				a.dump = a.dump[i:]
			}
		}
		if h<=BZ {
			bb = append(bb,a.dump)
			a.dump = nil
		}
		rb = append(rb,blk)
	}
	for i,r := range rb {
		blk,err := a.bmr.Allocate(r)
		if err!=nil { return err }
		enc := msgpack.NewEncoder(&bwriter{targ:blk})
		err = enc.EncodeUint(a.bf)
		if err!=nil { return err }
		err = enc.Encode(bb[i])
		if err!=nil { return err }
		err = a.bmr.WriteBack(r,blk)
		if err!=nil { return err }
		a.bf = r
	}
	return nil
}


