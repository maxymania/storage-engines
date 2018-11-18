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


package blkalloc

import "io"
import "encoding/binary"
import "bytes"
import "github.com/cznic/file"
import "errors"
import "github.com/RoaringBitmap/roaring"
import "sync"

// 1 k
const K = 1<<10
const kM = K-1

const k64L = 16
const k64 = 1<<k64L
const k64M = k64-1

var (
	ErrInvalidState = errors.New("ErrInvalidState")
	ErrNotFound = errors.New("ErrNotFound")
)

//ErrNotFound = errors.New("ErrNotFound") // obsolete

var lE = binary.LittleEndian

var buffs = sync.Pool{New: func()interface{}{ return new(bytes.Buffer) }}

func bufpt(b *bytes.Buffer) {
	if b==nil { return }
	b.Reset()
	buffs.Put(b)
}

func binarize(data interface{}) []byte {
	var buf bytes.Buffer
	binary.Write(&buf,lE,data)
	return buf.Bytes()
}

type block [K]int64
type mblock struct{
	Direct [K]int64 // % 1<<10
	Ind       int64 // % 1<<20
	DblInd    int64 // % 1<<30
	TriInd    int64 // % 1<<40
	QuadInd   int64 // % 1<<50
}

/*
Basic block free-list/free-table. Not thread safe.
This free table supports all block addresses between 0 and 2⁶⁴-1 .
*/
type RawAlloc struct{
	F file.File
	A *file.Allocator
	MB int64
	c mblock
}
func (r *RawAlloc) Init() (err error,modified bool) {
	var b [8]byte
	_,err = r.F.ReadAt(b[:],0)
	if err!=nil { return }
	r.MB = int64(lE.Uint64(b[:]))
	if r.MB == 0 {
		modified = true
		r.MB,err = r.A.Alloc(int64(binary.Size(&r.c)))
		if err!=nil { return }
		r.c = mblock{}
		_,err = r.F.WriteAt(binarize(&r.c),r.MB)
		if err!=nil { return }
		err = r.A.Flush()
	} else {
		err = binary.Read(io.NewSectionReader(r.F,r.MB,int64(binary.Size(&r.c))),lE,&r.c)
	}
	return
}

func (r *RawAlloc) mkblock() (int64,error) {
	var b block
	i,e := r.A.Alloc(int64(binary.Size(&b)))
	if e!=nil { return 0,e }
	_,e = r.F.WriteAt(binarize(&b),i)
	if e!=nil { return 0,e }
	return i,nil
}
func (r *RawAlloc) ri(od int64,i uint,u uint64) (int64,error) {
	var err error
	var b block
	for {
		if od==0 { return 0,nil } // Default to 0
		v := (u>>(i*10))&kM
		err = binary.Read(io.NewSectionReader(r.F,od,int64(binary.Size(&b))),lE,&b)
		if err!=nil { return 0,err }
		od = b[v]
		if i==0 { break }
		i--
	}
	return od,err
}
func (r *RawAlloc) wi(od int64,i uint,u uint64,z int64) error {
	var nd int64
	var err error
	var b block
	for {
		v := (u>>(i*10))&kM
		err = binary.Read(io.NewSectionReader(r.F,od,int64(binary.Size(&b))),lE,&b)
		if err!=nil { return err }
		if i==0 {
			b[v] = z
			_,err = r.F.WriteAt(binarize(&b),od)
			return err
		}
		nd = b[v]
		if nd==0 {
			nd,err = r.mkblock()
			if err!=nil { return err }
			b[v] = nd
		}
		_,err = r.F.WriteAt(binarize(&b),od)
		if err!=nil { return err }
		i--
		od = nd
	}
}
var scales = []uint64 {
	1<<10, // Direct
	1<<20, // Indirect Block
	1<<30, // Double Indirect Block
	1<<40, // Tribble Indirect Block
	1<<50, // Quad Indirect Block
}
func (r *RawAlloc) jptr(u uint64) (j int,p *int64,v uint64){
	j = -1
	for i,sc := range scales {
		if sc>u { j = i ; break }
		u -= sc
	}
	v = u
	switch j {
	case 0: p = &r.c.Direct[u]
	case 1: p = &r.c.Ind
	case 2: p = &r.c.DblInd
	case 3: p = &r.c.TriInd
	case 4: p = &r.c.QuadInd
	}
	return
}
func (r *RawAlloc) lookup(u uint64) (int64,error) {
	j,p,v := r.jptr(u)
	if p==nil { return 0,ErrInvalidState }
	if j==0 { return *p,nil }
	return r.ri(*p,uint(j),v)
}
func (r *RawAlloc) store(u uint64,z int64) (err error) {
	j,p,v := r.jptr(u)
	if p==nil { return ErrInvalidState }
	if j==0 {
		*p = z
		_,err = r.F.WriteAt(binarize(&r.c),r.MB)
		return
	}
	if *p==0 {
		*p,err = r.mkblock()
		if err!=nil { return }
	}
	
	return r.wi(*p,uint(j),v,z)
}

var bitmaps = sync.Pool{New: func()interface{}{ return roaring.New() }}
func bput(b *roaring.Bitmap) {
	if b==nil { return }
	b.Clear()
	bitmaps.Put(b)
}

func (r *RawAlloc) loadBitmap(od int64) (bm *roaring.Bitmap,err error) {
	if od==0 { panic("BUG: od = 0") }
	var hdr uint32
	err = binary.Read(io.NewSectionReader(r.F,od,16),lE,&hdr)
	if err!=nil { return }
	bm = bitmaps.Get().(*roaring.Bitmap)
	_,err = bm.ReadFrom(io.NewSectionReader(r.F,od+4,int64(hdr)))
	if err!=nil { bput(bm); bm = nil }
	return
}
func (r *RawAlloc) loadBitmapSeg(u uint64) (od int64,bm *roaring.Bitmap,err error) {
	od,err = r.lookup(u)
	if err!=nil { return }
	if od==0 {
		bm = bitmaps.Get().(*roaring.Bitmap)
	} else {
		bm,err = r.loadBitmap(od)
	}
	return
}
func (r *RawAlloc) loadBitmapSegOpt(u uint64) (od int64,bm *roaring.Bitmap,err error) {
	od,err = r.lookup(u)
	if err!=nil { return }
	if od==0 {
		err = ErrNotFound
	} else {
		bm,err = r.loadBitmap(od)
	}
	return
}
func (r *RawAlloc) storeBitmap(od int64,bm *roaring.Bitmap) (err error) {
	buf := buffs.Get().(*bytes.Buffer)
	defer bufpt(buf)
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(0)
	buf.WriteByte(0)
	_,err = bm.WriteTo(buf)
	if err!=nil { return }
	b := buf.Bytes()
	bl := len(b)-4
	lE.PutUint32(b,uint32(bl))
	_,err = r.F.WriteAt(b,od)
	return
}
func (r *RawAlloc) storeBitmapSeg(u uint64,od int64,bm *roaring.Bitmap) (err error) {
	nod := od==0
	if nod {
		od,err = r.A.Alloc(0x20ff)
		if err!=nil { return }
	}
	err = r.storeBitmap(od,bm)
	if err!=nil { return }
	if nod { err = r.store(u,od) }
	return
}
func (r *RawAlloc) storeBitmapSegOpt(u uint64,od int64,bm *roaring.Bitmap) (err error) {
	if bm.GetCardinality()==0 {
		if od!=0 {
			err = r.A.Free(od)
			if err!=nil { return }
			err = r.store(u,0)
		}
		return
	}
	return r.storeBitmapSeg(u,od,bm)
}
func (r *RawAlloc) storeBitmapSegContains(u uint64,od int64,bm *roaring.Bitmap) (err error,cont bool) {
	if bm.GetCardinality()==0 {
		if od!=0 {
			err = r.A.Free(od)
			if err!=nil { return }
			err = r.store(u,0)
		}
		return
	}
	return r.storeBitmapSeg(u,od,bm),true
}
func (r *RawAlloc) xorBitmapSeg(u uint64,bm *roaring.Bitmap) error{
	od,obm,err := r.loadBitmapSeg(u)
	if err!=nil { return err }
	defer bput(obm)
	obm.Xor(bm)
	return r.storeBitmapSegOpt(u,od,obm)
}
func (r *RawAlloc) orBitmapSeg(u uint64,bm *roaring.Bitmap) error{
	od,obm,err := r.loadBitmapSeg(u)
	if err!=nil { return err }
	defer bput(obm)
	obm.Or(bm)
	return r.storeBitmapSegOpt(u,od,obm)
}
func (r *RawAlloc) ContainsBlock(u uint64) (bool,error) {
	_,obm,err := r.loadBitmapSeg(u>>k64L)
	if err!=nil { return false,err }
	defer bput(obm)
	return obm.Contains(uint32(u&k64M)),nil
}

func (r *RawAlloc) AddBlock(u uint64) error {
	od,obm,err := r.loadBitmapSeg(u>>k64L)
	if err!=nil { return err }
	defer bput(obm)
	obm.Add(uint32(u&k64M))
	return r.storeBitmapSegOpt(u>>k64L,od,obm)
}
func (r *RawAlloc) RemoveBlock(u uint64) error {
	od,obm,err := r.loadBitmapSeg(u>>k64L)
	if err!=nil { return err }
	defer bput(obm)
	obm.Remove(uint32(u&k64M))
	return r.storeBitmapSegOpt(u>>k64L,od,obm)
}

/*
Allocator with slot-cache. Not thread safe.
Supports block addresses between 0 and 2⁴⁸-1.
*/
type CachedAlloc struct{
	*RawAlloc
	
	
	exists *roaring.Bitmap
	unchk *roaring.Bitmap
	
	// both = exists | unchk
	both *roaring.Bitmap
	
	both_flip *roaring.Bitmap
	both_locked bool
}
func (r *CachedAlloc) lock() {
	r.both_locked = true
}
func (r *CachedAlloc) unlock() {
	r.both_locked = false
	r.both.Xor(r.both_flip)
	r.both_flip.Clear()
}
func (r *CachedAlloc) Init2(lng uint64) (err error){
	r.exists = roaring.New()
	r.unchk  = roaring.New()
	r.both   = roaring.New()
	r.both_flip = roaring.New()
	if lng > 1<<48 { panic("...") }
	r.unchk.AddRange(0, (lng+k64M)>>k64L)
	r.both.AddRange(0, (lng+k64M)>>k64L)
	return nil
}
func (r *CachedAlloc) sc_lookup(u uint64) (od int64,err error) {
	if u >= 1<<32 { return r.lookup(u) }
	if r.unchk.Contains(uint32(u)) {
		od,err = r.lookup(u)
		if err!=nil { return }
		r.unchk.Remove(uint32(u))
		if od==0 {
			if r.both_locked {
				r.both_flip.Add(uint32(u))
			} else {
				r.both.Remove(uint32(u))
			}
		} else {
			r.exists.Add(uint32(u))
		}
		return
	}
	if !r.exists.Contains(uint32(u)) { return 0,nil }
	return r.lookup(u)
}

func (r *CachedAlloc) loadBitmapSeg(u uint64) (od int64,bm *roaring.Bitmap,err error) {
	od,err = r.sc_lookup(u)
	if err!=nil { return }
	if od==0 {
		bm = bitmaps.Get().(*roaring.Bitmap)
	} else {
		bm,err = r.loadBitmap(od)
	}
	return
}
func (r *CachedAlloc) loadBitmapSegOpt(u uint64) (od int64,bm *roaring.Bitmap,err error) {
	od,err = r.sc_lookup(u)
	if err!=nil { return }
	if od==0 {
		err = ErrNotFound
	} else {
		bm,err = r.loadBitmap(od)
	}
	return
}
func (r *CachedAlloc) storeBitmapSegOpt(u uint64,od int64,bm *roaring.Bitmap) (err error) {
	var ok bool
	err,ok = r.storeBitmapSegContains(u,od,bm)
	if u < 1<<32 {
		if ok {
			r.exists.Add(uint32(u))
		} else {
			r.exists.Remove(uint32(u))
		}
		r.unchk.Remove(uint32(u))
		if r.both_locked {
			if ok==r.both.Contains(uint32(u)) {
				r.both_flip.Remove(uint32(u))
			} else {
				r.both_flip.Add(uint32(u))
			}
		} else {
			if ok {
				r.both.Add(uint32(u))
			} else {
				r.both.Remove(uint32(u))
			}
		}
	}
	return
}
func (r *CachedAlloc) xorBitmapSeg(u uint64,bm *roaring.Bitmap) error{
	od,obm,err := r.loadBitmapSeg(u)
	if err!=nil { return err }
	defer bput(obm)
	obm.Xor(bm)
	return r.storeBitmapSegOpt(u,od,obm)
}
func (r *CachedAlloc) orBitmapSeg(u uint64,bm *roaring.Bitmap) error{
	od,obm,err := r.loadBitmapSeg(u)
	if err!=nil { return err }
	defer bput(obm)
	obm.Or(bm)
	return r.storeBitmapSegOpt(u,od,obm)
}
func (r *CachedAlloc) ContainsBlock(u uint64) (bool,error) {
	_,obm,err := r.loadBitmapSeg(u>>k64L)
	if err!=nil { return false,err }
	defer bput(obm)
	return obm.Contains(uint32(u&k64M)),nil
}
func (r *CachedAlloc) AddBlock(u uint64) error {
	od,obm,err := r.loadBitmapSeg(u>>k64L)
	if err!=nil { return err }
	defer bput(obm)
	obm.Add(uint32(u&k64M))
	return r.storeBitmapSegOpt(u>>k64L,od,obm)
}
func (r *CachedAlloc) RemoveBlock(u uint64) error {
	od,obm,err := r.loadBitmapSeg(u>>k64L)
	if err!=nil { return err }
	defer bput(obm)
	obm.Remove(uint32(u&k64M))
	return r.storeBitmapSegOpt(u>>k64L,od,obm)
}


func (r *CachedAlloc) find_1(i uint32,f func(uint64)bool) (uint64,bool,error) {
	_,obm,err := r.loadBitmapSeg(uint64(i))
	if err!=nil { return 0,false,err }
	defer bput(obm)
	iter := obm.Iterator()
	v := uint64(i)<<k64L
	for iter.HasNext() {
		j := iter.Next()
		u := uint64(j)|v
		if f(u) { return u,true,nil }
	}
	
	return 0,false,nil
}
func (r *CachedAlloc) allocate_1(i uint32,f func(uint64)bool) (uint64,bool,error) {
	od,obm,err := r.loadBitmapSeg(uint64(i))
	if err!=nil { return 0,false,err }
	defer bput(obm)
	iter := obm.Iterator()
	v := uint64(i)<<k64L
	for iter.HasNext() {
		j := iter.Next()
		u := uint64(j)|v
		if !f(u) { continue }
		obm.Remove(j)
		return u,true,r.storeBitmapSegOpt(uint64(i),od,obm)
	}
	
	return 0,false,nil
}
func (r *CachedAlloc) get_1(a bool,i uint32,f func(uint64)bool) (uint64,bool,error) {
	if a { return r.allocate_1(i,f) }
	return r.find_1(i,f)
}
func (r *CachedAlloc) alloc_0(a bool,f func(uint64)bool) (uint64,bool,error) {
	r.lock(); defer r.unlock()
	iter := r.both.Iterator()
	for iter.HasNext() {
		i := iter.Next()
		u,ok,err := r.get_1(a,i,f)
		if err!=nil || ok { return u,ok,err }
	}
	return 0,false,nil
}

func anyBlock (uint64)bool { return true }

func (r *CachedAlloc) Allocate() (uint64,bool,error) {
	return r.alloc_0(true,anyBlock)
}


