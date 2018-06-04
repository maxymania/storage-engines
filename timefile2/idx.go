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


package timefile

import (
	"errors"
	"github.com/maxymania/storage-engines/leveldbx"
	"os"
	"io"
	"sync"

	"bytes"
	"encoding/binary"
	ldb_errors "github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/opt"
)
import "github.com/syndtr/goleveldb/leveldb/cache"

var (
	EExist = errors.New("EExist")
	ENotFound = ldb_errors.ErrNotFound
	EOverSize = errors.New("EOverSize")
)

type Getter interface{
	SetValue(f io.ReaderAt,off int64,lng int32) error
}
type Unwrapper_os_File interface {
	Unwrap_os_File() *os.File
}

var readerPool = sync.Pool{New: func() interface{} { return new(bytes.Reader) }}
func reclaimReader(b *bytes.Reader) {
	b.Reset(nil)
	readerPool.Put(b)
}

type storeHeader struct{
	FileID uint64
	Offset int64
	Length int32
}
func (s *storeHeader) decode(b []byte) error {
	r := readerPool.Get().(*bytes.Reader)
	r.Reset(b)
	defer reclaimReader(r)
	return binary.Read(r,binary.BigEndian,s)
}
func (s storeHeader) encode() []byte {
	var buf bytes.Buffer
	binary.Write(&buf,binary.BigEndian,s)
	return buf.Bytes()
}

type AutoExpire struct{}
func (AutoExpire) Retain(b []byte) bool {
	var s storeHeader
	if s.decode(b)!=nil { return true }
	return s.FileID < current
}


type iFile struct{
	*os.File
	length int64
	lock sync.Mutex
	klist *kArray
	k uint64
}
func (i *iFile) Unwrap_os_File() *os.File { return i.File }
func (i *iFile) Release() {
	i.klist.remove(i.k,i)
	i.Close()
}
func (i *iFile) AppendMz(b []byte,max int64) (int64,error) {
	if max<=0 { return i.Append(b) }
	i.lock.Lock(); defer i.lock.Unlock()
	cur := i.length
	nwl := cur + int64(len(b))
	if nwl>max { return 0,EOverSize }
	_,e := i.WriteAt(b,cur)
	if e!=nil {
		i.Truncate(cur) // Revert growth, if any!
		return 0,e
	}
	i.length = nwl
	return cur,nil
}
func (i *iFile) Append(b []byte) (int64,error) {
	i.lock.Lock(); defer i.lock.Unlock()
	cur := i.length
	_,e := i.WriteAt(b,cur)
	if e!=nil {
		i.Truncate(cur) // Revert growth, if any!
		return 0,e
	}
	i.length = cur + int64(len(b))
	return cur,nil
}

type Store struct{
	Alloc *Allocator
	DB    *leveldb.DB
	MaxSizePerFile int64 // Maximum file size or 0
	MaxDayOffset   int   // Maximum days of later expiration
	files lCache
	klist kArray
}
func (s *Store) getfile(k uint64) cache.Value {
	fn := s.Alloc.GetPath(k)
	f,e := os.OpenFile(fn,os.O_RDWR|os.O_CREATE,0644)
	if e!=nil { return nil }
	r := new(iFile)
	r.File = f
	r.length,e = f.Seek(0,2)
	r.klist = &s.klist
	r.k = k
	if e!=nil {
		f.Close()
		return nil
	}
	r.klist.insert(k,r)
	return r
}
func (s *Store) Init(size int) {
	if size<=0 { size = 1024 }
	s.klist.init()
	s.files.init(size,s.getfile)
}

// Erases the handles of expired files.
// This allow the FS to reclaim disk space, if they are unkink()-ed.
func (s *Store) CleanupInstance() {
	// Erase handles of expired files. This allow the FS to reclaim disk space, if they are unkink()-ed.
	for _,e := range s.klist.until(current,1<<16) {
		s.files.remove(e)
	}
	//for _,e := range s.files.keys() {
	//	if (e.(uint64))<current { s.files.remove(e) }
	//}
}

var wopt = &opt.WriteOptions{ Sync:false, }

var bufferPool = sync.Pool{ New: func() interface{} { return new(bytes.Buffer) } }
func reclaimBuffer(b *bytes.Buffer) {
	b.Reset()
	bufferPool.Put(b)
}

func (s *Store) indexPut(k []byte, v storeHeader, o *opt.WriteOptions) error {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer reclaimBuffer(buf)
	binary.Write(buf,binary.BigEndian,v)
	return s.DB.Put(k,buf.Bytes(),o)
}
func (s *Store) Insert(k, v []byte, expireAt uint64) error {
	return s.insert_2(k, v, expireAt)
}

func (s *Store) insert_2(k, v []byte, expireAt uint64) error {
	
	ok,err := s.DB.Has(k,nil)
	if ok && err==nil { return EExist }
	tfn,err := s.Alloc.AllocateTimeFile(expireAt)
	nExp := expireAt
	
	cnt := 0
	dayoff := 0
	
	for {
		
		if err!=nil { return err }
		ce := s.files.get(tfn)
		if ce==nil { return EFalse }
		defer ce.Release()
		pos,err := ce.Value().(*iFile).AppendMz(v,s.MaxSizePerFile)
		if err==EOverSize {
			for {
				if cnt>128 { return err } /* Limit the iterations! */
				if dayoff>s.MaxDayOffset { return err } /* Maximum day-offset reached! */
				tfn,err = s.Alloc.GrabAnotherFile(nExp,tfn)
				cnt++
				if err==EOptionsExhausted {
					/* nExp += secDay // Bump the expiration date */
					
					// Bump the expiration date by 1 day.
					nExp = trunci(nExp+secDay,secDay) + (60*60)
					dayoff++
					continue
				}
				if err!=nil { return err }
				break
			}
			continue
		}
		if err!=nil { return err }
		
		//return s.DB.Put(k,storeHeader{tfn,pos,int32(len(v))}.encode(),wopt)
		return s.indexPut(k,storeHeader{tfn,pos,int32(len(v))},wopt)
	}
	panic("unreachable")
}

func (s *Store) Get(key []byte, value Getter) error {
	//defer s.CleanupInstance()
	pos,err := s.DB.Get(key,nil)
	if err!=nil { return err }
	var p storeHeader
	err = p.decode(pos)
	if err!=nil { return err }
	if p.FileID < current { return ldb_errors.ErrNotFound }
	
	ce := s.files.get(p.FileID)
	if ce==nil { return EFalse }
	defer ce.Release()
	
	return value.SetValue(ce.Value().(*iFile),p.Offset,p.Length)
}


