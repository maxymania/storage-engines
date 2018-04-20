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

var (
	EExist = errors.New("EExist")
	ENotFound = ldb_errors.ErrNotFound
)

type Getter interface{
	SetValue(f io.ReaderAt,off int64,lng int32) error
}
type Unwrapper_os_File interface {
	Unwrap_os_File() *os.File
}

type storeHeader struct{
	FileID uint64
	Offset int64
	Length int32
}
func (s *storeHeader) decode(b []byte) error {
	r := bytes.NewReader(b)
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
}
func (i *iFile) Unwrap_os_File() *os.File { return i.File }
func (i *iFile) Release() {
	i.Close()
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
	files cCache
}
func (s *Store) getfile(k interface{}) Releaser {
	fn := s.Alloc.GetPath(k.(uint64))
	f,e := os.OpenFile(fn,os.O_RDWR|os.O_CREATE,0644)
	if e!=nil { return nil }
	r := new(iFile)
	r.File = f
	r.length,e = f.Seek(0,2)
	if e!=nil {
		f.Close()
		return nil
	}
	return r
}
func (s *Store) Init(size int) {
	if size<=0 { size = 1024 }
	s.files.init(size,s.getfile)
}

// Erases the handles of expired files.
// This allow the FS to reclaim disk space, if they are unkink()-ed.
func (s *Store) CleanupInstance() {
	// Erase handles of expired files. This allow the FS to reclaim disk space, if they are unkink()-ed.
	for _,e := range s.files.keys() {
		if (e.(uint64))<current { s.files.remove(e) }
	}
}

var wopt = &opt.WriteOptions{ Sync:false, }

func (s *Store) Insert(k, v []byte, expireAt uint64) error {
	ok,err := s.DB.Has(k,nil)
	if ok && err==nil { return EExist }
	tfn,err := s.Alloc.AllocateTimeFile(expireAt)
	if err!=nil { return err }
	ce := s.files.get(tfn)
	if ce==nil { return EFalse }
	defer ce.release()
	pos,err := ce.value.(*iFile).Append(v)
	if err!=nil { return err }
	
	return s.DB.Put(k,storeHeader{tfn,pos,int32(len(v))}.encode(),wopt)
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
	defer ce.release()
	
	return value.SetValue(ce.value.(*iFile),p.Offset,p.Length)
}


