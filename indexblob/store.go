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


package indexblob

import "sync"
import "io"
import "github.com/maxymania/storage-engines/leveldbx"
import "github.com/syndtr/goleveldb/leveldb/storage"
import "github.com/syndtr/goleveldb/leveldb/errors"
import "github.com/maxymania/storage-engines/framelog"
import "github.com/syndtr/goleveldb/leveldb/util"
import "encoding/json"
import "encoding/binary"
import "sort"
import "bytes"
import "fmt"

var EExist = fmt.Errorf("EExist")

type aexp struct{}

func (aexp) Retain(b []byte) bool {
	var v value
	if v.decode(b)!=nil { return true }
	return current < v.Expire
}


type value struct{
	Fd,Pos int64
	Expire uint64
}
func (v *value) decode(b []byte) error {
	return binary.Read(bytes.NewReader(b),binary.BigEndian,v)
}

type manifest struct {
	ok      bool
	Version uint64  `json:"version"`
	Files   []int64 `json:"files"`
}
func (m *manifest) load(rdr storage.Reader) (err error) {
	err = json.NewDecoder(rdr).Decode(m)
	m.ok = err==nil
	return
}
func (m *manifest) loadFrom(stor storage.Storage,fd storage.FileDesc) error {
	f,err := stor.Open(fd)
	if err!=nil { return err }
	defer f.Close()
	return m.load(f)
}
func (m *manifest) storeTo(stor storage.Storage,fd storage.FileDesc) error {
	f,err := stor.Create(fd)
	if err!=nil { return err }
	defer f.Close()
	return json.NewEncoder(f).Encode(*m)
}


type Store struct {
	
	db *leveldb.DB
	
	data storage.Storage
	instcache cCache
	
	targ struct {
		sync.Mutex
		framelog.Writer
		FD storage.FileDesc
	}
	
	mani struct{
		sync.Mutex
		m   manifest
		bnp int64
		vers [5]uint64
	}
	
	compaction chan struct{}
}
func OpenStore(idx, data storage.Storage) (s *Store,err error) {
	s = new(Store)
	
	s.db,err = leveldb.Open(idx,nil,aexp{})
	if errors.IsCorrupted(err) {
		s.db,err = leveldb.Recover(idx,nil,aexp{})
	}
	
	s.data = data
	s.instcache.init(128)
	s.compaction = make(chan struct{},1)
	return
}
func (s *Store) open(k interface{}) util.Releaser {
	fd := k.(storage.FileDesc)
	r,err := s.data.Open(fd)
	if err!=nil { return nil }
	rdr := new(reader)
	rdr.R = r
	return rdr
}
func (s *Store) getManifest() {
	s.mani.Lock(); defer s.mani.Unlock()
	if s.mani.m.ok { return }
	var a manifest
	for i := range s.mani.vers { s.mani.vers[i] = 0 }
	for i := int64(1); i<5; i++ {
		if a.loadFrom(s.data,storage.FileDesc{storage.TypeManifest,i})!=nil { continue }
		s.mani.vers[i] = a.Version
		if s.mani.m.ok {
			if s.mani.m.Version >= a.Version { continue }
		}
		s.mani.m = a
	}
	// No manifest?
	l,err := s.data.List(storage.TypeTable)
	if err!=nil { l = nil }
	l2 := make([]int64,len(l))
	for i,e := range l { l2[i]=e.Num }
	sort.Slice(l2,func(i,j int) bool { return l2[i]<l2[j] })
	s.mani.m = manifest{true,1,l2}
}
func (s *Store) store() {
	min := int64(0)
	mv := ^uint64(0)
	for i := int64(1); i<5; i++ {
		if s.mani.vers[i]==0 { continue }
		if mv>s.mani.vers[i] {
			min = i
			mv = s.mani.vers[i]
		}
	}
	for i := int64(1); i<5; i++ {
		if min!=0 { break }
		if s.mani.vers[i]!=0 { continue }
		min = 0
		mv = s.mani.vers[i]
	}
	if s.mani.m.storeTo(s.data,storage.FileDesc{storage.TypeManifest,min}) == nil {
		s.mani.vers[min] = s.mani.m.Version
	}
}
func (s *Store) allocTarget() (storage.FileDesc,bool) {
	s.getManifest()
	s.mani.Lock(); defer s.mani.Unlock()
	f := s.mani.m.Files
	if len(f)==0 {
		s.mani.m.Files = append(f,1)
		s.store()
		return storage.FileDesc{storage.TypeTable,1},true
	}
	nn := f[len(f)-1]+1
	if nn>=1000000 {
		nn=1
		if f[0]==nn { return storage.FileDesc{},false }
	}
	s.mani.m.Files = append(f,1)
	s.store()
	return storage.FileDesc{storage.TypeTable,nn},true
}

func (s *Store) getTarget() {
	s.targ.Lock(); defer s.targ.Unlock()
	if s.targ.Damage!=nil {
		s.targ.Cw.W.(io.Closer).Close()
		s.targ.Cw.W = nil
	}
	if s.targ.Cw.W == nil {
		s.targ.Cw = framelog.CountWriter{}
		fd,ok := s.allocTarget()
		if !ok { return }
		for {
			fl,err := s.data.Open(fd)
			if err!=nil { break } /* Not found! */
			fl.Close()
			fd,ok = s.allocTarget()
			if !ok { return }
		}
		fw,err := s.data.Create(fd)
		if err!=nil {
			s.targ.Damage = err
			return
		}
		s.targ.FD = fd
		s.targ.Cw.W = fw
		s.targ.Damage = nil
	}
}
func (s *Store) addLog(k,v []byte, expireAt uint64) (int64,int64,error) {
	s.getTarget()
	s.targ.Lock(); defer s.targ.Unlock()
	if s.targ.Cw.W == nil {
		err := s.targ.Damage
		if err==nil { err = errors.ErrNotFound }
		return 0,0,err
	}
	if s.targ.Damage !=nil {
		return 0,0,s.targ.Damage
	}
	pos := s.targ.Insert(k,v,expireAt)
	if s.targ.Damage !=nil {
		return 0,0,s.targ.Damage
	}
	return s.targ.FD.Num,pos,nil
}
func (s *Store) Insert(k,v []byte, expireAt uint64) error {
	ok,_ := s.db.Has(k,nil)
	if ok { return EExist }
	
	fp,pos,err := s.addLog(k,v,expireAt)
	if err!=nil { return err }
	buf := new(bytes.Buffer)
	binary.Write(buf,binary.BigEndian,value{fp,pos,expireAt})
	err = s.db.Put(k,buf.Bytes(),nil)
	return err
}


type reader struct{
	framelog.Reader
}
func (r *reader) Release() {
	if c,ok := r.R.(io.Closer); ok {
		c.Close()
	}
}

func (s *Store) Get(k []byte) ([]byte,error) {
	var vl value
	v,err := s.db.Get(k,nil)
	if err!=nil { return nil,err }
	err = vl.decode(v)
	if err!=nil { return nil,err }
	
	ch := s.instcache.Get(storage.FileDesc{storage.TypeTable,vl.Fd},s.open)
	defer ch.release()
	rdr := ch.value.(*reader)
	ent,err := rdr.ReadEntry(vl.Pos)
	if err!=nil { return nil,err }
	vb := make([]byte,ent.Vlen)
	n,err := rdr.R.ReadAt(vb,ent.Vpos)
	if n!=len(vb) { return nil,err }
	return vb,nil
}

