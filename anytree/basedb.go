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

package anytree

import "github.com/cznic/file"
import "encoding/binary"
import "errors"

var (
	ErrRDONLY = errors.New("RDONLY")
	ErrUndersized = errors.New("Undersized Target")
	ErrINVAL = errors.New("ErrINVAL")
	ErrInternal = errors.New("ErrInternal")
)

type TxType uint
const (
	TxWrite TxType = iota
	TxReadComitted
	TxReadUncomitted
)

type DB struct{
	f,w file.File
	
	wal *file.WAL
	a *file.Allocator
}
func NewDB(f,w file.File) *DB {
	return &DB{f:f,w:w}
}/*
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
func (db *DB) getWAL() (f file.File,err error) {
	if db.wal!=nil { return db.wal,nil }
	if db.w==nil { return db.f,nil }
	db.wal,err = file.NewWAL(db.f,db.w,0,12)
	if err!=nil { db.wal=nil; return }
	return db.wal,nil
}
func (db *DB) getAlloc() (a *file.Allocator,err error) {
	var f file.File
	if db.a!=nil { return db.a,nil }
	f,err = db.getWAL()
	if err!=nil { return }
	a,err = file.NewAllocator(f)
	if err==nil { db.a = a }
	return
}
func (db *DB) Begin(t TxType) (*Tx,error) {
	var err error
	tx := new(Tx)
	switch t {
	case TxWrite:
		tx.a,err = db.getAlloc()
		if err!=nil { return nil,err }
		tx.parent = db
		fallthrough
	case TxReadUncomitted:
		tx.f,err = db.getWAL()
	case TxReadComitted:
		tx.f = db.f
		err = nil
	}
	if err!=nil { return nil,err }
	return tx,nil
}

type Tx struct{
	f file.File
	a *file.Allocator
	parent *DB
}
func (t *Tx) GetRoot() (off int64,e error) {
	var rob [8]byte
	_,e = t.f.ReadAt(rob[:],8)
	off = int64(binary.BigEndian.Uint64(rob[:]))
	return
}
func (t *Tx) SetRoot(off int64) (e error) {
	if t.a==nil { return ErrRDONLY }
	var rob [8]byte
	binary.BigEndian.PutUint64(rob[:],uint64(off))
	_,e = t.f.WriteAt(rob[:],8)
	return
}
func (t *Tx) Read(off int64) (b Buffer,e error){
	var lng [4]byte
	_,e = t.f.ReadAt(lng[:],off)
	if e!=nil { return }
	b = AllocBuffer(int(binary.BigEndian.Uint32(lng[:])))
	_,e = t.f.ReadAt(b.Ptr,off+4)
	return
}
func (t *Tx) Update(off int64,b []byte) error {
	var lng [4]byte
	if t.a==nil { return ErrRDONLY }
	binary.BigEndian.PutUint32(lng[:],uint32(len(b)))
	l := int64(len(b)+4)
	i,err := t.a.UsableSize(off)
	if err!=nil { return err }
	if i<l { return ErrUndersized }
	_,err = t.f.WriteAt(lng[:],off)
	if err!=nil { return err }
	_,err = t.f.WriteAt(b,off+4)
	return err
}
func (t *Tx) Insert(b []byte) (int64,error) {
	var lng [4]byte
	if t.a==nil { return 0,ErrRDONLY }
	binary.BigEndian.PutUint32(lng[:],uint32(len(b)))
	l := int64(len(b)+4)
	off,err := t.a.Alloc(l)
	if err!=nil { return off,err }
	_,err = t.f.WriteAt(lng[:],off)
	if err!=nil { return off,err }
	_,err = t.f.WriteAt(b,off+4)
	return off,err
}
func (t *Tx) Delete(off int64) error {
	if t.a==nil { return ErrRDONLY }
	return t.a.Free(off)
}
func (t *Tx) Commit() error {
	if t.parent==nil { return nil }
	t.a.Close()
	t.parent.a = nil
	err := error(nil)
	if t.parent.wal!=nil {
		err = t.parent.wal.Commit()
	}
	t.parent = nil
	return err
}
func (t *Tx) Rollback() error {
	if t.parent==nil { return nil }
	t.parent.a = nil
	t.parent.wal = nil
	err := error(nil)
	if t.parent.w != nil {
		err = t.parent.w.Truncate(0) /* Erase the write-ahead-log with the uncomitted data. */
	}
	return err
}


