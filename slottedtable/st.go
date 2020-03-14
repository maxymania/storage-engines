/*
Copyright (c) 2020 Simon Schmidt

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


/*
Implements a slotted table storage engine design.

MMAP-support can be added by importing the MMAP-plugin package.
Note that this plugin is not supported on certain platforms like plan9.

	import _ "github.com/maxymania/storage-engines/pagedfile/mmap"
*/
package slottedtable

import "io"
import "os"
import "bytes"
import "github.com/maxymania/storage-engines/pagedfile"
import "github.com/vmihailenco/msgpack"
import "errors"
import "sync"
import "github.com/maxymania/storage-engines/buffer"

var ErrOverflow = errors.New("Overflow: record consumes too much space in page")

// A Tuple-ID identifies a record's physical location within the table file.
// It is represented as '(block-id,record-id)', where the 'block-id' is the
// location of the block within the table file, and 'record-id' is the
// block-local record number.
type TID [2]int64

// A Block-range identifies a range of blocks.
type BRange [2]int64

type Record struct {
	blob []byte
	page *pagedfile.Page
	tid  TID
}
func (r *Record) Bytes() []byte { return r.blob }
func (r *Record) Release() {
	if r.page!=nil {
		r.page.Release()
	}
}
func (r *Record) TID() TID { return r.tid }

/*
Is used for iterating over records. Returns false to stop the iteration.
*/
type IterFunc func(tid TID,rec []byte) bool

/*
Is used to filter records. Returns false, if a record should be dropped.
*/
type FilterFunc func(tid TID,rec []byte) bool

/*
Warning: This interface is not meant for direct use.
Expect breaking changes.
*/
type Locker interface{
	WLock(bid int64) func()
	RLock(bid int64) func()
}

type SlottedTable struct {
	Store  *pagedfile.PagedFile
	Locker Locker
	
	mu sync.RWMutex
}

func NewSlottedTable(pf *pagedfile.PagedFile) (st *SlottedTable,err error) {
	st = new(SlottedTable)
	st.Store = pf
	if pf.HasMMAP() {
		st.Locker = NewMmapLocker()
	} else {
		st.Locker = NewNoMmapLocker()
	}
	return
}

const (
	// Memory-Map the file, if possible.
	// It also that it is (potentially) shared
	// across concurrent readers and MUST not be
	// modified by them.
	//
	// In order for F_MMAP to have any effect you need to
	// import _ "github.com/maxymania/storage-engines/pagedfile/mmap"
	F_MMAP = pagedfile.F_MMAP
)

func NewSlottedTableFile(f *os.File,psz int, flags uint) (st *SlottedTable,err error) {
	var pf *pagedfile.PagedFile
	pf,err = pagedfile.NewPagedFile(f,psz,pagedfile.F_MEASURE|flags)
	if err!=nil { return }
	st = new(SlottedTable)
	st.Store = pf
	if pf.HasMMAP() {
		st.Locker = NewMmapLocker()
	} else {
		st.Locker = NewNoMmapLocker()
	}
	return
}

func (st *SlottedTable) writer() func() {
	st.mu.RLock()
	return st.mu.RUnlock
}
func (st *SlottedTable) Read(tid TID) (*Record,error) {
	defer st.Locker.RLock(tid[0])()
	page,err := st.Store.Read(tid[0])
	if err!=nil { return nil,err }
	dec := msgpack.NewDecoder(newReader(page.Bytes()))
	
	for {
		i,blob,err := readRecord(dec)
		if err!=nil { page.Release(); return nil,err }
		if i<tid[1] { continue }
		if i>tid[1] { page.Release(); return nil,io.EOF }
		return &Record{blob,page,tid},nil
	}
}
func (st *SlottedTable) iterBlock(bid int64,iter IterFunc) (error,bool) {
	defer st.Locker.RLock(bid)()
	page,err := st.Store.Read(bid)
	if err!=nil { return err,true }
	defer page.Release()
	dec := msgpack.NewDecoder(newReader(page.Bytes()))
	
	for {
		i,blob,err := readRecord(dec)
		if err==io.EOF { return nil,true }
		if err!=nil { return err,true }
		if !iter(TID{bid,i},blob) { return nil,false }
	}
}
func (st *SlottedTable) IterBlock(bid int64,iter IterFunc) (err error) {
	err,_ = st.iterBlock(bid,iter)
	return
}
func (st *SlottedTable) Iterate(iter IterFunc) (err error) {
	more := true
	for bid := int64(0); more && bid<st.Store.NBlocks; bid++ {
		err,more = st.iterBlock(bid,iter)
	}
	return
}

func (st *SlottedTable) InsertInBlock(bid int64,rec []byte) (tid TID,err error) {
	defer st.writer()()
	defer st.Locker.WLock(bid)()
	var page *pagedfile.Page
	var siz,lastid int64
	
	tid[0] = bid
	
	page,err = st.Store.Read(bid)
	if err!=nil { return }
	
	siz,lastid,err = measure(page.Bytes())
	if err!=nil { page.Release(); return }
	
	tid[1] = lastid+1
	
	raw := buffer.CGet(st.Store.Pagesize)
	defer buffer.Put(raw)
	buf := bytes.NewBuffer((*raw)[:st.Store.Pagesize])
	buf.Reset()
	buf.Write(page.Bytes()[:siz])
	page.Release()
	enc := msgpack.NewEncoder(buf)
	enc.EncodeInt64(tid[1])
	enc.EncodeBytes(rec)
	enc.EncodeUint64(hashBuf(rec))
	
	if buf.Len()>st.Store.Pagesize {
		err = ErrOverflow
		return
	}
	// The rest of the buffer is zeroed out anyways. So there is
	// no need to add padding.
	
	err = st.Store.Write((*raw)[:st.Store.Pagesize],bid)
	return
}
func (st *SlottedTable) Insert(r *BRange, rec []byte) (tid TID,err error) {
	var begin,end int64
	if r!=nil {
		begin = r[0]
		end = r[1]
	}
	if end==0 || end>st.Store.NBlocks { end = st.Store.NBlocks }
	err=ErrOverflow
	for i := begin; i<end; i++ {
		tid,err = st.InsertInBlock(i,rec)
		if err==ErrOverflow { continue }
		return
	}
	return
}

func (st *SlottedTable) FilterBlock(bid int64,filt FilterFunc) (err error) {
	defer st.writer()()
	defer st.Locker.WLock(bid)()
	var page *pagedfile.Page
	var tid TID
	
	tid[0] = bid
	
	page,err = st.Store.Read(bid)
	if err!=nil { return }
	defer page.Release()
	
	raw := buffer.CGet(st.Store.Pagesize)
	defer buffer.Put(raw)
	buf := bytes.NewBuffer((*raw)[:st.Store.Pagesize])
	buf.Reset()
	
	dec := msgpack.NewDecoder(newReader(page.Bytes()))
	enc := msgpack.NewEncoder(buf)
	
	for {
		id2,rec,err2 := readRecord(dec)
		if err2==ErrHashError { continue } // If corrupted, drop the record.
		if err2!=nil { break }
		tid[1] = id2
		if !filt(tid,rec) { continue } // If filter returns false, drop the record.
		
		enc.EncodeInt64(id2)
		enc.EncodeBytes(rec)
		enc.EncodeUint64(hashBuf(rec))
	}
	
	// The rest of the buffer is zeroed out anyways. So there is
	// no need to add padding.
	
	err = st.Store.Write((*raw)[:st.Store.Pagesize],bid)
	return
}

// Msyncs the changes to the filesystem, if the table is MMAPed.
// If the table is not MMAPed, this method has no effect.
func (st *SlottedTable) Msync() {
	// Fast Path.
	if st.Store.HasMMAP() { return }
	
	st.mu.Lock()
	defer st.mu.Unlock()
	st.Store.Msync()
}

