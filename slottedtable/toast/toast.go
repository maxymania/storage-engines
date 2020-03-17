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
Implements a storage method for oversized records. It is inspired by PostgreSQL's
TOAST-table system.
*/
package toast

import slt "github.com/maxymania/storage-engines/slottedtable"
import "sync/atomic"
import "io"
import "fmt"

func debug(i ...interface{}) {
	fmt.Println(i...)
}

func updateMinStat(m *int64,i int64) {
	n := *m
	if n<0||n>i { *m = i }
}

func getPos(i *int64) int64 {
	return atomic.LoadInt64(i)
}
func increasePos(i *int64,n int64) {
	for {
		d := atomic.LoadInt64(i)
		if d>=n { return }
		if atomic.CompareAndSwapInt64(i,d,n) { return }
	}
}
func decreasePos(i *int64,n int64) {
	for {
		d := atomic.LoadInt64(i)
		if d<=n { return }
		if atomic.CompareAndSwapInt64(i,d,n) { return }
	}
}

/*
9 Byte for int64 and uint64 (tuple-id and hash)
2-5 byte for bytes-length
*/
const overhead = 9+9+5

type Toaster struct {
	poses [4]int64
	inner *slt.SlottedTable
	maxsz int
}
func Make(inner *slt.SlottedTable) (*Toaster,error) {
	_,err := inner.Store.EnsureSize(8)
	if err!=nil { return nil,err }
	t := new(Toaster)
	t.inner = inner
	t.maxsz = inner.Store.Pagesize-overhead
	return t,nil
}
func (t *Toaster) insert(r *slt.BRange,bya []byte) (slt.TID,error) {
	var posptr *int64
	posptr = &t.poses[3]
	if len(bya)<=128 {
		posptr = &t.poses[0]
	} else if len(bya)<=512 {
		posptr = &t.poses[1]
	} else if len(bya)<=(t.maxsz/2) {
		posptr = &t.poses[2]
	}
	return t.insert2(r,posptr,bya)
}
func (t *Toaster) insert2(r *slt.BRange,posptr *int64, bya []byte) (slt.TID,error) {
	for {
		d := getPos(posptr)
		*r = slt.BRange{d,0}
		tid,err := t.inner.Insert(r,bya)
		if err==slt.ErrOverflow {
			_,err = t.inner.Store.Grow()
			if err!=nil { return slt.TID{},err }
			continue
		} else if err==nil {
			increasePos(posptr,tid[0])
		}
		return tid,err
	}
}

func (t *Toaster) Write(bya []byte) (tid slt.TID,level int,err error) {
	return t.write(new(slt.BRange),bya)
}
func (t *Toaster) write(r *slt.BRange, bya []byte) (tid slt.TID,level int,err error) {
	if len(bya)==0 {
		tid,err = t.insert(r,nil)
		return
	}
	var ctid slt.TID
	tids := make([]slt.TID,0,128)
	for{
		var part []byte
		if len(bya)>t.maxsz {
			part,bya = bya[:t.maxsz],bya[t.maxsz:]
		} else {
			part,bya = bya,nil
		}
		
		ctid,err = t.insert(r,part)
		if err!=nil { return }
		tids = append(tids,ctid)
		if len(bya)>0 { continue }
		switch len(tids) {
		case 0: panic("invalid state")
		case 1:
			tid = tids[0]
			return
		}
		bya = encodeList(tids)
		tids = tids[:0]
		level++
	}
}

func (t *Toaster) Read(tid slt.TID,level int,target io.Writer) (err error) {
	return t.read(tid,level,target)
}
func (t *Toaster) read(tid slt.TID,level int,target io.Writer) (err error) {
	var rec *slt.Record
	rec,err = t.inner.Read(tid)
	if err!=nil { return }
	defer rec.Release()
	if level<1 {
		_,err = target.Write(rec.Bytes())
		return
	}
	tids := decodeList(rec.Bytes())
	for _,ctid := range tids {
		err = t.read(ctid,level-1,target)
		if err!=nil { break }
	}
	return
}

func (t *Toaster) del(set *slt.TIDSet, s *int64, tid slt.TID, level int) (err error) {
	if level<1 { return }
	
	var rec *slt.Record
	rec,err = t.inner.Read(tid)
	if err!=nil { return }
	tids := decodeList(rec.Bytes())
	rec.Release()
	for _,ctid := range tids {
		err2 := t.del(set,s,ctid,level-1)
		if err2!=nil { err = err2 }
		updateMinStat(s,ctid[0])
	}
	set.Clear().AddAll(tids)
	err2 := t.inner.DeleteAll(set)
	if err2!=nil { err = err2 }
	return
}
func (t *Toaster) del2(set *slt.TIDSet, s *int64, tid slt.TID, level int) (err error) {
	err = t.del(set,s,tid,level)
	set.Clear().Add(tid)
	err2 := t.inner.DeleteAll(set)
	updateMinStat(s,tid[0])
	if err2!=nil { err = err2 }
	return
}
func (t *Toaster) Delete(tid slt.TID, level int) (err error) {
	s := new(int64)
	*s = -1
	err = t.del2(new(slt.TIDSet),s,tid,level)
	if *s<0 { *s = 0 }
	for i := range t.poses {
		decreasePos(&t.poses[i],*s)
	}
	return
}

