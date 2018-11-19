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

import "github.com/maxymania/storage-engines/gstore/blockmgr"
import "container/list"

type txnref struct{
	refc int64
	defs []uint64
}
func (t *txnref) inc() {
	t.refc++
}
func (t *txnref) dec() {
	t.refc--
}

/*
Snapshot-Cookie
*/
type Snapshot struct{
	elem *list.Element
}

type Allocator struct{
	alloclist
	txs *list.List
}
func (a *Allocator) Init(b blockmgr.BlockManager,blocklog uint,b1,b2 uint64) {
	// Basic stuff
	a.bmr = b
	a.blksize = 1<<blocklog
	a.ba = b1
	a.bf = b2
	
	// Additional stuff
	a.exclude = make(blkSet)
	
	a.txs = list.New()
	a.txs.PushBack(new(txnref))
}
func (a *Allocator) SetHead(b1,b2 uint64) {
	a.ba = b1
	a.bf = b2
	a.free = a.free[:0]
	a.dump = a.dump[:0]
}
func (a *Allocator) Head() (b1,b2 uint64) {
	b1 = a.ba
	b2 = a.bf
	return
}
func (a *Allocator) Alloc() (uint64,error) { return a.alloc() }
func (a *Allocator) Free(u uint64) {
	a.putblock(u)
}
func (a *Allocator) Snapshot() Snapshot {
	e := a.txs.Back()
	e.Value.(*txnref).inc()
	return Snapshot{e}
}
func (a *Allocator) ReleaseSnapshot(sn Snapshot) {
	sn.elem.Value.(*txnref).dec()
	a.reduce()
}
func (a *Allocator) Flush() error {
	a.txs.PushBack(new(txnref))
	a.reduce()
	e1 := a.writeback()
	e2 := a.writefrees()
	if e1==nil { e1=e2 }
	return e1
}
func (a *Allocator) reduce() {
	b := a.txs.Back()
	for {
		f := a.txs.Front()
		if b==f { return }
		if f.Value.(*txnref).refc>0 { return }
		for _,val := range f.Value.(*txnref).defs { delete(a.exclude,val) }
		a.txs.Remove(f)
	}
}

