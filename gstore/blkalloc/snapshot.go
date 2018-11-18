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

import "container/list"

type snapshot struct{
	refs uint
	blocks []uint64
}

type SnapshotAlloc struct{
	*CachedAlloc
	snapshots *list.List
	marked    map[uint64]bool
	fusable   func(u uint64)bool
}
func (r *SnapshotAlloc) Init3() {
	r.snapshots = list.New()
	r.marked    = make(map[uint64]bool)
	marked := r.marked
	r.fusable = func (u uint64)bool { return !marked[u] }
	r.snapshots.PushBack(new(snapshot))
}
func (r *SnapshotAlloc) Allocate() (uint64,bool,error) {
	return r.alloc_0(true,r.fusable)
}
func (r *SnapshotAlloc) AddBlock(u uint64) error {
	if err := r.CachedAlloc.AddBlock(u) ; err!=nil { return err }
	sn := r.snapshots.Back().Value.(*snapshot)
	sn.blocks = append(sn.blocks,u)
	r.marked[u] = true
	return nil
}
func (r *SnapshotAlloc) reduce() {
	b := r.snapshots.Back()
	for {
		f := r.snapshots.Front()
		if f==b { return }
		sn := f.Value.(*snapshot)
		if sn.refs>0 { return }
		for _,u := range sn.blocks { delete(r.marked,u) }
		r.snapshots.Remove(f)
	}
}
// Release a snapshot.
func (r *SnapshotAlloc) Release(e *list.Element) {
	e.Value.(*snapshot).refs--
	r.reduce()
}
// Obtain a snapshot. Don't inspect the list!
func (r *SnapshotAlloc) Obtain() *list.Element {
	e := r.snapshots.Back()
	e.Value.(*snapshot).refs++
	return e
}
func (r *SnapshotAlloc) CommitSn() error {
	if err := r.A.Flush() ; err!=nil { return err }
	r.snapshots.PushBack(new(snapshot))
	r.reduce()
	return nil
}

