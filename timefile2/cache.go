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

import rbt "github.com/emirpasic/gods/trees/redblacktree"
import "github.com/emirpasic/gods/utils"
import "sync"

/* Deleted body! */

func u64grow(r []uint64) []uint64 {
	c := cap(r)
	if c>len(r) { return r }
	if c<16 { c = 16 }
	e := make([]uint64,len(r),c<<1)
	copy(e,r)
	return e
}

type kArray struct{
	t *rbt.Tree
	m sync.Mutex
}
func (a *kArray) init() {
	a.t = rbt.NewWith(utils.UInt64Comparator)
}
func (a *kArray) insert(k uint64,v interface{}) {
	a.m.Lock(); defer a.m.Unlock()
	a.t.Put(k,v)
}
func (a *kArray) remove(k uint64,v interface{}) {
	a.m.Lock(); defer a.m.Unlock()
	if nv,_ := a.t.Get(k); nv==v {
		a.t.Remove(k)
	}
}
func (a *kArray) until(u uint64, maxsz int) (r []uint64) {
	a.m.Lock(); defer a.m.Unlock()
	i := a.t.Iterator()
	i.Begin()
	for len(r)<maxsz || maxsz<=0 {
		if !i.Next() { break }
		E := i.Key().(uint64)
		if E>u { break }
		r = append(u64grow(r),E)
	}
	return
}

