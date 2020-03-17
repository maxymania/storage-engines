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


package slottedtable

type TIDSet struct {
	m1 map[int64]bool
	m2 map[TID]bool
}
func (t *TIDSet) Init() *TIDSet {
	if t.m1==nil { t.m1 = make(map[int64]bool) }
	if t.m2==nil { t.m2 = make(map[TID]bool) }
	return t
}
func (t *TIDSet) Clear() *TIDSet {
	t.Init()
	for k := range t.m1 { delete(t.m1,k) }
	for k := range t.m2 { delete(t.m2,k) }
	return t
}
func (t *TIDSet) Add(id TID) {
	t.m1[id[0]] = true
	t.m2[id] = true
}
func (t *TIDSet) AddAll(ids []TID) {
	for _,id := range ids {
		t.m1[id[0]] = true
		t.m2[id] = true
	}
}
func (t *TIDSet) filter(tid TID, rec []byte) bool {
	return !(t.m2[tid])
}


func (st *SlottedTable) DeleteAll(t *TIDSet) (err error) {
	for k := range t.m1 {
		err2 := st.FilterBlock(k,t.filter)
		if err==nil { err = err2 }
	}
	return
}

