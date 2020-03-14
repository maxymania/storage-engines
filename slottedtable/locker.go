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

import "sync"

var noop func() = func(){}

var locknommap = func() sync.Locker { return new(sync.Mutex) }
var lockmmap = func() sync.Locker { return new(sync.RWMutex) }

type rLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

type wrapMap struct{
	inner sync.Map
	noread bool
}
func (m *wrapMap) get(key interface{}) interface{} {
	if val,ok := m.inner.Load(key); ok { return val }
	var nv sync.Locker
	if m.noread {
		nv = new(sync.Mutex)
	} else {
		nv = new(sync.RWMutex)
	}
	val,_ := m.inner.LoadOrStore(key,nv)
	return val
}

func (m *wrapMap) WLock(bid int64) func() {
	l := m.get(bid).(sync.Locker)
	l.Lock()
	return l.Unlock
}
func (m *wrapMap) RLock(bid int64) func() {
	if m.noread { return noop }
	
	l := m.get(bid).(rLocker)
	l.RLock()
	return l.RUnlock
}

func NewMmapLocker() Locker { return &wrapMap{} }
func NewNoMmapLocker() Locker { return &wrapMap{noread:true} }

