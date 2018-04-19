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
import "sync/atomic"
import "github.com/syndtr/goleveldb/leveldb/util"
import "github.com/hashicorp/golang-lru/simplelru"

type cElement struct{
	refc  int64
	value util.Releaser
}
func (c *cElement) release(){
	if atomic.AddInt64(&(c.refc),-1)<1 { return }
	c.value.Release()
}
func cElementEvict(k, v interface{}) {
	v.(*cElement).release()
}

type cCache struct{
	sync.Mutex
	lru *simplelru.LRU
}
func (c *cCache) init(n int){
	c.lru,_ = simplelru.NewLRU(n,cElementEvict)
}
func (c *cCache) Get(k interface{}, vG func(interface{}) util.Releaser ) *cElement {
	c.Lock(); defer c.Unlock()
	v,ok := c.lru.Get(k)
	if ok {
		vr := v.(*cElement)
		atomic.AddInt64(&(vr.refc),1)
		return vr
	}
	ve := vG(k)
	if ve==nil { return nil }
	elem := &cElement{value:ve}
	atomic.StoreInt64(&(elem.refc),1)
	c.lru.Add(k,elem)
	return elem
}
func (c *cCache) Purge() {
	c.Lock(); defer c.Unlock()
	c.lru.Purge()
}
func (c *cCache) Remove(key interface{}) {
	c.Lock(); defer c.Unlock()
	c.lru.Remove(key)
}

