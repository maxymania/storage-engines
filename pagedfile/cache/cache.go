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



package pagecache

import . "github.com/maxymania/storage-engines/pagedfile"
import "github.com/syndtr/goleveldb/leveldb/cache"
import "sync"
import "sync/atomic"

type cacher struct {
	cc *cache.Cache
	ns uint64
}

func (c cacher) Get(i int64) (outer,inner Releaser) {
	h := c.cc.Get(c.ns,uint64(i),nil)
	if h==nil { return }
	outer = h
	inner,_ = h.Value().(Releaser)
	return
}
func (c cacher) Put(i int64,size int,supp Releaser) (outer,inner Releaser,used bool) {
	h := c.cc.Get(c.ns,uint64(i),func()(int,cache.Value){
		used = true
		return size,supp
	})
	if h==nil { return }
	outer = h
	inner,_ = h.Value().(Releaser)
	return
}
func (c cacher) Invalidate(i int64) {
	c.cc.Delete(c.ns,uint64(i),nil)
}
func (c cacher) Clear() {
	c.cc.EvictNS(c.ns)
}
var _ PageCache = cacher{}

var globalCache *cache.Cache
var globalCacheInit sync.Once
var globalCacheCtr uint64

func initGc() {
	// Default Cache size 128 MB
	globalCache = cache.NewCache(cache.NewLRU(1<<27))
}

func SetCapacity(capacity int) {
	globalCacheInit.Do(initGc)
	globalCache.SetCapacity(capacity)
}
func Capacity() int {
	globalCacheInit.Do(initGc)
	return globalCache.Capacity()
}

func cacherNew(capacity int) (PageCache,error) {
	chc := cache.NewCache(cache.NewLRU(capacity))
	return cacher{chc,1},nil
}

func createCache() (PageCache,error) {
	ns := atomic.AddUint64(&globalCacheCtr,1)
	globalCacheInit.Do(initGc)
	return cacher{globalCache,ns},nil
}

func init() {
	PageCacheNew = createCache
}
