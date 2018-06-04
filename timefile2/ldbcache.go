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

import "github.com/syndtr/goleveldb/leveldb/cache"

type lCache struct{
	*cache.Cache
	vG func(key uint64) cache.Value
}
func (l *lCache) init(n int, vG func(key uint64) cache.Value){
	l.Cache = cache.NewCache(cache.NewLRU(n))
	l.vG = vG
}
func (l *lCache) get(key uint64) *cache.Handle {
	return l.Cache.Get(1,key,func()(int,cache.Value) {
		return 1,l.vG(key)
	})
}
func (l *lCache) purge() {
	l.EvictAll()
}
func (l *lCache) remove(key uint64) {
	l.Cache.Evict(1,key)
}

