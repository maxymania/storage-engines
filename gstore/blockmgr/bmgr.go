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


package blockmgr

import "github.com/cznic/file"
import slab "github.com/steveyen/go-slab"
import "sync"

type cache struct{
	sync.Mutex
	f map[uint64][]byte
	r map[*byte]uint64
}
func (c *cache) init() {
	c.f = make(map[uint64][]byte)
	c.r = make(map[*byte]uint64)
}
func (c *cache) get(s *slab.Arena,i uint64) ([]byte,bool) {
	c.Lock(); defer c.Unlock()
	b,ok := c.f[i]
	if ok { s.AddRef(b) }
	return b,ok
}
func (c *cache) update(s *slab.Arena,i uint64,b []byte) []byte {
	c.Lock(); defer c.Unlock()
	if ob,ok := c.f[i]; ok { s.DecRef(b); return ob }
	p := &b[0]
	c.f[i] = b
	c.r[p] = i
	return b
}
func (c *cache) free(s *slab.Arena,b []byte) {
	c.Lock(); defer c.Unlock()
	p := &b[0]
	if s.DecRef(b) {
		i,ok := c.r[p]
		if !ok { return }
		delete(c.r,p)
		delete(c.f,i)
	}
}

type BlockFile struct{
	blocklog uint
	nblocks uint64
	file  file.File
	alloc *slab.Arena
	cache cache
	grow sync.Mutex
}

func NewBlockFile(f file.File,a *slab.Arena, blocklog uint) (*BlockFile,error) {
	s,err := f.Stat()
	if err!=nil { return nil,err }
	r := new(BlockFile)
	r.blocklog = blocklog
	r.nblocks = uint64(s.Size())>>blocklog
	r.file = f
	r.alloc = a
	r.cache.init()
	return r,nil
}
func (r *BlockFile) Length() uint64 {
	return r.nblocks
}
func (r *BlockFile) ReadBlock(i uint64) ([]byte,error) {
	if i>=r.nblocks { return nil,ErrOutOfBounds }
	if b,ok := r.cache.get(r.alloc,i); ok { return b,nil }
	
	buf := r.alloc.Alloc(1<<r.blocklog)
	_,err := r.file.ReadAt(buf,int64(i<<r.blocklog))
	if err!=nil {
		r.alloc.DecRef(buf)
		return nil,err
	}
	buf = r.cache.update(r.alloc,i,buf)
	return buf,nil
}
func (r *BlockFile) DiscardRead(buf []byte) {
	r.cache.free(r.alloc,buf)
}
func (r *BlockFile) Allocate(i uint64) ([]byte,error) {
	if i>=r.nblocks { return nil,ErrOutOfBounds }
	// XXX: we need to lock on the allocator, because it isn't thread-safe.
	r.cache.Lock(); defer r.cache.Unlock()
	buf := r.alloc.Alloc(1<<r.blocklog)
	return buf,nil
}
func (r *BlockFile) WriteBack(i uint64,buf []byte) error {
	if i>=r.nblocks { return ErrOutOfBounds }
	defer r.alloc.DecRef(buf)
	_,err := r.file.WriteAt(buf,int64(i<<r.blocklog))
	return err
}
func (r *BlockFile) DiscardWrite(buf []byte) {
	// XXX: we need to lock on the allocator, because it isn't thread-safe.
	r.cache.Lock(); defer r.cache.Unlock()
	r.alloc.DecRef(buf)
}
func (r *BlockFile) GrowStep() (offset uint64,lng int,e error) {
	r.grow.Lock(); defer r.grow.Unlock()
	offset = r.nblocks
	lng = mb128>>r.blocklog
	if lng==0 { return }
	nbl2 := r.nblocks + uint64(lng)
	e = r.file.Truncate(int64(nbl2<<r.blocklog))
	if e!=nil { return }
	r.nblocks = nbl2
	return
}
func (r *BlockFile) Sync() error {
	return r.file.Sync()
}
func (r *BlockFile) Close() error {
	return r.file.Close()
}

var _ BlockManager = (*BlockFile)(nil)

