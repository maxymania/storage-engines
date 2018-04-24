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


package mmdb

import "os"
import "github.com/edsrzf/mmap-go"
import "errors"
import "sync/atomic"
import "encoding/binary"

var EOverChunkSize = errors.New("EOverChunkSize")

var blank [4]byte

const (
	RDONLY = mmap.RDONLY
	RDWR   = mmap.RDWR
)

type Chunk struct {
	Position int64
	Size int64
	Snapshots[8] int64
	Mmap mmap.MMap
	Fobj *os.File
	
}
func (c *Chunk) Snapshot() {
	pos := atomic.LoadInt64(&(c.Position))
	siz := c.Snapshots[0]
	snp := 0
	for i,n := range c.Snapshots {
		if n<siz {
			siz = n
			snp = i
		}
	}
	c.Snapshots[snp] = pos
	binary.BigEndian.PutUint64(c.Mmap[snp*8:],uint64(c.Snapshots[snp]))
	if c.Fobj!=nil { c.Fobj.Sync() }
}
func (c *Chunk) Format() {
	for i := range c.Snapshots {
		c.Snapshots[i] = 0
		binary.BigEndian.PutUint64(c.Mmap[i*8:],0)
	}
	c.Position = 8*8
	if c.Fobj!=nil { c.Fobj.Sync() }
}
func (c *Chunk) Load() {
	c.Position = 0
	for i := range c.Snapshots {
		L := int64(binary.BigEndian.Uint64(c.Mmap[i*8:]))
		c.Snapshots[i] = L
		if c.Position < L { c.Position = L }
	}
}
func (c *Chunk) GetCommitted() (mem []byte,start int64) {
	mem = c.Mmap[:c.Position]
	start = 8*8 // Exclude the snapshot map.
	mem = mem[start:]
	return
}
func (c *Chunk) SetLast(p int64) {
	mod := false
	if c.Size<p { return }
	for i,n := range c.Snapshots {
		if n<=p { continue }
		c.Snapshots[i] = p
		binary.BigEndian.PutUint64(c.Mmap[i*8:],uint64(p))
		mod = true
	}
	if pos := atomic.LoadInt64(&(c.Position)); pos > p {
		atomic.StoreInt64(&(c.Position),p)
	}
	if mod && c.Fobj!=nil { c.Fobj.Sync() }
}

func (c *Chunk) Append(buf []byte) (record []byte,pos int64, err error) {
	l := int64(len(buf))
	pos = atomic.AddInt64(&(c.Position),l)
	if pos > c.Size {
		atomic.AddInt64(&(c.Position),-l) // unapply it!
		pos = 0
		err = EOverChunkSize
		return
	}
	pos -= l
	record = c.Mmap[pos:][:l]
	copy(record,buf)
	return
}
func (c *Chunk) Skip(i int64) (slice []byte,pos int64,err error){
	pos = atomic.AddInt64(&(c.Position),i)
	if pos > c.Size {
		atomic.AddInt64(&(c.Position),-i) // unapply it!
		pos = 0
		err = EOverChunkSize
		return
	}
	pos -= i
	slice = c.Mmap[pos:pos+i]
	return
}

func (c *Chunk) Map(f *os.File, prot, flags int) (err error) {
	c.Mmap,err = mmap.Map(f,prot,flags)
	c.Size = int64(len(c.Mmap))
	c.Fobj = f
	return
}
func (c *Chunk) MapRegion(f *os.File, length int, prot, flags int, offset int64) (err error) {
	c.Mmap,err = mmap.MapRegion(f,length,prot,flags,offset)
	c.Size = int64(len(c.Mmap))
	c.Fobj = f
	return
}
func (c *Chunk) Close() {
	c.Mmap.Unmap()
}
func (c *Chunk) GetAhead() []byte {
	return c.Mmap[atomic.LoadInt64(&(c.Position)):]
}
func (c *Chunk) At(pos,lng int64) ([]byte,error) {
	if c.Size < pos+lng { return nil,EOverChunkSize }
	return c.Mmap[pos:pos+lng],nil
}


