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


/*
Implements IO-Primitives for page-based tables.

MMAP-support is not builtin by default. Instead, if MMAP is desired,
a plugin-package must be imported. Note that this plugin is not supported
on certain platforms like plan9.

	import _ "github.com/maxymania/storage-engines/pagedfile/mmap"
*/
package pagedfile

import (
	//"github.com/vmihailenco/msgpack"
	//"bytes"
	"github.com/maxymania/storage-engines/buffer"
	"os"
	//"io"
	"errors"
)

const (
	// Measure the size at startup.
	F_MEASURE = 1<<iota
	
	// Memory-Map the file, if possible.
	// This means, that the byte-slice returned by
	// (*Page).Bytes() will change without notice,
	// when a page is updated/overwritten.
	// It also means that it is (potentially) shared
	// across concurrent readers and MUST not be
	// modified by them.
	//
	// In order for F_MMAP to have any effect you need to
	// import _ "github.com/maxymania/storage-engines/pagedfile/mmap"
	F_MMAP
)

var (
	ErrBlockTooShort = errors.New("Block too short")
)

func has(flags, flag uint) bool {
	return (flags&flag)!=0
}

/*
Don't use this interface. Expect changes, that will break your stuff.
*/
type MmapLoader interface{
	Read(offset int64,size int) (bool,[]byte)
	Write(buf []byte,offset int64) bool
	NotifySize(size int64)
	Sync()
	Close()
}
var MmapNew func(f *os.File) (MmapLoader,error)

type Page struct{
	alloc *[]byte
	buf []byte
}
func (p *Page) Release() {
	p.buf = nil
	if p.alloc!=nil { buffer.Put(p.alloc) }
	p.alloc = nil
}
func (p *Page) Bytes() []byte {
	return p.buf
}

type PagedFile struct{
	File     *os.File
	Pagesize int
	NBlocks  int64
	
	mmapLdr  MmapLoader
}
func NewPagedFile(f *os.File, psz int, flags uint) (*PagedFile,error) {
	p := new(PagedFile)
	p.File = f
	p.Pagesize = psz
	if has(flags,F_MEASURE|F_MMAP) {
		siz,err := p.length()
		if err!=nil { return nil,err }
		p.NBlocks = siz/int64(psz)
	}
	if has(flags,F_MMAP) && MmapNew!=nil {
		l,err := MmapNew(f)
		if err!=nil { return nil,err }
		l.NotifySize(p.NBlocks*int64(p.Pagesize))
		p.mmapLdr = l
	}
	
	return p,nil
}

// Returns true, if, and only if this paged file has a mmap-loader.
// If this paged file is not MMAPed, this returns false.
func (f *PagedFile) HasMMAP() bool {
	return f.mmapLdr!=nil
}
func (f *PagedFile) EnsureSize(blocks int64) (int64,error) {
	if f.NBlocks==0 {
		siz,err := f.length()
		if err!=nil { return 0,err }
		f.NBlocks = siz/int64(f.Pagesize)
	}
	if f.NBlocks<blocks {
		err := f.File.Truncate(blocks*int64(f.Pagesize))
		if err!=nil { return 0,err }
		f.NBlocks = blocks
	}
	return f.NBlocks,nil
}
func (f *PagedFile) Grow() (int64,error) {
	const maxstep = 1<<28
	siz,err := f.length()
	if err!=nil { return 0,err }
	siz -= siz%int64(f.Pagesize)
	gro := siz
	if gro>maxstep { gro = maxstep }
	if gro<int64(f.Pagesize) { gro = int64(f.Pagesize) }
	siz += gro
	err = f.File.Truncate(siz)
	if err!=nil { return 0,err }
	f.NBlocks = siz/int64(f.Pagesize)
	if f.mmapLdr!=nil {
		f.mmapLdr.NotifySize(f.NBlocks*int64(f.Pagesize))
	}
	return f.NBlocks,nil
}
func (f *PagedFile) length() (int64,error) {
	s,e := f.File.Stat()
	if e!=nil { return 0,e }
	return s.Size(),nil
}

func (f *PagedFile) Read(idx int64) (*Page,error) {
	ok,pag,err := f.readMmap(idx)
	if !ok { pag,err = f.read(idx) }
	return pag,err
}
func (f *PagedFile) readMmap(idx int64) (bool,*Page,error) {
	if f.mmapLdr==nil { return false,nil,nil }
	
	ok,buf := f.mmapLdr.Read(idx*int64(f.Pagesize),f.Pagesize)
	if !ok { return false,nil,nil }
	
	p := new(Page)
	p.buf = buf
	
	return true,p,nil
}
func (f *PagedFile) read(idx int64) (*Page,error) {
	if idx<0 { return nil,nil }
	
	p := new(Page)
	p.alloc = buffer.Get(f.Pagesize)
	p.buf = (*p.alloc)[:f.Pagesize]
	
	n,err := f.File.ReadAt(p.buf,idx*int64(f.Pagesize))
	
	if n<f.Pagesize && err==nil {
		err = ErrBlockTooShort
	}
	if err!=nil {
		buffer.Put(p.alloc)
		return nil,err
	}
	if n<f.Pagesize {
		panic(ErrBlockTooShort)
	}
	
	return p,nil
}
func (f *PagedFile) Write(buf []byte,idx int64) (error) {
	ok,err := f.writeMmap(buf,idx)
	if !ok { err = f.write(buf,idx) }
	return err
}
func (f *PagedFile) writeMmap(buf []byte,idx int64) (bool,error) {
	if f.mmapLdr==nil { return false,nil }
	if len(buf)>f.Pagesize { buf = buf[:f.Pagesize] }
	ok := f.mmapLdr.Write(buf,idx*int64(f.Pagesize))
	return ok,nil
}
func (f *PagedFile) write(buf []byte,idx int64) (error) {
	if len(buf)>f.Pagesize { buf = buf[:f.Pagesize] }
	_,err := f.File.WriteAt(buf,idx*int64(f.Pagesize))
	return err
}
// Syncs the MMAP writer.
// If this paged file is not MMAPed, this has no effect.
func (f *PagedFile) Msync() {
	if f.mmapLdr==nil { return }
	f.mmapLdr.Sync()
}
// Closes/frees the MMAP resources.
// If this paged file is not MMAPed, this has no effect.
func (f *PagedFile) Mclose() {
	if f.mmapLdr==nil { return }
	f.mmapLdr.Close()
}
