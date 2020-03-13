
package pfmmap

import "os"
import "sort"
//import "runtime"
import "sync"
import mmap "github.com/edsrzf/mmap-go"
import "github.com/maxymania/storage-engines/pagedfile"

type fileElement struct {
	offset,length int64
	dirty  bool
	region mmap.MMap
}

const maxint = (^uint(0))>>1

var pagesize int

func (e *fileElement) Read(offset int64, size int) (bool, []byte) {
	if e==nil { return false,nil }
	offset -= e.offset
	end := offset+int64(size)
	if offset<0 || end>e.length { return false,nil }
	return true,e.region[offset:end]
}
func (e *fileElement) Write(buf []byte, offset int64) bool {
	ok,dst := e.Read(offset,len(buf))
	if ok { copy(dst,buf) }
	return ok
}
func (e *fileElement) NotifySize(size int64) {}
func (e *fileElement) Sync() {
	if e.dirty {
		e.region.Flush()
	}
}
func (e *fileElement) Close() {
	e.region.Unmap()
}

func createMapping(f *os.File, offset int64, size int) *fileElement {
	numap,err := mmap.MapRegion(f,size,mmap.RDWR,0,offset)
	if err!=nil { return nil }
	fe := new(fileElement)
	fe.offset = offset
	fe.length = int64(size)
	fe.region = numap
	return fe
}

func search(a []*fileElement,offset int64) *fileElement {
	switch len(a) {
	case 0: return nil
	case 1: return a[0]
	}
	
	p := sort.Search(len(a),func(i int) bool {
		return offset<a[i].offset
	})-1
	
	if p<0 { return a[0] }
	return a[p]
}
func getEnd(a []*fileElement) int64 {
	i := len(a)-1
	if i<0 { return 0 }
	e := a[i]
	return e.offset+e.length
}

type fileMapping struct {
	file *os.File
	mu sync.Mutex
	elems []*fileElement
}

func (f *fileMapping) Read(offset int64, size int) (bool, []byte) {
	return search(f.elems,offset).Read(offset,size)
}
func (f *fileMapping) Write(buf []byte, offset int64) bool {
	return search(f.elems,offset).Write(buf,offset)
}
func (f *fileMapping) NotifySize(size int64) {
	size -= size%int64(pagesize)
	if size>int64(maxint) { return }
	
	f.mu.Lock(); defer f.mu.Unlock()
	
	offset := getEnd(f.elems)
	lng := size-offset
	fm := createMapping(f.file,offset,int(lng))
	if fm==nil { return }
	f.elems = append(f.elems,fm)
}
func (f *fileMapping) Sync() {
	f.mu.Lock(); defer f.mu.Unlock()
	for _,e := range f.elems {
		e.Sync()
	}
}
func (f *fileMapping) Close() {
	f.mu.Lock(); defer f.mu.Unlock()
	for _,e := range f.elems {
		e.Close()
	}
}
var _ pagedfile.MmapLoader = (*fileMapping)(nil)

func createLoader(f *os.File) (pagedfile.MmapLoader,error) {
	fm := new(fileMapping)
	fm.file = f
	return fm,nil
}

func init() {
	pagesize = os.Getpagesize()
	//if runtime.GOOS=="windows" {
		//pagesize = 1<<16
	//}
	if pagesize==0 { return }
	pagedfile.MmapNew = createLoader
}
