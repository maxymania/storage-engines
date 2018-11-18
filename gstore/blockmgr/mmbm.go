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

import "os"
import "github.com/edsrzf/mmap-go"
import "sync"

func chunkUp(i int64) []mmap.MMap{
	i /= mb128
	if i==0 { return nil }
	n := int64(1<<10)
	for i>n { n<<=1 }
	return make([]mmap.MMap,int(i),int(n))
}

type MmapBlockFile struct{
	blocklog  uint
	blocksize uint64
	bps uint64
	file   *os.File
	chunks []mmap.MMap
	dirty  []bool
	
	gmutex sync.Mutex
	drwmut sync.RWMutex
}
func NewMmapBlockFile(f *os.File, blocklog uint) (*MmapBlockFile,error) {
	s,err := f.Stat()
	if err!=nil { return nil,err }
	r := new(MmapBlockFile)
	r.blocklog = blocklog
	r.blocksize = 1<<blocklog
	r.bps = mb128>>blocklog
	r.file = f
	r.chunks = chunkUp(s.Size())
	r.dirty = make([]bool,cap(r.chunks))
	for i := range r.chunks {
		mm,err := mmap.MapRegion(r.file,mb128,mmap.RDWR,0,int64(i)*mb128)
		if err!=nil {
			for j := 0; j<i; j++ { r.chunks[j].Unmap() }
			return nil,err
		}
		r.chunks[i] = mm
	}
	return r,nil
}

func (m *MmapBlockFile) grow() []mmap.MMap{
	c := m.chunks
	n := cap(c)
	nn := n
	if n==0 {
		nn = 1<<10
	} else if n==len(c) {
		nn = n<<1 
	}
	if n==nn { return c }
	nc := make([]mmap.MMap,len(c),nn)
	copy(nc,c)
	od := m.dirty
	nd := make([]bool,nn)
	m.drwmut.Lock()
	m.dirty = nd
	go func() {
		defer m.drwmut.Unlock()
		for i,ok := range od {
			if ok { nd[i] = true }
		}
	}()
	return nc
}

func (m *MmapBlockFile) Length() uint64 { return uint64(len(m.chunks))*m.bps }
func (m *MmapBlockFile) ReadBlock(i uint64) ([]byte,error) {
	if i>=m.Length() { return nil,ErrOutOfBounds }
	j := i/m.bps
	k := i&(m.bps-1)<<m.blocklog
	return m.chunks[j][k:k+m.blocksize],nil
}
func (m *MmapBlockFile) DiscardRead(buf []byte) {}
func (m *MmapBlockFile) Allocate(i uint64) ([]byte,error) { return m.ReadBlock(i) }
func (m *MmapBlockFile) WriteBack(i uint64,buf []byte) error {
	m.dirty[i/m.bps] = true
	return nil
}
func (m *MmapBlockFile) DiscardWrite(buf []byte) {}
func (m *MmapBlockFile) GrowStep() (offset uint64,lng int,e error) {
	m.gmutex.Lock(); defer m.gmutex.Unlock()
	nz := (1+int64(len(m.chunks))) * mb128
	e = m.file.Truncate(nz)
	if e!=nil { return }
	var mm mmap.MMap
	mm,e = mmap.MapRegion(m.file,mb128,mmap.RDWR,0,nz-mb128)
	offset = uint64(nz-mb128)>>m.blocklog
	if e!=nil {
		m.file.Truncate(nz-mb128)
		return
	}
	m.chunks = append(m.grow(),mm)
	lng = int(m.bps)
	return
}
func (m *MmapBlockFile) Sync() error {
	m.drwmut.RLock(); defer m.drwmut.RUnlock()
	for i,mm := range m.chunks {
		if !m.dirty[i] { continue }
		e := mm.Flush()
		if e!=nil { return e }
		m.dirty[i] = false
	}
	return nil
}
func (m *MmapBlockFile) Close() error {
	var me error
	for i := range m.chunks {
		e := m.chunks[i].Unmap()
		if me==nil&&e!=nil { me=e }
	}
	fe := m.file.Close()
	return epick(me,fe)
}

var _ BlockManager = (*MmapBlockFile)(nil)

