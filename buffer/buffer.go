/*
Copyright (c) 2017 Simon Schmidt

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


package buffer

import "sync"

const (
	minBZ = 6  // 2^6 = 64 is a CPU cache line size
	steps = 21 // 2^(6+21-1) = 64MB is the maximum size.
)

var pools [steps]sync.Pool

func bzero(b []byte) {
	for i := range b { b[i] = 0 }
}

func generator(n int) func() interface{} {
	return func() interface{} {
		buf := new([]byte)
		*buf = make([]byte,n,n)
		return buf
	}
}

func init() {
	for i := range pools {
		pools[i].New = generator(1<<(minBZ+uint(i)))
	}
}

func index(size int) int {
	i := 0
	m := 1<<minBZ
	for ; i<steps ; i++ {
		if size<=m { return i }
		m<<=1
	}
	return -1
}

func Get(size int) *[]byte {
	idx := index(size)
	if idx<0 { b := make([]byte,size) ; return &b }
	return pools[idx].Get().(*[]byte)
}
func CGet(size int) *[]byte {
	data := Get(size)
	bzero(*data)
	return data
}
func Put(b *[]byte) {
	if b==nil { return }
	n := len(*b)
	if (n&(n-1))!=0 || n<64 { return }
	idx := index(n)
	if idx<0 { return }
	pools[idx].Put(b)
}

