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

package anytree

import "github.com/maxymania/storage-engines/buffer"

func AllocBuffer(n int) Buffer {
	obj := buffer.Get(n)
	return Buffer{ (*obj)[:n],obj }
}
func WrapBuffer(b []byte) Buffer {
	return Buffer{b,nil}
}
func CopyBuffer(b []byte) Buffer {
	bb := AllocBuffer(len(b))
	copy(bb.Ptr,b)
	return bb
}

/*
A byte-buffer with associated resources.
*/
type Buffer struct{
	Ptr []byte
	res *[]byte
}
func (b Buffer) Free() {
	if b.res!=nil {
		buffer.Put(b.res)
	}
}

