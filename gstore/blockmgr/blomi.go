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

import "errors"

var ErrOutOfBounds = errors.New("ErrOutOfBounds")
var enotsupp = errors.New("enotsupp")

const mb128 = 1<<27

func epick(e ...error) error {
	for _,ee := range e {
		if ee!=nil { return ee }
	}
	return nil
}

type BlockManager interface{
	Length() uint64
	
	// Reads a block.
	// 
	// The block must be returned using DiscardRead in order to reclaim
	// memory.
	ReadBlock(i uint64) ([]byte,error)
	DiscardRead(buf []byte)
	
	// Write methods. Workflow is Allocate and WriteBack, or DiscardWrite if
	// the write is canceled. Note that, when using DiscardWrite, it is undefined,
	// wether or not the data is actually written.
	// 
	// In an Memory-Mapped implementation, Writes will always succeed.
	Allocate(i uint64) ([]byte,error)
	WriteBack(i uint64,buf []byte) error
	DiscardWrite(buf []byte)
	
	// Grows the Block file, returning offset and length of the
	// newly appended space.
	GrowStep() (offset uint64,lng int,e error)
	
	// Synchronized all writes.
	Sync() error
	
	// Closes the File.
	Close() error
}

