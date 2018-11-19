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


package gstore

import "github.com/cznic/file"
import "github.com/vmihailenco/msgpack"

type header struct{
	P1 uint64
	P2 uint64
	Tables map[string]uint64
}

type fileWriter struct{
	file.File
	pos int64
}
func (f *fileWriter) Write(p []byte) (n int, err error) {
	n,err = f.WriteAt(p,f.pos)
	f.pos += int64(n)
	return
}
func (f *fileWriter) cut() error { return f.Truncate(f.pos) }

func (h *header) write(f file.File) error {
	w := &fileWriter{f,0}
	err := msgpack.NewEncoder(w).Encode(h)
	if err!=nil { return err }
	return w.cut()
}

type fileReader struct{
	file.File
	pos int64
}
func (f *fileReader) Read(p []byte) (n int, err error) {
	n,err = f.ReadAt(p,f.pos)
	f.pos += int64(n)
	return
}

func (h *header) read(f file.File) error {
	return msgpack.NewDecoder(&fileReader{f,0}).Decode(h)
}

