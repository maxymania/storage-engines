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

import "github.com/davecgh/go-xdr/xdr2"
import "bytes"
import "errors"

var errOverflow = errors.New("overflow")

type bwriter struct{
	targ []byte
	pos  int
}
func (b *bwriter) rest() int {
	return len(b.targ)-b.pos
}
func (b *bwriter) Write(p []byte) (int,error) {
	if len(p)>=b.rest() { return 0,errOverflow }
	copy(b.targ[b.pos:],p)
	b.pos += len(p)
	return len(p),nil
}
func (b *bwriter) WriteString(p string) (int,error) {
	if len(p)>=b.rest() { return 0,errOverflow }
	copy(b.targ[b.pos:],p)
	b.pos += len(p)
	return len(p),nil
}
func (b *bwriter) WriteByte(p byte) error {
	if 0>=b.rest() { return errOverflow }
	b.targ[b.pos] = p
	b.pos++
	return nil
}

var align = [4]int{0,3,2,1}

type Pair struct{
	Key []byte
	Ref uint64
}
func (p Pair) Length() int {
	l := len(p.Key)
	return l+4+8+align[l&3]
}
type Pairs []Pair

func (p *Pairs) read(b []byte) error {
	_,err := xdr.Unmarshal(bytes.NewReader(b),p)
	return err
}
func (p Pairs) write(b []byte) error {
	_,err := xdr.Marshal(&bwriter{b,0},p)
	return err
}
