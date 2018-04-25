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


package mmdb2

/*
This structure represents a record.
*/
type Value struct{
	Record []byte
	Hash uint32
	KeyLen uint16
}
func (v *Value) Set(record []byte) *Value {
	v.Record = record
	v.KeyLen = getKeylen(record)
	return v
}
func (v *Value) Decode(start []byte) (err error) {
	var key []byte
	v.Record,key,_,err = DecodeRecord(start)
	v.KeyLen = uint16(len(key))
	return
}
func (v *Value) sHash(h uint32) { v.Hash = h }
func (v *Value) GHash () uint32 { return v.Hash }
func (v *Value) GKey  () []byte { return v.Record[8:8+uint(v.KeyLen)] }
func (v *Value) GValue() []byte { return v.Record[8+uint(v.KeyLen):] }

