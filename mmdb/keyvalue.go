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

import "bytes"
import "github.com/dgryski/go-farm"
import "github.com/emirpasic/gods/utils"

type Keyer func(b []byte) interface{}

type DBType struct{
	Comp utils.Comparator
	Keyf Keyer
}

/*
This structure represents a record.
*/
type Value struct{
	Record, Key, Value []byte
	Position int64
}
func (v *Value) Pos(pos int64) *Value {
	v.Position = pos
	return v
}
func (v *Value) Set(record []byte) *Value {
	v.Record = record
	v.Key,v.Value = SplitRecord(record)
	return v
}
func (v *Value) Decode(start []byte) (err error) {
	v.Record,v.Key,v.Value,err = DecodeRecord(start)
	return
}

func CompareBytes(a, b interface{}) int {
	return bytes.Compare(a.([]byte),b.([]byte))
}

func KeyfBytes(b []byte) interface{} { return b }

var T_Sorted = DBType { CompareBytes, KeyfBytes}

/*
This structure represents a tuple (hash(key),key).

farmhash's Fingerprint32 is the hash function.
*/
type Key struct{
	Hash uint32
	Self []byte
}
func CompareKeys(a, b interface{}) int {
	A := a.(Key)
	B := b.(Key)
	switch {
	case A.Hash > B.Hash: return 1
	case A.Hash < B.Hash: return -1
	default: return bytes.Compare(A.Self,B.Self)
	}
}
func MakeKey(b []byte) Key {
	return Key{farm.Fingerprint32(b),b}
}

func KeyfKey(b []byte) interface{} {
	return Key{farm.Fingerprint32(b),b}
}

var T_Hashed = DBType { CompareKeys, KeyfKey}

