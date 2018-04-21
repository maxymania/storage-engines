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


package idxtable

import "encoding/binary"
import "bytes"

type checker struct {
	tx *Tx
	damage error
}
func (d *checker) addKey(key Key, value []byte, exclusive bool) {
	var err error
	if d.damage!=nil { return } // No action!
	n := d.tx.root
	for _,elem := range key {
		if n==nil { return }
		n,err = n.lookup(elem,false)
		if err==EInvalidKey { d.damage = EInvalidKey ; return }
		if err!=nil { n = nil }
	}
	if n==nil { return }
	if n==d.tx.root { return } // empty key-tuples are not permitted
	d.damage = n.check(exclusive)
}
func (d *checker) UniqueKey(key Key, value []byte) {
	d.addKey(key,value,true)
}
func (d *checker) SharedKey(key Key, value []byte) {
	d.addKey(key,value,false)
}

// Checks, whether or not the record can be inserted without error.
// This method does not modify anything.
func (tx *Tx) Check(record []byte) (err error) {
	if tx.extract==nil { err = EInternal ; return }
	def := &checker{tx,nil}
	tx.extract.Extract(record,def)
	err = def.damage
	return
}

type checker2 struct {
	tx *Tx
	recid uint64
	damage error
}
func (d *checker2) addKey(key Key, value []byte, exclusive bool) {
	var err error
	if d.damage!=nil { return } // No action!
	n := d.tx.root
	for _,elem := range key {
		if n==nil { return }
		n,err = n.lookup(elem,false)
		if err==EInvalidKey { d.damage = EInvalidKey ; return }
		if err!=nil { n = nil }
	}
	if n==nil { return }
	if n==d.tx.root { return } // empty key-tuples are not permitted
	d.damage = n.check2(d.recid,exclusive)
}
func (d *checker2) UniqueKey(key Key, value []byte) {
	d.addKey(key,value,true)
}
func (d *checker2) SharedKey(key Key, value []byte) {
	d.addKey(key,value,false)
}

// Checks, whether or not the record can be updated without error.
// This method does not modify anything.
func (tx *Tx) CheckUpdate(recid uint64,record []byte) (err error) {
	var reckey [8]byte
	binary.BigEndian.PutUint64(reckey[:],recid)
	if tx.extract==nil { err = EInternal ; return }
	if tx.records==nil { err = ENoRecord ; return }
	ref,_ := tx.records.Cursor().Seek(reckey[:])
	if !bytes.Equal(ref,reckey[:]) { err = ENoRecord ; return }
	
	def := &checker2{tx,recid,nil}
	tx.extract.Extract(record,def)
	err = def.damage
	return
}
