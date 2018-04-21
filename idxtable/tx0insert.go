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

import "github.com/boltdb/bolt"
import "encoding/binary"

type definer struct {
	tx *Tx
	recid uint64
	damage error
}
func (d *definer) addKey(key Key, value []byte, exclusive bool) {
	if d.damage!=nil { return } // No action!
	n := d.tx.root
	for _,elem := range key {
		n,d.damage = n.lookup(elem,true)
		if d.damage!=nil { return } // abort!
	}
	if n==d.tx.root { return } // empty key-tuples are not permitted
	d.damage = n.set(d.recid,value,exclusive)
}
func (d *definer) UniqueKey(key Key, value []byte) {
	d.addKey(key,value,true)
}
func (d *definer) SharedKey(key Key, value []byte) {
	d.addKey(key,value,false)
}

// If an insertion fails, the transaction must be rolled back.
func (tx *Tx) Insert(record []byte) (recid uint64,err error) {
	var reckey [8]byte
	var seq uint64
	if !tx.inner.Writable() { err = bolt.ErrTxNotWritable ; return }
	if tx.extract==nil { err = EInternal ; return }
	seq,err = tx.records.NextSequence()
	if err!=nil { tx.hadError(err); return }
	binary.BigEndian.PutUint64(reckey[:],seq)
	tx.records.Put(reckey[:],record)
	def := &definer{tx,seq,nil}
	tx.extract.Extract(record,def)
	err = def.damage
	if err!=nil { tx.hadError(err); return }
	recid = seq
	return
}

