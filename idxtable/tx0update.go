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

import "bytes"
import "github.com/boltdb/bolt"
import "encoding/binary"

// If an insertion fails, the transaction must be rolled back.
func (tx *Tx) Update(recid uint64, record []byte) (err error) {
	var reckey [8]byte
	if !tx.inner.Writable() { err = bolt.ErrTxNotWritable ; return }
	if tx.extract==nil { err = EInternal ; return }
	binary.BigEndian.PutUint64(reckey[:],recid)
	refk,oldRec := tx.records.Cursor().Seek(reckey[:])
	if !bytes.Equal(refk,reckey[:]) { err = ENoRecord ; return }
	
	rm := &remover{tx,recid,nil}
	tx.extract.Extract(oldRec,rm) // Remove the indeces to the old record.
	err = rm.damage
	if err!=nil { tx.hadError(err); return }
	
	tx.records.Put(reckey[:],record) // Overwrite the record.
	
	def := &definer{tx,recid,nil}
	tx.extract.Extract(record,def) // Create indeces from the new one.
	err = def.damage
	if err!=nil { tx.hadError(err); return }
	return
}

