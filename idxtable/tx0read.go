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

func (tx *Tx) ReadRecord(recid uint64) []byte {
	var reckey [8]byte
	binary.BigEndian.PutUint64(reckey[:],recid)
	
	if tx.records==nil { return nil }
	
	return tx.records.Get(reckey[:])
}

type IndexNode struct{
	tx   *Tx
	self *node
}
func (i *IndexNode) Tx() *Tx { return i.tx }

func (tx *Tx) LookupKey(key Key) (j *IndexNode, err error) {
	n := tx.root
	if n==nil { err = ENoKey; return }
	for _,ks := range key {
		n,err = n.lookup(ks,false)
		if err!=nil { return }
	}
	j = &IndexNode{tx,n}
	return
}

func (i *IndexNode) LookupKey(key Key) (j *IndexNode, err error) {
	n := i.self
	if n==nil { err = ENoKey; return }
	for _,ks := range key {
		n,err = n.lookup(ks,false)
		if err!=nil { return }
	}
	j = &IndexNode{i.tx,n}
	return
}

// Count's the amount of records within this Node. Speed: O(1)
func (i *IndexNode) CountRecords() (num uint64,err error) {
	n := i.self
	if n==nil { err = ENoKey; return }
	num = decodeNum(n.b.Get(atRECORDS))
	return
}

// Count's the amount of sub-keys within this Node. Speed: O(1)
func (i *IndexNode) CountKeys() (num uint64,err error) {
	n := i.self
	if n==nil { err = ENoKey; return }
	num = decodeNum(n.b.Get(atKEYS))
	return
}

// Retrieves an Value node associated between an Index Node and a Record.
func (i *IndexNode) GetValue(recid uint64) []byte {
	n := i.self
	temp := n.m.reset().adds(":12345678").buf
	binary.BigEndian.PutUint64(temp[1:],recid)
	return n.b.Get(temp)
}

// Represents an iteration over all records.
type Cursor struct{
	cur *bolt.Cursor
}
func (c *Cursor) decode(k,v []byte) (recid uint64,value []byte, ok bool) {
	if len(k)!=8 { return }
	recid = binary.BigEndian.Uint64(k)
	value = v
	ok = true
	return
}
func (c *Cursor) First() (recid uint64,value []byte, ok bool) { if c.cur==nil { return }; return c.decode(c.cur.First()) }
func (c *Cursor) Last() (recid uint64,value []byte, ok bool) { if c.cur==nil { return }; return c.decode(c.cur.Last()) }
func (c *Cursor) Next() (recid uint64,value []byte, ok bool) { if c.cur==nil { return }; return c.decode(c.cur.Next()) }
func (c *Cursor) Prev() (recid uint64,value []byte, ok bool) { if c.cur==nil { return }; return c.decode(c.cur.Prev()) }
func (c *Cursor) Seek(id uint64) (recid uint64,value []byte, ok bool) {
	if c.cur==nil { return }
	var seek [8]byte
	binary.BigEndian.PutUint64(seek[:],id)
	return c.decode(c.cur.Seek(seek[:]))
}

func (tx *Tx) Cursor() *Cursor {
	if tx.records==nil { return nil }
	return &Cursor{tx.records.Cursor()}
	return &Cursor{tx.records.Cursor()}
}

