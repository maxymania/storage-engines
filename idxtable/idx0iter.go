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
import "bytes"

type RecordIter struct{
	curs *bolt.Cursor
	k,v  []byte
}

func (r *RecordIter) Next() (recid uint64,value []byte, ok bool) {
	if !bytes.HasPrefix(r.k,pxRECORD) { return }
	//if len(r.k)==0 { return }
	//if r.k[0]!=':' { return }
	recid = decodeNum(r.k[1:])
	value = r.v
	r.k,r.v = r.curs.Next()
	ok = true
	return
}
func (i *IndexNode) GetRecords() *RecordIter {
	r := new(RecordIter)
	r.curs = i.self.b.Cursor()
	r.k,r.v = r.curs.Seek(pxRECORD)
	return r
}
func (i *IndexNode) GetRecord() (recid uint64,value []byte,ok bool) {
	cur := i.self.b.Cursor()
	k,v := cur.Seek(pxRECORD)
	if !bytes.HasPrefix(k,pxRECORD) { return }
	recid = decodeNum(k[1:])
	value = v
	ok = true
	return
}

type IndexIter struct{
	tx   *Tx
	self *node
	curs *bolt.Cursor
	key  []byte
}
func (ii *IndexIter) Next() (key []byte, ni *IndexNode, ok bool) {
	if !bytes.HasPrefix(ii.key,pxKEY) { return }
	
	key = ii.key[1:]
	nn := &node{ii.self.m,ii.curs.Bucket(),ii.self,key}
	ni = &IndexNode{ii.tx,nn}
	ok = true
	ii.key,_ = ii.curs.Next()
	return
}
func (ii *IndexIter) Prev() (key []byte, ni *IndexNode, ok bool) {
	ii.key,_ = ii.curs.Prev()
	
	if !bytes.HasPrefix(ii.key,pxKEY) { return }
	
	key = ii.key[1:]
	nn := &node{ii.self.m,ii.curs.Bucket(),ii.self,key}
	ni = &IndexNode{ii.tx,nn}
	ok = true
	return
}

func (ii *IndexIter) Seek(elem interface{}) (key []byte, ni *IndexNode, ok bool) {
	pos := ii.self.m.reset().adds("/").addk(elem).buf
	ii.key,_ = ii.curs.Seek(pos)
	
	if !bytes.HasPrefix(ii.key,pxKEY) { return }
	
	key = ii.key[1:]
	nn := &node{ii.self.m,ii.curs.Bucket(),ii.self,key}
	ni = &IndexNode{ii.tx,nn}
	ok = true
	return
}

func (ii *IndexIter) Last() (key []byte, ni *IndexNode, ok bool) {
	{
		k,_ := ii.curs.Seek(pxlKEY)
		if len(k)==0 {
			ii.key,_ = ii.curs.Last()
		} else {
			ii.key,_ = ii.curs.Prev()
		}
	}
	
	if !bytes.HasPrefix(ii.key,pxKEY) { return }
	
	key = ii.key[1:]
	nn := &node{ii.self.m,ii.curs.Bucket(),ii.self,key}
	ni = &IndexNode{ii.tx,nn}
	ok = true
	return
}
func (ii *IndexIter) First() (key []byte, ni *IndexNode, ok bool) {
	ii.key,_ = ii.curs.Seek(pxKEY)
	
	if !bytes.HasPrefix(ii.key,pxKEY) { return }
	
	key = ii.key[1:]
	nn := &node{ii.self.m,ii.curs.Bucket(),ii.self,key}
	ni = &IndexNode{ii.tx,nn}
	ok = true
	return
}

func (i *IndexNode) GetKeys() *IndexIter {
	r := new(IndexIter)
	r.curs = i.self.b.Cursor()
	r.key,_ = r.curs.Seek(pxKEY)
	return r
}

