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
import "bytes"

var (
	pxRECORD = []byte(":")
	pxKEY    = []byte("/")
	
	pxlKEY   = []byte{'/' + 1}
	
	atRECORDS = []byte(".records")
	atKEYS    = []byte(".keys")
	atKING    = []byte(".king")
)

type sKey interface{}

func decodeNum(b []byte) uint64 {
	if len(b)<8 { return 0 }
	return binary.BigEndian.Uint64(b)
}

type memory struct{
	buf []byte
}
func (m *memory) reset() *memory{
	m.buf = m.buf[:0]
	return m
}
func (m *memory) add(b []byte) *memory{
	m.buf = append(m.buf,b...)
	return m
}
func (m *memory) adds(b string) *memory{
	m.buf = append(m.buf,b...)
	return m
}
func (m *memory) addk(b sKey) *memory{
	switch v := b.(type) {
	case string: m.buf = append(m.buf,v...)
	case []byte: m.buf = append(m.buf,v...)
	}
	return m
}
func tobytes(b sKey,s []byte) []byte{
	switch v := b.(type) {
	case string: return append(s[:0],v...)
	case []byte: return append(s[:0],v...)
	}
	return s[:0]
}

type node struct {
	m      *memory
	b      *bolt.Bucket
	parent *node
	name   []byte
}

func (n *node) incr(k []byte) error {
	var IT [8]byte
	rc := decodeNum(n.b.Get(k))
	binary.BigEndian.PutUint64(IT[:],rc + 1)
	return n.b.Put(k,IT[:])
}
func (n *node) decr(k []byte) (rc uint64,err error) {
	var IT [8]byte
	rc = decodeNum(n.b.Get(k)) - 1
	binary.BigEndian.PutUint64(IT[:],rc)
	err = n.b.Put(k,IT[:])
	return
}

func (n *node) lookup(key sKey, create bool) (sub *node,err error) {
	temp := n.m.reset().adds("/").addk(key).buf
	lk := len(temp)-1
	// Empty keys are invalid. An empty key can indicate an invalid value to be passed.
	if lk==0 { err = EInvalidKey; return }
	
	var b *bolt.Bucket
	
	var skey []byte
	{
		cur := n.b.Cursor()
		if k,_ := cur.Seek(temp); bytes.Equal(temp,k) {
			// Found it!
			skey = k[1:]
			create = false
		} else if create {
			// Create it!
			_,err = n.b.CreateBucket(temp)
			if err!=nil { return }
			k,_ := cur.Seek(temp)
			
			// The key SHOULD be available in the cursor should be available.
			if !bytes.Equal(temp,k) { err = EFatalInternal ; return }
			skey = k[1:]
		} else {
			// Not found, not created!
			err = ENoKey
			return
		}
		b = n.b.Bucket(temp)
	}
	
	
	if create {
		var NULL [8]byte
		binary.BigEndian.PutUint64(NULL[:],0)
		
		// '.records'
		err = b.Put(atRECORDS,NULL[:])
		if err!=nil { return }
		
		// '.keys'
		err = b.Put(atKEYS,NULL[:])
		if err!=nil { return }
		
		err = n.incr(atKEYS)
		if err!=nil { return }
	}
	
	//sub = &node{n.m,b,n,tobytes(key,make([]byte,lk))}
	sub = &node{n.m,b,n,skey}
	return
}

func (n *node) rmrec(recid uint64) (err error) {
	var NULL [8]byte
	binary.BigEndian.PutUint64(NULL[:],0)
	temp := n.m.reset().adds(":12345678").buf
	binary.BigEndian.PutUint64(temp[1:],recid)
	
	cur := n.b.Cursor()
	if ek,_ := cur.Seek(temp); !bytes.Equal(ek,temp) {
		return ENoKey
	}
	
	cur.Delete()
	
	err = n.b.Delete(atKING) // If we have '.king', there is only one Record!
	
	if err!=nil { return }
	
	var rc uint64
	
	rc,err = n.decr(atRECORDS)
	if err!=nil { return }
	
	if rc!=0 { return } // We still have more Records
	
	if decodeNum(n.b.Get(atKEYS))==0 {
		// At this point, the node has no records and no keys.
		return n.del()
	}
	
	return
}

func (n *node) del() (err error) {
	
	for n!=nil {
		key := n.name
		n = n.parent
		
		if n==nil { return } // If there is no parent, break the loop
		
		temp := n.m.reset().adds("/").add(key).buf
		err = n.b.DeleteBucket(temp)
		if err!=nil { return }
		
		var rc uint64
		
		rc,err = n.decr(atKEYS)
		if err!=nil { return }
		
		// Check the sub-key counter.
		// If we have more than zero keys left in the parent node,
		// it will not be deleted.
		if rc!=0 { return }
		
		// Check the record counter.
		// If we have more than zero records left in the parent node,
		// it will not be deleted.
		if decodeNum(n.b.Get(atRECORDS))!=0 { return }
		
		// At this point, the parent node has both '.keys' and '.records'
		// equals ZERO. This means, it will be deleted as well!
	}
	
	return
}

func (n *node) check(exclusive bool) error {
	cur := n.b.Cursor()
	
	if exclusive {
		if k,_ := cur.Seek(pxRECORD); bytes.HasPrefix(k,pxRECORD) {
			/* We want a node exclusively, but there is already another Record. */
			return EConflict
		}
	}
	
	// if we have '.king', we have a conflict
	if k,_ := cur.Seek(atKING); bytes.Equal(k,atKING) {
		/* Another record already owns this node exclusively. */
		return EConflict
	}
	
	return nil
}

func (n *node) check2(recid uint64, exclusive bool) error {
	temp := n.m.reset().adds(":12345678").buf
	binary.BigEndian.PutUint64(temp[1:],recid)
	
	cur := n.b.Cursor()
	
	// A reference to our record already exists? No Conflict!
	if k,_ := cur.Seek(temp); bytes.Equal(k,temp) { return nil }
	
	if exclusive {
		if k,_ := cur.Seek(pxRECORD); bytes.HasPrefix(k,pxRECORD) {
			/* We want a node exclusively, but there is already another Record. */
			return EConflict
		}
	}
	
	// if we have '.king', we have a conflict
	if k,v := cur.Seek(atKING); bytes.Equal(k,atKING) && decodeNum(v)!=recid {
		/* Another record already owns this node exclusively. */
		return EConflict
	}
	
	return nil
}


func (n *node) set(recid uint64,value []byte, exclusive bool) error {
	var REC [8]byte
	binary.BigEndian.PutUint64(REC[:],recid)
	temp := n.m.reset().adds(":12345678").buf
	binary.BigEndian.PutUint64(temp[1:],recid)
	cur := n.b.Cursor()
	
	/* We already have a key. Overwrite. */
	if k,_ := cur.Seek(temp); bytes.Equal(k,temp) { return n.b.Put(temp,value) } // Fast path.
	
	if exclusive {
		if k,_ := cur.Seek(pxRECORD); bytes.HasPrefix(k,pxRECORD) {
			/* Do not delete, already in use. */
			return EConflict
		}
	}
	
	// if we have '.king', we have a conflict
	if k,v := cur.Seek(atKING); bytes.Equal(k,atKING) && decodeNum(v)!=recid {
		/* Do not delete, already in use. */
		return EConflict
	}
	
	if exclusive {
		n.b.Put(atKING,REC[:])
	}
	
	rc := decodeNum(n.b.Get(atRECORDS))
	
	binary.BigEndian.PutUint64(REC[:],rc+1)
	n.b.Put(atRECORDS,REC[:])
	
	// --------------------------------------------
	
	//temp = n.m.reset().adds(":12345678").buf
	//binary.BigEndian.PutUint64(temp[1:],recid)
	
	n.b.Put(temp,value)
	
	return nil
}
func (n *node) get(recid uint64) (value []byte,err error) {
	temp := n.m.reset().adds(":12345678").buf
	binary.BigEndian.PutUint64(temp[1:],recid)
	if k,v := n.b.Cursor().Seek(temp); bytes.Equal(temp,k) {
		value = v
	} else {
		err = ENoRecord
	}
	value = n.b.Get(temp)
	return
}

// Secret API

func (secrets) ReadIndex(root *bolt.Bucket,keys Key, recid uint64) (value []byte,err error) {
	n := &node{new(memory),root,nil,nil}
	for _,key := range keys {
		n,err = n.lookup(key,false)
		if err!=nil { return }
	}
	return n.get(recid)
}
func (secrets) WriteIndex(root *bolt.Bucket,keys Key, recid uint64, value []byte, exclusive bool) (err error) {
	n := &node{new(memory),root,nil,nil}
	for _,key := range keys {
		n,err = n.lookup(key,true)
		if err!=nil { return }
	}
	return n.set(recid,value,exclusive)
}

