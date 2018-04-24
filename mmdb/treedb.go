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

import bt "github.com/emirpasic/gods/trees/btree"
import rb "github.com/emirpasic/gods/trees/redblacktree"
import avl "github.com/emirpasic/gods/trees/avltree"
import "sync"
import "github.com/emirpasic/gods/containers"

type LoadFlags uint

const (
	L_NONE LoadFlags = 0
	
	// If this flag is given. The DB will use an AVL tree instead of a Red-Black-Tree
	L_USE_AVL_TREE LoadFlags = 1<<iota
	
	// If this flag is given. The DB will use a B-tree (order=5) instead of a Red-Black-Tree
	L_USE_B_TREE
	
	// If this flag is set, the load process stops, once it encounters an invalid record.
	L_STOP_INVALID
	
	// If the log contains an invalid tail, it will be discarded using this flag.
	L_DISCARD_INVALID_TAIL
)
func (l LoadFlags) has(f LoadFlags) bool { return (l&f)!=0 }

type Iterator struct {
	l sync.Locker
	i containers.ReverseIteratorWithKey
}
func (i *Iterator) Begin() { i.i.Begin() }
func (i *Iterator) First() bool { return i.i.First() }
func (i *Iterator) Next() bool { return i.i.Next() }
func (i *Iterator) Prev() bool { return i.i.Prev() }
func (i *Iterator) Last() bool { return i.i.Last() }
func (i *Iterator) End() { i.i.End() }
func (i *Iterator) Value() interface{} { return i.Raw().Value }
func (i *Iterator) Key() interface{} { return i.Raw().Key }
func (i *Iterator) Raw() *Value { return i.i.Value().(*Value) }
func (i *Iterator) Close() {
	l := i.l
	i.l = nil
	if l!=nil { l.Unlock() }
}

type bTree struct{
	*bt.Tree
}
func (b bTree) Iterator() containers.ReverseIteratorWithKey {
	i := new(bt.Iterator)
	*i = b.Tree.Iterator()
	return i
}

type rbTree struct{
	*rb.Tree
}
func (r rbTree) Iterator() containers.ReverseIteratorWithKey {
	i := new(rb.Iterator)
	*i = r.Tree.Iterator()
	return i
}

type iTree interface{
	Put(key interface{}, value interface{})
	Get(key interface{}) (value interface{}, found bool)
	Remove(key interface{})
	Iterator() containers.ReverseIteratorWithKey
	Size() int
}

type DB struct{
	Ty  DBType
	Ch *Chunk
	tree iTree
	tlck sync.RWMutex
}

/*
This method initializes the search-tree and loads the Database.

".Ty" must be set to T_Sorted or T_Hashed (or a custom implementation).

".Ch" must be set to a properly initialized *Chunk-object.
*/
func (db *DB) Open(flags LoadFlags) {
	if flags.has(L_USE_AVL_TREE) {
		db.tree = avl.NewWith(db.Ty.Comp)
	} else if flags.has(L_USE_B_TREE) {
		db.tree = bTree{bt.NewWith(5,db.Ty.Comp)}
	} else {
		db.tree = rbTree{rb.NewWith(db.Ty.Comp)}
	}
	// Load the Key-Value records
	db.load(flags)
}

func (db *DB) indexRecord(record []byte,pos int64) (err error){
	v := new(Value).Set(record).Pos(pos)
	k := db.Ty.Keyf(v.Key)
	db.tlck.Lock(); defer db.tlck.Unlock()
	if len(v.Value)==0 {
		db.tree.Remove(k)
	} else {
		db.tree.Put(k,v)
	}
	
	return
}
// Inserts a raw Record. Useful if you copy or compact a database.
func (db *DB) PutRAW(record []byte) (err error) {
	var pos int64
	record,pos,err = db.Ch.Append(record)
	if err!=nil { return }
	err = db.indexRecord(record,pos)
	return
}
func (db *DB) Delete(key []byte) (err error) {
	return db.Put(key,nil)
}
func (db *DB) Put(key, value []byte) (err error) {
	var reclen int
	var pos int64
	var record []byte
	reclen,err = recordLength(len(key),len(value))
	if err!=nil { return }
	record,pos,err = db.Ch.Skip(int64(reclen))
	if err!=nil { return }
	encodeRecordInto(record,key,value)
	err = db.indexRecord(record,pos)
	return
}
func (db *DB) Get(key []byte, locked bool) (okey,value []byte,ok bool) {
	if locked {
		db.tlck.RLock(); defer db.tlck.RUnlock()
	}
	v1,_ := db.tree.Get(db.Ty.Keyf(key))
	v2,dec := v1.(*Value)
	if !dec { return }
	okey = v2.Key
	value = v2.Value
	ok = true
	return
}
func (db *DB) Iterator(locked bool) *Iterator {
	var l sync.Locker
	if locked {
		l = db.tlck.RLocker()
		l.Lock()
	}
	return &Iterator{l,db.tree.Iterator()}
}
func (db *DB) load(flags LoadFlags) {
	mem,pos := db.Ch.GetCommitted()
	
	// The end of the last valid record in the log.
	usedPos := pos
	for {
		v := new(Value)
		err := v.Pos(pos).Decode(mem)
		if err==EShortRecord { break }
		if err==EInvalidRecord && flags.has(L_STOP_INVALID) { break }
		mem = mem[len(v.Record):]
		pos += int64(len(v.Record))
		
		if err!=nil { continue } // Do not use invalid or short records!
		
		// Update the end of the last record.
		usedPos = pos
		
		k := db.Ty.Keyf(v.Key)
		if len(v.Value)==0 {
			db.tree.Remove(k)
		} else {
			db.tree.Put(k,v)
		}
	}
	if flags.has(L_DISCARD_INVALID_TAIL) {
		db.Ch.SetLast(usedPos)
	}
}
func (db *DB) Commit() {
	db.Ch.Snapshot()
}

