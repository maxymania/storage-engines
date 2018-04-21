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

const (
	tRecords = "RECORDS"
	tIndex   = "INDEX"
)

type Tx struct{
	extract Extractor
	inner   *bolt.Tx
	records *bolt.Bucket
	index   *bolt.Bucket
	root    *node
	err     error
}

// recs []byte(tRecords)
// idxs []byte(tIndex  )
func (t *Tx) init(e Extractor,i *bolt.Tx, recs, idxs []byte) (err error) {
	t.extract = e
	t.inner = i
	if t.inner.Writable() {
		t.records,err = t.inner.CreateBucketIfNotExists(recs)
		if err!=nil { return }
		t.index  ,err = t.inner.CreateBucketIfNotExists(idxs)
	} else {
		t.records = t.inner.Bucket(recs)
		t.index   = t.inner.Bucket(idxs)
	}
	if t.index!=nil {
		t.root = &node{new(memory),t.index,nil,nil}
	}
	return
}
func (t *Tx) hadError(e error) {
	t.err = e
}
func (t *Tx) getError(e error) error {
	if e==nil { e = t.err }
	return e
}
func (t *Tx) MustRollback() bool {
	return t.err!=nil
}

/*
Commit writes all changes to disk and updates the meta page.
Returns an error if a disk write error occurs, or if Commit
is called on a read-only transaction,
or if tx.MustRollback() returns true.
*/
func (t *Tx) Commit() error {
	if t.err!=nil { return t.err }
	return t.inner.Commit()
}
func (t *Tx) Rollback() error {
	return t.inner.Rollback()
}

