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

type DB struct{
	DB *bolt.DB
	Extr Extractor
}

var (
	bktRecords = []byte(tRecords)
	bktIndex   = []byte(tIndex  )
)

func (db *DB) Begin(writable bool) (*Tx, error) {
	ltx,err := db.DB.Begin(writable)
	if err!=nil { return nil,err }
	tx := new(Tx)
	tx.init(db.Extr,ltx,bktRecords,bktIndex)
	return tx,nil
}
func (db *DB) Close() error { return db.DB.Close() }
func (db *DB) GoString() string { return db.DB.GoString() }
func (db *DB) Info() *bolt.Info { return db.DB.Info() }
func (db *DB) IsReadOnly() bool { return db.DB.IsReadOnly() }
func (db *DB) Path() string { return db.DB.Path() }
func (db *DB) Stats() bolt.Stats { return db.DB.Stats() }
func (db *DB) Sync() error { return db.DB.Sync() }
func (db *DB) View(fn func(*Tx) error) error {
	return db.DB.View(func(ltx *bolt.Tx) error {
		tx := new(Tx)
		tx.init(db.Extr,ltx,bktRecords,bktIndex)
		return fn(tx)
	})
}
func (db *DB) Update(fn func(*Tx) error) error {
	return db.DB.Update(func(ltx *bolt.Tx) error {
		tx := new(Tx)
		tx.init(db.Extr,ltx,bktRecords,bktIndex)
		return tx.getError(fn(tx))
	})
}
func (db *DB) Batch(fn func(*Tx) error) error {
	var bad error
	err := db.DB.Batch(func(ltx *bolt.Tx) error {
		if bad!=nil { return nil } // Commit without changes!
		tx := new(Tx)
		tx.init(db.Extr,ltx,bktRecords,bktIndex)
		
		bad = tx.err
		
		return tx.getError(fn(tx))
	})
	if err==nil { err = bad }
	return nil
}
