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


package timefile

import (
	"github.com/boltdb/bolt"
	"encoding/binary"
	"path/filepath"
	"os"
	"fmt"
)

var allocator = []byte("alloc")

const secDay uint64 = 60*60*24


type Ebool bool
func (e Ebool) Error() string {
	if e { return "ECorrupted" }
	return "EFalse"
}
const (
	EFalse = Ebool(false)
	ECorrupted = Ebool(true)
)


func ts2fn(i uint64) string {
	return fmt.Sprintf("tf_%016x",i)
}

type Allocator struct{
	DB *bolt.DB
	Path string
}
func (a *Allocator) check(expireAt uint64) (uint64,error) {
	var fi uint64
	var expb [8]byte
	binary.BigEndian.PutUint64(expb[:],expireAt)
	
	tx,err := a.DB.Begin(false)
	defer tx.Rollback()
	if err!=nil { return 0,err }
	bkt := tx.Bucket(allocator)
	if bkt==nil { return 0,EFalse }
	cur := bkt.Cursor()
	
	k,_ := cur.Seek(expb[:])
	if len(k)<8 { return 0,EFalse }
	fi = binary.BigEndian.Uint64(k)
	// Lemma: fi >= expireAt
	if (fi-secDay)>expireAt {
		// The expiration time is more than one day ahead. Not acceptable.
		return 0,EFalse
	}
	return fi,nil
}
func (a *Allocator) erase(bkt *bolt.Bucket,n int) {
	cur := bkt.Cursor()
	k,_ := cur.First()
	for i := 0; i<n; i++ {
		if len(k)<8 {
			if len(k)==0 { break }
			cur.Delete()
			k,_ = cur.Next()
			continue
		}
		fi := binary.BigEndian.Uint64(k)
		if fi<current {
			cur.Delete()
			k,_ = cur.Next()
			os.Remove(filepath.Join(a.Path,ts2fn(fi))) // Also remove the file.
			continue
		}
		
		break
	}
}
func (a *Allocator) GetPath(fi uint64) string { return filepath.Join(a.Path,ts2fn(fi)) }

func (a *Allocator) alloc(expireAt uint64) (fi uint64,err error) {
	//var err2 error
	err = a.DB.Batch(func(tx *bolt.Tx) error {
		var expb [8]byte
		binary.BigEndian.PutUint64(expb[:],expireAt)
		bkt,err := tx.CreateBucketIfNotExists(allocator)
		if err!=nil { return err }
		defer a.erase(bkt,256)
		cur := bkt.Cursor()
		
		k,_ := cur.Seek(expb[:])
		if len(k)<8 {
			k,_ = cur.Last()
		}
		if len(k)<8 { goto createfile }
		fi = binary.BigEndian.Uint64(k)
		
		// Check, whether or not the current values ar ok.
		if fi >= expireAt { goto done }
		if (fi-secDay) <= expireAt { goto done }
		
		// Current strategy: one file per day. Other variants are possible as well!
		
		createfile:
		
		fi = expireAt+secDay-1
		fi -= fi%secDay // Truncate the Time to a Day.
		
		// Store file ID.
		binary.BigEndian.PutUint64(expb[:],fi)
		return bkt.Put(expb[:],expb[:])
		
		done:
		return nil
	})
	//if err==nil { err = err2 }
	return
}
func (a *Allocator) AllocateTimeFile(expireAt uint64) (uint64,error) {
	/* Don't allow expired items to enter! */
	//fmt.Println(expireAt,"<=",current,":",expireAt <= current)
	if expireAt <= current { return 0,EFalse }
	if u,err := a.check(expireAt); err==nil {  return u,nil }
	return a.alloc(expireAt)
}
func (a *Allocator) Cleanup(n int){
	a.DB.Batch(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(allocator)
		if bkt==nil { return nil }
		a.erase(bkt,n)
		return nil
	})
}
func (a *Allocator) Comb() {
	dir,e := os.Open(a.Path)
	if e!=nil { return }
	m := make(map[uint64]bool)
	no := uint64(0)
	scans := []interface{}{no}
	for {
		names,e := dir.Readdirnames(128)
		if e!=nil { break }
		for _,name := range names {
			if len(name)<3 { continue }
			if name[:3]!="tf_" { continue }
			n,e := fmt.Sscanf(name,"tf_%016x",scans...)
			if e!=nil { continue }
			if n<1 { continue }
			if no<current { m[no] = true }
		}
	}
	for fi := range m {
		os.Remove(a.GetPath(fi))
	}
}


