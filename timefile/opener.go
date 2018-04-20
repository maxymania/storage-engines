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

/*
Efficient Key-Value/Key-BLOB Storage with automatic expiration.

The storage engine "timefile" partially resembles badger (https://github.com/dgraph-io/badger)
and the WiscKey paper (https://www.usenix.org/system/files/conference/fast16/fast16-papers-lu.pdf)
in that it seperates the Values (BLOBs) from the keys, and that it keeps the keys in a
LSM tree.

It differs however significantly, becaus it does not feature a Garbage collection for the value log.
A value log (called a time-file) is only appended to, never compacted, but only deleted.
These time-files are named after the time, at which they will expire. The format is "tf_" followed
by a 16-digit hexadecimal representation of the expiration-timestamp in unix format
(number of seconds elapsed since January 1, 1970 UTC). If an BLOB is inserted, it must carry the
expiration date with it (in unix format). The BLOB is then stored in a time-file that expires not
earlier, but possibly later, than the BLOB. Once a time-file expires, the BLOBs within it are not
longer accessible. The expired time-files are periodically swept, typically on every creation of
a new time-file.

Facts:
 - Unlike badger or WiscKey, timefile is not designed for SSDs
 - Timefile is designed to store millions (!) of GB
 - With timefile, you can not overwrite BLOBs.
 - With timefile, you can not delete BLOBs¹.

¹ The ability to "delete" BLOBs by deleting their keys is planned, but freeing space by deleting
BLOBs prior to their expiration won't be supported!

Timefile utilizes two different embedded key-value databases: bolt, a LMDB-workalike written in Go
(github.com/boltdb/bolt) and LevelDB-go (github.com/syndtr/goleveldb/leveldb) with modifications
(github.com/maxymania/storage-engines/leveldbx).
Bolt is used to keep track of the timefiles. The reason, why bolt is choosen is, because bolt
offers reliable transactional consistency.
The modified LevedDB-go is used to assign the keys to the positions of their values
(timefile-ID,offset,length). The reason, why LevelDB is choosen is, that LevelDB is an LSM tree,
and by its nature, the database is compacted by copying the entries pairs stored within.
This means, entries refering expired time-files can be dropped on the fly during compaction.
This can't be efficiently done with B-Trees, the index-structure bolt is using.
*/
package timefile

import (
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/maxymania/storage-engines/leveldbx"
	"github.com/boltdb/bolt"

	"github.com/syndtr/goleveldb/leveldb/opt"
	"path/filepath"
)

type Options struct{
	Index *opt.Options  // Options for the index DB, or nil for default.
	Alloc *bolt.Options // Options for the allocator DB, or nil for default.
	Files int           // Approximate number of open files, or 0 for default.
}

var defOptions = Options{
	Index: &opt.Options{
		NoSync: true,
	},
}

func OpenStore(base string,opt *Options) (*Store,error){
	var lopt Options
	s := new(Store)
	alloc := filepath.Join(base,"alloc.tf")
	index := filepath.Join(base,"index")
	
	if opt!=nil { lopt = *opt }
	
	if lopt.Index==nil { lopt.Index = defOptions.Index }
	
	b,e := bolt.Open(alloc,0644, lopt.Alloc)
	if e!=nil { return nil,e }
	l,e := leveldb.OpenFile(index, lopt.Index, AutoExpire{})
	if errors.IsCorrupted(e) {
		l,e = leveldb.RecoverFile(index, lopt.Index, AutoExpire{})
	}
	if e!=nil { return nil,e }
	
	s.Alloc = new(Allocator)
	s.Alloc.Path = base
	s.Alloc.DB = b
	s.DB = l
	s.Init(lopt.Files)
	
	return s,e
}

