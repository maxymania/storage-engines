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

A memory-mapped, log-structured database with in-memory search tree.
As in-memory search tree, eighter Red-Black-Tree, AVL-Tree or B-Tree can
be choosen.
All keys and values are stored within the memory-mapped file, the on-heap
search tree references the keys and values within the mmap'ed file rather
than maintaining an on-heap copy of them. This contributes to a low memory
consumption (ignoring the mmap'ed file) in comparison to other in-memory
indeces.

The file (-region) organization:
 +-------+-----------------+----------------+
 | hdr[] | Log (Records).. | unused....     |
 +-------+-----------------+----------------+
      |                    ^                ^
      V                    |                |
 max(hdr[])----------------/              Size


The record format:
 +========+==========+=========================+
 | TYPE   | NUM BYTES| Contents                |
 +========+==========+=========================+
 | uint32 |        4 | farmhash32(record[4:])  |
 +--------+----------+-------------------------+
 | uint16 |        2 | key_sz                  |
 +--------+----------+-------------------------+
 | uint16 |        2 | value_sz                |
 +--------+----------+-------------------------+
 | bytes  |   key_sz | Key...                  |
 +--------+----------+-------------------------+
 | bytes  | value_sz | Value...                |
 +--------+----------+-------------------------+

There are two indexing types, that can be used:
 - T_Sorted
 - T_Hashed

If T_Sorted is choosen, the in-memory search tree is ordered by the keys.
If T_Hashed is choosen, the search tree is orderes by the tuple:
 {Farmhash32(Key), Key}
This means, the tree is primarily ordered by the hash value and secondarily
ordered by the String making up the key. The benefit of this is, that a
string-comparison only takes place, if the hashes are equal (which indicates
eighter a hash-collision or equal strings).
As - on a cold cache - string comparisons cause IO (via page-fault), this can
speed-up the lookups considerably.

Third party go libraries used:
 - github.com/dgryski/go-farm
 - github.com/edsrzf/mmap-go
 - github.com/emirpasic/gods

*/
package mmdb
