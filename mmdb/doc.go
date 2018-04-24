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

A memory-mapped, log-structured database with in-memory search tree (Red-Black,
AVL or B-Tree).

The file organization:
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

Third party go libraries used:
 - github.com/dgryski/go-farm
 - github.com/edsrzf/mmap-go
 - github.com/emirpasic/gods

*/
package mmdb
