/*
This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

In jurisdictions that recognize copyright laws, the author or authors
of this software dedicate any and all copyright interest in the
software to the public domain. We make this dedication for the benefit
of the public at large and to the detriment of our heirs and
successors. We intend this dedication to be an overt act of
relinquishment in perpetuity of all present and future rights to this
software under copyright law.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
OTHER DEALINGS IN THE SOFTWARE.

For more information, please refer to <http://unlicense.org/>
*/

package leveldb

import "github.com/syndtr/goleveldb/leveldb/iterator"

type expireIterator struct {
	iterator.Iterator
	aexp AutoExpire
}
func (ei *expireIterator) First() (b bool) {
	b = ei.Iterator.First()
	
	for b && !ei.aexp.Retain(ei.Value()) {
		b = ei.Iterator.Next()
	}
	return
}
func (ei *expireIterator) Next() (b bool) {
	b = ei.Iterator.Next()
	
	for b && !ei.aexp.Retain(ei.Value()) {
		b = ei.Iterator.Next()
	}
	return
}
func (ei *expireIterator) Seek(key []byte) (b bool) {
	b = ei.Iterator.Seek(key)
	
	for b && !ei.aexp.Retain(ei.Value()) {
		b = ei.Iterator.Next()
	}
	return
}
func (ei *expireIterator) Last() (b bool) {
	b = ei.Iterator.Last()
	
	for b && !ei.aexp.Retain(ei.Value()) {
		b = ei.Iterator.Prev()
	}
	return
}
func (ei *expireIterator) Prev() (b bool) {
	b = ei.Iterator.Prev()
	
	for b && !ei.aexp.Retain(ei.Value()) {
		b = ei.Iterator.Prev()
	}
	return
}


