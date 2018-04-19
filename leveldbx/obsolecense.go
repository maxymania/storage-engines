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

/*
Auto-Expire offers a function, that decides, whether or not a record should be dropped.
*/
type AutoExpire interface{
	// This method is called on every value, deciding whether or not to drop it.
	//
	// If the method returns true, the database will retain the value, otherwise
	// it will be dropped.
	//
	// Good practices too implement it:
	//  - Perform as fast as possible. The slower the function works, the slower
	//    the compaction will be, an the whole database will be sluggish.
	//  - If in doubt, return true.
	Retain(b []byte) bool
}

type defaultAutoExpire struct{}
func (defaultAutoExpire) Retain(b []byte) bool { return true }
func getAutoExpire(a AutoExpire) AutoExpire {
	if a==nil { return defaultAutoExpire{} }
	return a
}
