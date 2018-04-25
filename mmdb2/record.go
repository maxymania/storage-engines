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


package mmdb2

import "encoding/binary"
import "github.com/dgryski/go-farm"
import "errors"

var EKeyTooLarge = errors.New("EKeyTooLarge")
var EValueTooLarge = errors.New("EValueTooLarge")

var EInvalidRecord = errors.New("EInvalidRecord")
var EShortRecord = errors.New("EShortRecord")


var space64 [8]byte

/*
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
*/

func irecordLength(key, value int) int {
	return 8 + key + value
}

func recordLength(key, value int) (int,error) {
	if key>0xFFFF { return 0,EKeyTooLarge }
	if value>0xFFFF { return 0,EValueTooLarge }
	return 8 + key + value, nil
}


/*
Encodes a key-value record into the given buffer.
If the buffer is nil or has insufficient capacity, it will be reallocated.
*/
func EncodeRecord(buffer, key, value []byte) (record []byte,err error) {
	if len(key  )>0xFFFF { err = EKeyTooLarge  ; return  }
	if len(value)>0xFFFF { err = EValueTooLarge; return  }
	
	record = buffer[:0]
	record = append(record,space64[:]...)
	record = append(record,key...)
	record = append(record,value...)
	
	binary.BigEndian.PutUint16(record[4:],uint16(len(key  )))
	binary.BigEndian.PutUint16(record[6:],uint16(len(value)))
	
	binary.BigEndian.PutUint32(record,farm.Fingerprint32(record[4:]))
	
	return
}

func encodeRecordInto(record, key, value []byte) {
	if len(record) != irecordLength(len(key),len(value)) { panic("len(record) != irecordLength(len(key),len(value))") }
	
	copy(record,space64[:])
	copy(record[8:],key)
	copy(record[8+len(key):],value)
	
	binary.BigEndian.PutUint16(record[4:],uint16(len(key  )))
	binary.BigEndian.PutUint16(record[6:],uint16(len(value)))
	
	binary.BigEndian.PutUint32(record,farm.Fingerprint32(record[4:]))
}

/*
Decodes a key-value record from the given slice. The record must be at the beginning
of the slice. If not, do this:
 DecodeRecord(aByteSlice[offset:])

The returned slices, 'record', 'key' and 'value' are sub-slices of the given argument,
thus avoid memory copying.
*/
func DecodeRecord(start []byte) (record, key, value []byte, err error) {
	if len(start)<8 { err = EShortRecord; return }
	checksum := binary.BigEndian.Uint32(start)
	
	keyl := int(binary.BigEndian.Uint16(start[4:]))
	vall := int(binary.BigEndian.Uint16(start[6:]))
	
	reclen := keyl+vall+8
	if len(start)<reclen { err = EShortRecord; return }
	
	record = start[:reclen]
	
	if checksum != farm.Fingerprint32(record[4:]) { err = EInvalidRecord; record = nil }
	
	key = record[8:8+keyl]
	value = record[8+keyl:]
	
	return
}

func getKeylen(record []byte) uint16 {
	return binary.BigEndian.Uint16(record[4:])
}
