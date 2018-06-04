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


package anytree

//import "github.com/cznic/file"
import "github.com/vmihailenco/msgpack"
//import "github.com/vmihailenco/msgpack/codes"
//import "encoding/binary"
//import "context"

type KeyType func() interface{}

type Pair struct {
	K interface{}
	V Buffer
}

type Element struct{
	P interface{}
	Data []byte
	Ptr int64
}
func (e *Element) EncodeMsgpack(dst *msgpack.Encoder) error {
	return dst.Encode(e.P,e.Data,e.Ptr)
}
func (e *Element) DecodeMsgpack(src *msgpack.Decoder) error {
	return src.Decode(e.P,&e.Data,&e.Ptr)
}

var _ msgpack.CustomEncoder = (*Element)(nil)
var _ msgpack.CustomDecoder = (*Element)(nil)


type Node struct{
	KT KeyType
	Elems []Element
}
func (n *Node) EncodeMsgpack(dst *msgpack.Encoder) error {
	err := dst.EncodeInt(int64(len(n.Elems)))
	if err!=nil { return err }
	for i := range n.Elems {
		err = dst.Encode(&(n.Elems[i]))
		if err!=nil { return err }
	}
	return nil
}

func (n *Node) DecodeMsgpack(src *msgpack.Decoder) error {
	l,e := src.DecodeInt()
	if e!=nil { return e }
	if cap(n.Elems)<l { n.Elems = make([]Element,l) } else { n.Elems = n.Elems[:l] }
	for i := range n.Elems {
		n.Elems[i].P = n.KT()
		e = src.Decode(&(n.Elems[i]))
		if e!=nil { return e }
	}
	return nil
}

var _ msgpack.CustomEncoder = (*Node)(nil)
var _ msgpack.CustomDecoder = (*Node)(nil)

/* Operations for GiST indeces. */
type GistOps struct{
	KT KeyType
	
	// Consistent(p,q)->bool:
	// Given the key E, and the predicate q, Consistent returns false,
	// if it can be guaranteed, that the subset of E matched by q is empty.
	Consistent func(E, q interface{}) bool
	
	// Union(P)=>R so that (r → p for all p in P)
	Union func(P []Element) interface{}
	// Compress(E) and Decompress(E) is handled by the (de)serialization
	
	// Penalty(E¹,E²): Calculates the penalty of merging E¹ and E² under E¹.
	Penalty func(E1,E2 interface{}) float64
	
	// PickSplit(P)->P¹,P²
	PickSplit func(P []Element) (P1,P2 []Element)
	
	// Compare (E¹,E²):
	// Returns <0 if E¹ preceeds E²
	//         >0 if E² preceeds E¹
	//          0 otherwise.
	//
	// This function is optional. If this function is given, IsOrdered is true automatically.
	Compare func(E1,E2 interface{}) int
}
func (o *GistOps) IsOrdered() bool { return o.Compare!=nil }
func (o *GistOps) SortOrder(e []Element) func(i,j int) bool {
	return func(i,j int) bool {
		return o.Compare(e[i].P,e[j].P) < 0
	}
}
func SortOrder(compare func(E1,E2 interface{}) int, e []Element) func(i,j int) bool {
	return func(i,j int) bool {
		return compare(e[i].P,e[j].P) < 0
	}
}

