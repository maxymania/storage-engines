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


package gistops

import "github.com/maxymania/storage-engines/anytree"
import "github.com/vmihailenco/msgpack"
import "sort"
import "bytes"
import "fmt"

var (
	E_SK_Format = fmt.Errorf("E_SK_Format")
)

func strPrefix(a,b []byte) int {
	if len(a)>len(b) { a = a[:len(b)] }
	for i,v := range a {
		if b[i]!=v { return i }
	}
	return len(a)
}
func strEqualize(a,b []byte) ([]byte,[]byte) {
	if len(a)>len(b) { a = a[:len(b)] } else if len(a)<len(b) { b = b[:len(a)] }
	return a,b
}
func strComp(a, b []byte, arb, brb bool) int {
	if arb||brb {
		res := bytes.Compare(strEqualize(a,b))
		if (res==0) && arb && (len(a)<len(b)) { res =  1 }
		if (res==0) && brb && (len(b)<len(a)) { res = -1 }
		return res
	}
	
	return bytes.Compare(a,b)
}

type internalStrKey struct {
	_msgpack struct{} `msgpack:",asArray"`
	Lowest []byte
	Highbeg uint
	Highest []byte
}

const (
	/* Strkey.Lowest == Strkey.Highest */
	SK_QuickMatch uint = 1<<iota
	SK_PrefixMatch
	SK_PrefixRangeMatch
	
	/* Eighter Prefix-Match or Quick-Match */
	iSK_Single = SK_QuickMatch|SK_PrefixMatch
	
	/* Eighter Prefix-Match or Prefix-Range-Match */
	iSK_PrefixMatch = SK_PrefixRangeMatch|SK_PrefixMatch
)

/*
A key data type for anytree's GiSTs that indexes (byte-)strings as a B-Tree-like data structure.

This key can represent individual strings, ranges, prefixes or prefix-ranges.
*/
type Strkey struct{
	Lowest []byte
	Highest []byte
	Flags uint
}
func StrkeyLiteral(l []byte) *Strkey {
	return &Strkey{l,l,SK_QuickMatch}
}
func StrkeyPrefix(l []byte) *Strkey {
	return &Strkey{l,l,SK_PrefixMatch}
}
func StrkeyRange(b,e []byte) *Strkey {
	return &Strkey{b,e,0}
}
func StrkeyPrefixRange(b,e []byte) *Strkey {
	return &Strkey{b,e,SK_PrefixRangeMatch}
}

func (s *Strkey) String() string {
	suffix := "$"
	if s.hasFlags(iSK_PrefixMatch) { suffix = ".*" }
	if s.hasFlags(iSK_Single) {
		return fmt.Sprintf("Single(^%q%s)",s.Lowest,suffix)
	}
	return fmt.Sprintf("Range(^%q$ ... ^%q%s)",s.Lowest,s.Highest,suffix)
}
func (s *Strkey) hasFlags(i uint) bool { return (s.Flags&i)!=0 }
func (s *Strkey) matchOne(str []byte,rb bool) bool {
	lowest  := strComp(s.Lowest,str,false,rb)
	highest := strComp(s.Highest,str,s.hasFlags(iSK_PrefixMatch),rb)
	
	return lowest <= 0 && highest >= 0
}

func (s *Strkey) Match(o *Strkey) bool {
	if s.hasFlags(iSK_Single) && o.hasFlags(iSK_Single) {
		if !bytes.Equal(strEqualize(s.Lowest,o.Lowest)) { return false }
		
		if len(o.Lowest)<len(s.Lowest) && !o.hasFlags(SK_PrefixMatch) { return false } /* O.isPrefix(S) but !O.isPrefixMatch() */
		if len(s.Lowest)<len(o.Lowest) && !s.hasFlags(SK_PrefixMatch) { return false } /* S.isPrefix(O) but !S.isPrefixMatch() */
		
		return true
	}
	
	if s.hasFlags(SK_QuickMatch) { return o.matchOne(s.Lowest,false) }
	if o.hasFlags(SK_QuickMatch) { return s.matchOne(o.Lowest,false) }
	
	if s.matchOne(o.Lowest,false) { return true }
	if s.matchOne(o.Highest,s.hasFlags(iSK_PrefixMatch)) { return true }
	if o.matchOne(s.Lowest,false) { return true }
	if o.matchOne(s.Highest,o.hasFlags(iSK_PrefixMatch)) { return true }
	
	return false
}
func (s *Strkey) Subset(o *Strkey) bool {
	if strComp(s.Lowest,o.Lowest,false,false) > 0 { return false }
	if strComp(s.Highest,o.Highest,s.hasFlags(iSK_PrefixMatch),o.hasFlags(iSK_PrefixMatch)) < 0 { return false }
	return true
}
func (s *Strkey) UnionInPlace(o *Strkey) {
	chlowest := false
	chhighest := false
	
	if strComp(s.Lowest,o.Lowest,false,false) > 0 { /* o.Lowest < s.Lowest */
		s.Lowest = o.Lowest
		chlowest = true
	}
	if strComp(s.Highest,o.Highest,s.hasFlags(iSK_PrefixMatch),o.hasFlags(iSK_PrefixMatch)) < 0 { /* s.Highest < o.Highest */
		s.Highest = o.Highest
		chhighest = true
	}
	
	if chlowest && chhighest {
		s.Flags = o.Flags
	} else if chlowest || chhighest {
		nf := uint(0)
		if	(s.hasFlags(iSK_PrefixMatch) && !chlowest) ||
			(o.hasFlags(iSK_PrefixMatch) && chhighest) {
			nf |= SK_PrefixRangeMatch
		}
		s.Flags = nf
	}
}

func (s *Strkey) EncodeMsgpack(dst *msgpack.Encoder) error {
	pref := strPrefix(s.Lowest,s.Highest)
	if s.hasFlags(iSK_Single) {
		err := dst.EncodeUint(uint64(s.Flags))
		if err!=nil { return err }
		err = dst.EncodeBytes(s.Lowest)
		return err
	}
	if pref==len(s.Lowest) && pref==len(s.Highest) {
		nf := SK_QuickMatch
		if s.hasFlags(iSK_PrefixMatch) { nf = SK_PrefixMatch }
		err := dst.EncodeUint(uint64(nf))
		if err!=nil { return err }
		err = dst.EncodeBytes(s.Lowest)
		return err
	}
	
	isk := new(internalStrKey)
	isk.Lowest = s.Lowest
	isk.Highbeg = uint(pref)
	isk.Highest = s.Highest[pref:]
	err := dst.EncodeUint(uint64(s.Flags))
	if err!=nil { return err }
	return dst.Encode(isk)
}
func (s *Strkey) DecodeMsgpack(src *msgpack.Decoder) error {
	flags,err := src.DecodeUint()
	if err!=nil { return err }
	if (flags&iSK_Single)!=0 {
		str,err := src.DecodeBytes()
		if err!=nil { return err }
		s.Lowest = str
		s.Highest = str
		s.Flags = flags
		return nil
	}
	isk := new(internalStrKey)
	err = src.Decode(isk)
	if err!=nil { return err }
	s.Lowest = isk.Lowest
	if uint(len(isk.Lowest))<isk.Highbeg { return E_SK_Format }
	if isk.Highbeg==0 {
		s.Highest = isk.Highest
	} else {
		s.Highest = make([]byte,0,isk.Highbeg+uint(len(isk.Highest)))
		s.Highest = append(s.Highest,isk.Lowest[:isk.Highbeg]...)
		s.Highest = append(s.Highest,isk.Highest...)
	}
	s.Flags = flags
	
	return nil
}

type tStr struct{}

func (t tStr) KT() interface{} {
	return new(Strkey)
}

func (t tStr) Consistent(E, q interface{}) bool {
	a := E.(*Strkey)
	b := q.(*Strkey)
	
	return a.Match(b)
}

// Union(P)=>R so that (r → p for all p in P)
func (t tStr) Union(P []anytree.Element) interface{} {
	nr := new(Strkey)
	*nr = *(P[0].P.(*Strkey))
	for _,e := range P[1:] {
		x := e.P.(*Strkey)
		nr.UnionInPlace(x)
	}
	return nr
}

// Penalty(E¹,E²): Calculates the penalty of merging E¹ and E² under E¹.
func (t tStr) Penalty(E1,E2 interface{}) float64 {
	a1 := E1.(*Strkey)
	a2 := E2.(*Strkey)
	if a1.Subset(a2) { return 0 } /* Full encosure */
	if a1.Match(a2) { return 1 } /* At least overlap.  */
	if bytes.Compare(a1.Lowest,a2.Lowest) > 0 { return 3 }
	return 2
}

// PickSplit(P)->P¹,P²
func (t tStr) PickSplit(P []anytree.Element) (P1,P2 []anytree.Element) {
	sort.Slice(P,anytree.SortOrder(t.Compare,P))
	return P[:len(P)/2],P[len(P)/2:]
}

// Compare (E¹,E²):
// Returns <0 if E¹ preceeds E²
//         >0 if E² preceeds E¹
//          0 otherwise.
//
// This function is optional. If this function is given, IsOrdered is true automatically.
func (t tStr) Compare(E1,E2 interface{}) int {
	a1 := E1.(*Strkey).Lowest
	a2 := E2.(*Strkey).Lowest
	
	return bytes.Compare(a1,a2)
}

/*
An operator class for *Strkey.
*/
func StrkeyOps() *anytree.GistOps {
	var t tStr
	return &anytree.GistOps{
		KT:t.KT,
		Consistent:t.Consistent,
		Union:t.Union,
		Penalty:t.Penalty,
		PickSplit:t.PickSplit,
		Compare:t.Compare,
	}
}

