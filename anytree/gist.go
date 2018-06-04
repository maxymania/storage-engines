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
A simplistic GiST (Generalized Search Tree) implementation.

This library is rather intended as a building-block than a turn-key database.

http://db.cs.berkeley.edu/papers/vldb95-gist.pdf
*/
package anytree

import "context"
import "github.com/vmihailenco/msgpack"
import "sort"

/*
A GiST (Generalized Search Tree) implementation roughly based upon the GiST-Paper¹.

The Insertion, Sort and Deletion algorithms somehow deviate from the algorithms
from the Paper, otherwise, the functions supplied by the user are equivalent.

The Object wraps the Gist-Ops, a transaction and some arguments.

The GiST-struct is designed to carry a transaction within it, it must be
created one per transaction. However, as it is desireable to create it only
once and to create transaction object from that global GiST object, one can
derive copies from a global GiST object.
	gist := &anytree.GiST{Ops: yourOps ,TreeM: yourM ,Root: yourRoot }
	gist_tx := gist.WithTransaction(tx)

As the root node may be relocated or deleted, after an commit one must
adjust the pointer to the root node to whatever .Root is pointing.
This Position is usually stored somewhere else in the DB.

¹ http://db.cs.berkeley.edu/papers/vldb95-gist.pdf
*/
type GiST struct{
	_noinit struct{}
	*Tx
	Ops *GistOps
	TreeM int
	Root int64
}
func (t GiST) WithTransaction(tx *Tx) *GiST {
	t.Tx = tx
	return &t
}
func (t *GiST) write(ptr int64,data []byte) (int64,error) {
	err := t.Update(ptr,data)
	if err==nil {
		return ptr,nil
	} else if err==ErrUndersized {
		nptr,err := t.Insert(data)
		if err!=nil { return 0,err }
		t.Delete(ptr)
		return nptr,nil
	}
	return 0,err
}

func (t *GiST) sort(n *Node) {
	if !t.Ops.IsOrdered() { return }
	sort.Slice(n.Elems,t.Ops.SortOrder(n.Elems))
}

/* the insertion algorithm is also a bit simplified. */
func (t *GiST) chooseInsertGiST(ctx context.Context,node int64, p interface{}, val []byte) ([]Element,error) {
	if node<=16 { return nil,ErrInternal }
	if err := ctx.Err(); err!=nil { return nil,err }
	b,err := t.Read(node)
	defer b.Free()
	if err!=nil { return nil,err }
	nobj := &Node{KT:t.Ops.KT}
	err = msgpack.Unmarshal(b.Ptr,nobj)
	if err!=nil { return nil,err }
	sp := int64(0)
	sc := float64(0)
	si := -1
	
	for i,e := range nobj.Elems {
		if len(e.Data)>0 { continue }
		pen := t.Ops.Penalty(e.P,p)
		if sc>pen || si<0 {
			sp = e.Ptr
			sc = pen
			si = i
		}
	}
	
	toolong	:= false
	if si<0 {
		nobj.Elems = append(nobj.Elems,Element{P:p,Data:val})
		toolong = len(nobj.Elems)>t.TreeM
	} else {
		elems,err := t.chooseInsertGiST(ctx,sp,p,val)
		if err!=nil { return nil,err }
		
		if len(elems)==0 { panic("internal error") }
		nobj.Elems[si] = elems[0]
		
		if len(elems)>1 {
			nobj.Elems = append(nobj.Elems,elems[1:]...)
			toolong = len(nobj.Elems)>t.TreeM
		}
	}
	
	if toolong {
		nobj2 := &Node{KT:t.Ops.KT}
		nobj.Elems,nobj2.Elems = t.Ops.PickSplit(nobj.Elems)
		
		t.sort(nobj)
		t.sort(nobj2)
		
		data,err := msgpack.Marshal(nobj)
		if err!=nil { return nil,err }
		data2,err := msgpack.Marshal(nobj2)
		if err!=nil { return nil,err }
		nptr,err := t.write(node,data)
		if err!=nil { return nil,err }
		nptr2,err := t.Insert(data2)
		if err!=nil { return nil,err }
		
		return []Element{
			{P:t.Ops.Union(nobj.Elems),Ptr:nptr},
			{P:t.Ops.Union(nobj2.Elems),Ptr:nptr2},
		},nil
	} else {
		t.sort(nobj)
		
		data,err := msgpack.Marshal(nobj)
		if err!=nil { return nil,err }
		nptr,err := t.write(node,data)
		if err!=nil { return nil,err }
		
		return []Element{
			{P:t.Ops.Union(nobj.Elems),Ptr:nptr},
		},nil
	}
	panic("unreachable!")
}
func (t *GiST) InsertGiST(ctx context.Context,p interface{},val []byte) error {
	root := t.Root
	if root==0 {
		data,err := msgpack.Marshal(&Node{Elems:[]Element{{P:p,Data:val}}})
		if err!=nil { return err }
		nptr,err := t.Insert(data)
		t.Root = nptr
		if err!=nil { return err }
		return nil
	}
	elems,err := t.chooseInsertGiST(ctx,root,p,val)
	if err!=nil { return err }
	
	if len(elems)==0 { panic("internal error!") }
	if len(elems)==1 {
		t.Root = elems[0].Ptr
		return nil
	}
	
	/* Our root node has splitted. Create a new Parent. */
	
	data,err := msgpack.Marshal(&Node{Elems:elems})
	if err!=nil { return err }
	nptr,err := t.Insert(data)
	if err!=nil { return err }
	t.Root = nptr
	return nil
}

/* The search algorithm is a bit simplified. */
func (t *GiST) search(ctx context.Context,q interface{},node int64, ch chan <- Pair) error {
	if node<=16 { return ErrInternal }
	err := ctx.Err()
	if err!=nil { return err }
	b,err := t.Read(node)
	defer b.Free()
	if err!=nil { return err }
	nobj := &Node{KT:t.Ops.KT}
	err = msgpack.Unmarshal(b.Ptr,nobj)
	if err!=nil { return err }
	for _,e := range nobj.Elems {
		if !t.Ops.Consistent(e.P,q) { continue }
		if len(e.Data)>0 {
			select {
			case ch <- Pair{e.P,WrapBuffer(e.Data)}:
			case <- ctx.Done() : return ctx.Err()
			}
		} else if e.Ptr>=16 {
			err = t.search(ctx,q,e.Ptr,ch)
			if err!=nil { return err }
		}
	}
	return nil
}

func (t *GiST) Search(ctx context.Context,q interface{},ch chan <- Pair) error {
	root := t.Root
	defer close(ch)
	return t.search(ctx,q,root,ch)
}
func (t *GiST) SearchGiST(ctx context.Context,q interface{},ch chan <- Pair) error { return t.Search(ctx,q,ch) }

func (t *GiST) joinNodes(node int64,appnd []Element) (x_key []Element,x_err error) {
	if node<=16 { x_err = ErrInternal; return }
	b,err := t.Read(node)
	defer b.Free()
	if err!=nil { x_err = err; return }
	nobj := &Node{KT:t.Ops.KT}
	err = msgpack.Unmarshal(b.Ptr,nobj)
	nobj.Elems = append(nobj.Elems,appnd...)
	
	shouldSplit := len(nobj.Elems)>t.TreeM
	after := []Element(nil)
	if shouldSplit {
		nobj.Elems,after = t.Ops.PickSplit(nobj.Elems)
	}
	t.sort(nobj)
	
	data,err := msgpack.Marshal(nobj)
	if err!=nil { x_err = err; return }
	nptr,err := t.write(node,data)
	if err!=nil { x_err = err; return }
	
	x_key = []Element{{P:t.Ops.Union(nobj.Elems),Ptr:nptr}}
	
	if shouldSplit {
		xn := &Node{Elems:after}
		t.sort(xn)
		
		data,err = msgpack.Marshal(xn)
		if err!=nil { x_err = err; return }
		nptr,err = t.Insert(data)
		if err!=nil { x_err = err; return }
		x_key = append(x_key,Element{P:t.Ops.Union(after),Ptr:nptr})
	}
	return
}

/* The search algorithm is a bit simplified. */
func (t *GiST) delet(ctx context.Context,q interface{},node int64, prepend []Element, chk func(p Pair) bool) (x_key []Element,x_sub []Element,x_err error) {
	if node<=16 { x_err = ErrInternal; return }
	err := ctx.Err()
	if err!=nil { x_err = err; return }
	b,err := t.Read(node)
	defer b.Free()
	if err!=nil { x_err = err; return }
	nobj := &Node{KT:t.Ops.KT}
	err = msgpack.Unmarshal(b.Ptr,nobj)
	if err!=nil { x_err = err; return }
	var keys,prev,nelems []Element
	nelems = make([]Element,0,len(nobj.Elems)+len(prepend))
	
	for _,e := range prepend {
		nelems = append(nelems,e)
	}
	for _,e := range nobj.Elems {
		if !t.Ops.Consistent(e.P,q) {
			nelems = append(nelems,e)
			continue
		}
		
		if len(e.Data)>0 {
			if !chk(Pair{e.P,WrapBuffer(e.Data)}) {
				nelems = append(nelems,e)
			}
		} else if e.Ptr>=16 {
			keys,prev,err = t.delet(ctx,q,e.Ptr,prev,chk)
			if err!=nil { x_err = err; return }
			nelems = append(nelems,keys...)
		}
	}
	if len(prev)>0 {
		apper := true
		if len(nelems)>0 {
			lst := len(nelems)-1
			if nelems[lst].Ptr>=16 { /* This pointer should be valid, but safety first. */
				ne,err := t.joinNodes(nelems[lst].Ptr,prev)
				if err!=nil { x_err = err; return }
				nelems = append(nelems[:lst],ne...)
				apper = false
			}
		}
		if apper {
			data,err := msgpack.Marshal(&Node{Elems:prev})
			if err!=nil { x_err = err; return }
			nptr,err := t.Insert(data)
			nelems = append(nelems,Element{P:t.Ops.Union(prev),Ptr:nptr})
		}
	}
	if len(nelems)<2 {
		err = t.Delete(node)
		return nil,nelems,err
	}
	shouldSplit := len(nelems)>t.TreeM
	after := []Element(nil)
	if shouldSplit {
		nelems,after = t.Ops.PickSplit(nelems)
	}
	
	xn := &Node{Elems:nelems}
	t.sort(xn)
	
	data,err := msgpack.Marshal(xn)
	if err!=nil { x_err = err; return }
	nptr,err := t.write(node,data)
	if err!=nil { x_err = err; return }
	
	x_key = []Element{{P:t.Ops.Union(nelems),Ptr:nptr}}
	
	if shouldSplit {
		xn = &Node{Elems:after}
		t.sort(xn)
		
		data,err = msgpack.Marshal(xn)
		if err!=nil { x_err = err; return }
		nptr,err = t.Insert(data)
		if err!=nil { x_err = err; return }
		x_key = append(x_key,Element{P:t.Ops.Union(after),Ptr:nptr})
	}
	return
}
func (t *GiST) DeleteGiST(ctx context.Context,q interface{},chk func(p Pair) bool) error {
	root := t.Root
	if chk==nil { chk = func(Pair) bool { return true } }
	k,s,err := t.delet(ctx,q,root,nil,chk)
	if err!=nil { return err }
	if len(k)==0 { k = s }
	if len(k)==0 {
		t.Root = 0
		return nil
	}
	if len(k)==1 {
		t.Root = k[0].Ptr
		return nil
	}
	
	/* Lemma: len(k)>1 */
	data,err := msgpack.Marshal(&Node{Elems:k})
	if err!=nil { return err }
	nptr,err := t.Insert(data)
	if err!=nil { return err }
	
	t.Root = nptr
	return nil
}

