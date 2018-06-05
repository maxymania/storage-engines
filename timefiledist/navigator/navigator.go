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
Navigate from node to node in a Red-Black-Tree.
This package helps with the navigation within a red-black-tree of the following implementation:
	import "github.com/emirpasic/gods/trees/redblacktree"
*/
package navigator

import rbt "github.com/emirpasic/gods/trees/redblacktree"

/*
Explanation:

  B
 / \
A   C

A is the left child of B
C is the right child of B

B is the right parent of A
B is the left parent of C
*/




func Next(r *rbt.Node) *rbt.Node {
	/* Navigate to the right child.*/
	if r.Right!=nil {
		r = r.Right
		/* Navigate to the left-most node in the subtree. */
		for {
			l := r.Left
			if l==nil { break }
			r = l
		}
		return r
	}
	/* Get the first right parent. */
	for {
		p := r.Parent
		if p==nil { break }
		if p.Right==r { r = p; continue }
		return p
	}
	return nil
}

func Prev(r *rbt.Node) *rbt.Node {
	/* Navigate to the left child.*/
	if r.Left!=nil {
		r = r.Left
		/* Navigate to the right-most node in the subtree. */
		for {
			l := r.Right
			if l==nil { break }
			r = l
		}
		return r
	}
	/* Get the first left parent. */
	for {
		p := r.Parent
		if p==nil { break }
		if p.Left==r { r = p; continue }
		return p
	}
	return nil
}

func FloorRing(tree *rbt.Tree,key interface{}) *rbt.Node {
	if n,ok := tree.Floor(key); ok { return n }
	return tree.Right()
}
func CeilingRing(tree *rbt.Tree,key interface{}) *rbt.Node {
	if n,ok := tree.Ceiling(key); ok { return n }
	return tree.Left()
}

func NextRing(r *rbt.Node) *rbt.Node {
	/* Navigate to the right child.*/
	if r.Right!=nil {
		r = r.Right
		/* Navigate to the left-most node in the subtree. */
		for {
			l := r.Left
			if l==nil { break }
			r = l
		}
		return r
	}
	/* Get the first right parent. */
	for {
		p := r.Parent
		if p==nil {
			for {
				l := r.Left
				if l==nil { break }
				r = l
			}
			return r
		}
		if p.Right==r { r = p; continue }
		return p
	}
	return nil
}

func PrevRing(r *rbt.Node) *rbt.Node {
	/* Navigate to the left child.*/
	if r.Left!=nil {
		r = r.Left
		/* Navigate to the right-most node in the subtree. */
		for {
			l := r.Right
			if l==nil { break }
			r = l
		}
		return r
	}
	/* Get the first left parent. */
	for {
		p := r.Parent
		if p==nil {
			for {
				l := r.Right
				if l==nil { break }
				r = l
			}
			return r
		}
		if p.Left==r { r = p; continue }
		return p
	}
	return nil
}

