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


package timefiledist

//import "github.com/byte-mug/golibs/bufferex"
import "github.com/vmihailenco/msgpack"
import rbt "github.com/emirpasic/gods/trees/redblacktree"
import "github.com/emirpasic/gods/utils"
import "github.com/maxymania/storage-engines/timefiledist/navigator"

import "sync"
import "net"
import "errors"
import "time"

import "bytes"

var ERingEmpty = errors.New("Ring Empty")

type metadataBlock struct{
	_msgpack struct{} `msgpack:",asArray"`
	Position uint64
	Port int
}

type Notify struct{
	C *Client
	Start,Limit uint64
}

type Landscape struct{
	m sync.RWMutex
	Map   map[string]*Node
	Clnt  map[string]*Client
	Ring  *rbt.Tree
	Ntfr  chan Notify
}
func (l *Landscape) Init() {
	l.Map  = make(map[string]*Node)
	l.Clnt = make(map[string]*Client)
	l.Ring = rbt.NewWith(utils.UInt64Comparator)
	l.Ntfr = make(chan Notify,128)
}
func (l *Landscape) remove(n *Node) {
	blk := new(metadataBlock)
	if msgpack.Unmarshal(n.Meta,blk)!=nil { return }
	l.Ring.Remove(blk.Position)
	start := navigator.FloorRing(l.Ring,blk.Position)
	if start==nil { return }
	end := navigator.NextRing(start)
	
	var ntf Notify
	ntf.C = start.Value.(*Client)
	ntf.Start = start.Key.(uint64)
	ntf.Limit = end.Key.(uint64)
	/* blk.Position */
	l.Ntfr <- ntf
}
func (l *Landscape) insert(n *Node) {
	blk := new(metadataBlock)
	if msgpack.Unmarshal(n.Meta,blk)!=nil { return }
	addr := net.TCPAddr{IP:n.Addr,Port:blk.Port}
	c := NewClient(n,addr.String())
	l.Clnt[n.Name] = c
	l.Ring.Put(blk.Position,c)
	
	start := navigator.FloorRing(l.Ring,blk.Position)
	if start==nil { return }
	end := navigator.NextRing(start)
	
	var ntf Notify
	ntf.C = start.Value.(*Client)
	ntf.Start = start.Key.(uint64)
	ntf.Limit = end.Key.(uint64)
	l.Ntfr <- ntf
}
func (l *Landscape) Enter(n *Node) {
	l.m.Lock(); defer l.m.Unlock()
	if on,ok := l.Map[n.Name]; ok {
		l.remove(on)
	}
	l.Map[n.Name] = n
	l.insert(n)
}
func (l *Landscape) Remove(n *Node) {
	l.m.Lock(); defer l.m.Unlock()
	if on,ok := l.Map[n.Name]; ok {
		l.remove(on)
		delete(l.Map,n.Name)
		delete(l.Clnt,n.Name)
	}
}
func (l *Landscape) find(u uint64) *rbt.Node {
	l.m.RLock(); defer l.m.RUnlock()
	return navigator.FloorRing(l.Ring,u)
}
func (l *Landscape) findExact(u uint64) (interface{},bool) {
	l.m.RLock(); defer l.m.RUnlock()
	return l.Ring.Get(u)
}
func (l *Landscape) Handle(m *Message) *Message {
	u := m.GetKeyHash()
	n := l.find(u)
	if n==nil { m.SetError(ERingEmpty); return m }
	switch string(m.Type) {
	case "find":
		m.Type = append(m.Type[:0],"lookup"...)
		err := n.Value.(*Client).Cli.DoDeadline(m,m,time.Now().Add(time.Second))
		if err!=nil { m.SetError(err) }
	/* This command is a bad idea here. */
	//case "put":
	//	m.Type = append(m.Type[:0],"write"...)
	//	err := n.Value.(*Client).Cli.DoDeadline(m,m,time.Now().Add(time.Second))
	//	if err!=nil { m.SetError(err) }
	case "get":
		o := AcquireMessage()
		defer o.ReleaseMessage()
		m.Type = append(m.Type[:0],"lookup|read"...)
		err := n.Value.(*Client).Cli.DoDeadline(m,o,time.Now().Add(time.Second))
		if err!=nil { m.SetError(err) ; return m }
		if !o.Ok { m.AssignResp(o); return m }
		
		dec := msgpack.NewDecoder(bytes.NewReader(o.Payload))
		isYes,err := dec.DecodeBool()
		if err!=nil { m.SetError(err) ; return m }
		if isYes {
			dec.Decode(&m.Payload)
			m.Ok = true
			return m
		}
		err = dec.Decode(&u)
		if err!=nil { m.SetError(err) ; return m }
		
		nn,ok := l.findExact(u)
		
		if !ok { m.SetError(ERingEmpty); return m }
		
		m.Type = append(m.Type[:0],"read"...)
		
		err = nn.(*Client).Cli.DoDeadline(m,m,time.Now().Add(time.Second))
		if err!=nil { m.SetError(err) }
	default:
		return nil
	}
	return m
}
//func (l *Landscape) 
//func (l *Landscape) 



