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

import "github.com/valyala/fastrpc"
import "github.com/vmihailenco/msgpack"

type pair struct{
	Key,Value []byte
}
func (p *pair) DecodeMsgpack(dec *msgpack.Decoder) error { return dec.Decode(&p.Key,&p.Value) }
func (p *pair) EncodeMsgpack(enc *msgpack.Encoder) error { return enc.Encode(p.Key,p.Value) }


type Client struct {
	Node *Node
	Cli fastrpc.Client
}
func NewClient(node *Node,addr string) *Client {
	c := new(Client)
	MakeConnection(&c.Cli)
	c.Node = node
	c.Cli.Addr = addr
	
	return c
}
func (c *Client) sendPairs(p []pair) {
	data,_ := msgpack.Marshal(p)
	o := AcquireMessage()
	o.SetPayload(data)
	o.Type = append(o.Type[:0],"index"...)
	o.Exp = 0
	c.Cli.SendNowait(o,func(req fastrpc.RequestWriter){
		req.(*Message).ReleaseMessage()
	})
}


