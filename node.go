package mutex

import (
	"context"
	"sync/atomic"
)

type Node struct {
	Peers      int64
	ID         int64
	InChan     chan InMessage
	OutChan    chan []Message
	OutAckChan chan struct{}

	clock    Clock
	requests Requests
	request  *Request
	acquired uint64
	granted  uint64
	acks     []*RequestAck
	outMsgs  outMsgs
}

func NewNode(id, peers int64, clock Clock) *Node {
	n := &Node{
		Peers:      peers,
		ID:         id,
		InChan:     make(chan InMessage),
		OutChan:    make(chan []Message),
		OutAckChan: make(chan struct{}),
		clock:      clock,
		requests:   make(Requests, 0, peers),
		acks:       make([]*RequestAck, peers),
		outMsgs:    make(outMsgs, peers),
	}
	return n
}

func (n *Node) Acquired() bool {
	return atomic.LoadUint64(&n.acquired) == 1
}

func (n *Node) Granted() bool {
	return atomic.LoadUint64(&n.granted) == 1
}

func (n *Node) Acquire(ctx context.Context) error {
	return n.WaitProcessed(ctx, Message{Type: MsgAcquire})
}

func (n *Node) Release(ctx context.Context) error {
	return n.WaitProcessed(ctx, Message{Type: MsgRelease})
}

func (n *Node) WaitProcessed(ctx context.Context, msg Message) error {
	inMsg := InMessage{Message: msg, ErrChan: make(chan error)}
	select {
	case n.InChan <- inMsg:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-inMsg.ErrChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (n *Node) Run() {
	var (
		outCh    chan []Message
		outAckCh chan struct{}
		outMsgs  []Message
	)

	for {
		outCh = nil
		if outAckCh == nil && n.outMsgs.Len() > 0 {
			outMsgs = n.outMsgs.Peek()
			outCh = n.OutChan
		}

		select {
		case inMsg := <-n.InChan:
			err := n.process(inMsg.Message)
			if inMsg.ErrChan != nil {
				inMsg.ErrChan <- err
				close(inMsg.ErrChan)
			}
		case outCh <- outMsgs:
			n.outMsgs.Pop()
			outAckCh = n.OutAckChan
		case <-outAckCh:
			outAckCh = nil
		}
	}

}

func (n *Node) process(msg Message) error {
	n.clock.Sync(msg.Time)

	switch msg.Type {
	case MsgAcquire:
		if !n.Acquired() {
			req := n.acquire()
			n.bcastAdd(req)
		}
	case MsgAddRequest:
		req := msg.Data.(*Request)
		ok := n.addRequest(req)
		if ok {
			n.ack(req)
		}
	case MsgAckRequest:
		ack := msg.Data.(*RequestAck)
		n.addAck(ack)
	case MsgRelease:
		if n.Acquired() {
			req := n.release()
			n.bcastRemove(req)
		}
	case MsgRemoveRequest:
		req := msg.Data.(*Request)
		n.removeRequest(req)
	}

	return nil
}

func (n *Node) acquire() (req *Request) {
	req = &Request{
		Timestamp: Timestamp{Time: n.clock.Assign(), From: n.ID},
	}
	i := n.requests.Search(req)
	n.requests.Insert(i, req)
	atomic.StoreUint64(&n.acquired, 1)
	n.request = req
	return req
}

func (n *Node) addRequest(req *Request) (added bool) {
	i := n.requests.Search(req)
	if !n.requests[i].Equal(req) {
		n.requests.Insert(i, req)
		added = true
	}
	return added
}

func (n *Node) ack(req *Request) {
	ack := &RequestAck{}
	ack.Timestamp = Timestamp{Time: n.clock.Assign(), From: n.ID}
	ack.Request = *req
	msg := Message{
		Timestamp: ack.Timestamp,
		Type:      MsgAckRequest,
		Data:      ack,
	}
	n.send(msg, req.From)
}

func (n *Node) addAck(ack *RequestAck) {
	i := n.requests.Search(&ack.Request)
	if n.requests[i].Equal(&ack.Request) {
		if n.acks[ack.From] == nil {
			n.acks[ack.From] = ack
			n.updateGranted()
		}
	}
}

func (n *Node) updateGranted() {
	if len(n.requests) == 0 || !n.requests[0].Equal(n.request) {
		return
	}
	for i, ack := range n.acks {
		if i != int(n.ID) && ack == nil {
			return
		}
	}
	atomic.StoreUint64(&n.granted, 1)
}

func (n *Node) release() (req *Request) {
	req = n.request
	i := n.requests.Search(req)
	n.requests.Remove(i)
	atomic.StoreUint64(&n.granted, 0)
	atomic.StoreUint64(&n.acquired, 0)
	n.acks = make([]*RequestAck, n.Peers)
	n.request = nil
	return req
}

func (n *Node) removeRequest(req *Request) {
	i := n.requests.Search(req)
	if n.requests[i].Equal(req) {
		n.requests.Remove(i)
		n.updateGranted()
	}
}

func (n *Node) bcastAdd(req *Request) {
	msg := Message{
		Timestamp: req.Timestamp,
		Type:      MsgAddRequest,
		Data:      req,
	}
	n.bcast(msg)
}

func (n *Node) bcastRemove(req *Request) {
	msg := Message{
		Timestamp: Timestamp{Time: n.clock.Assign(), From: n.ID},
		Type:      MsgRemoveRequest,
		Data:      req,
	}
	n.bcast(msg)
}

func (n *Node) bcast(msg Message) {
	for i := int64(0); i < n.Peers; i++ {
		if i != n.ID {
			n.send(msg, i)
		}
	}
}

func (n *Node) send(msg Message, to int64) {
	msg.To = to
	n.outMsgs.Push(msg, to)
}
