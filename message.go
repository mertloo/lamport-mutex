package mutex

import (
	"container/heap"
)

type MessageType int

const (
	MsgAcquire MessageType = iota
	MsgAddRequest
	MsgAckRequest
	MsgRelease
	MsgRemoveRequest
)

type InMessage struct {
	Message
	ErrChan chan error
}

type Message struct {
	Timestamp
	To   int64
	Type MessageType
	Data any
}

type peerMsgs []Message

func (pm peerMsgs) Len() int {
	return len(pm)
}

func (pm peerMsgs) Less(i, j int) bool {
	return pm[i].Timestamp.LessThan(pm[j].Timestamp)
}

func (pm peerMsgs) Swap(i, j int) {
	pm[i], pm[j] = pm[j], pm[i]
}

func (pm peerMsgs) Pop() any {
	n := len(pm)
	msg := pm[n-1]
	pm = pm[:n-1]
	return msg
}

func (pm peerMsgs) Push(x any) {
	pm = append(pm, x.(Message))
}

func (pm peerMsgs) Peek() Message {
	return pm[0]
}

type outMsgs []peerMsgs

func newOutMsgs(n int) outMsgs {
	om := make(outMsgs, n)
	for i := range om {
		om[i] = make(peerMsgs, 0)
	}
	return om
}

func (om outMsgs) Len() (n int) {
	for _, pm := range om {
		n += pm.Len()
	}
	return n
}

func (om outMsgs) Peek() (msgs []Message) {
	for _, pm := range om {
		if pm.Len() > 0 {
			msgs = append(msgs, pm.Peek())
		}
	}
	return msgs
}

func (om outMsgs) Pop() (msgs []Message) {
	for _, pm := range om {
		if pm.Len() > 0 {
			msg := heap.Pop(pm).(*Message)
			msgs = append(msgs, *msg)
		}
	}
	return msgs
}

func (om outMsgs) Push(msg Message, to int64) {
	heap.Push(om[to], msg)
}
