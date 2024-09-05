package mutex

import (
	"container/heap"
	"fmt"
)

type MessageType int

const (
	MsgAcquire MessageType = iota
	MsgAddRequest
	MsgAckRequest
	MsgRelease
	MsgRemoveRequest
	MsgTriggerAcquire
)

type Input struct {
	Message *Message
	ErrChan chan error
}

type Output struct {
	Messages Messages
}

type Messages []*Message

func (m Messages) Search(msg *Message) int {
	left, right := 0, len(m)-1
	for left <= right {
		i := (left + right) / 2
		if m[i].LessThan(msg) {
			left = i + 1
		} else {
			right = i - 1
		}
	}
	return left
}

func (m Messages) Insert(i int, msg *Message) {
	m = append(m, &Message{})
	copy(m[i+1:], m[i:])
	m[i] = msg
}

func (m Messages) Remove(i int) {
	copy(m[i:], m[i+1:])
	m = m[:len(m)-1]
}

type Message struct {
	Timestamp
	To   int64
	Type MessageType
	Data any
}

func (m *Message) Equal(msg *Message) bool {
	return m.Timestamp == msg.Timestamp
}

func (m *Message) LessThan(msg *Message) bool {
	return m.Timestamp.LessThan(msg.Timestamp)
}

func (m *Message) MustType(t MessageType) {
	if m.Type != t {
		e := fmt.Errorf("unexpected message type (exp %v, got %v)", t, m.Type)
		panic(e)
	}
}

type Timestamp struct {
	Time int64
	From int64
}

func (t *Timestamp) LessThan(ts Timestamp) bool {
	return t.Time < ts.Time || (t.Time == ts.Time && t.From < ts.From)
}

type peerMsgs []*Message

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
	pm = append(pm, x.(*Message))
}

func (pm peerMsgs) Peek() *Message {
	return pm[0]
}

type outMsgs []peerMsgs

func newOutMsgs(n int64) outMsgs {
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

func (om outMsgs) Peek() (msgs []*Message) {
	for _, pm := range om {
		if pm.Len() > 0 {
			msgs = append(msgs, pm.Peek())
		}
	}
	return msgs
}

func (om outMsgs) Pop() (msgs []*Message) {
	for _, pm := range om {
		if pm.Len() > 0 {
			msg := heap.Pop(pm).(*Message)
			msgs = append(msgs, msg)
		}
	}
	return msgs
}

func (om outMsgs) Push(msg *Message, to int64) {
	heap.Push(om[to], msg)
}
