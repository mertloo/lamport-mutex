package mutex

import (
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

type outMsgs []peerMsgs

func newOutMsgs(peers int64) outMsgs {
	om := make(outMsgs, peers)
	for i := range om {
		om[i] = make(peerMsgs, 0)
	}
	return om
}

func (om outMsgs) Len() (n int) {
	for _, pm := range om {
		n += len(pm)
	}
	return n
}

func (om outMsgs) Peek() (msgs []*Message) {
	for _, pm := range om {
		if len(pm) > 0 {
			msgs = append(msgs, pm[0])
		}
	}
	return msgs
}

func (om outMsgs) Pop() (msgs []*Message) {
	for i, pm := range om {
		if len(pm) > 0 {
			msgs = append(msgs, pm[0])
			om[i] = pm[1:]
		}
	}
	return msgs
}

func (om outMsgs) Push(msg *Message, to int64) {
	om[to] = append(om[to], msg)
}
