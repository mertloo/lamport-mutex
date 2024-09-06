package mutex

import (
	"fmt"
	"strings"
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

func (t MessageType) String() string {
	switch t {
	case MsgAcquire:
		return "Acquire"
	case MsgAddRequest:
		return "AddRequest"
	case MsgAckRequest:
		return "AckRequest"
	case MsgRelease:
		return "Release"
	case MsgRemoveRequest:
		return "RemoveRequest"
	case MsgTriggerAcquire:
		return "TriggerAcquire"
	default:
		return "Unknown"
	}
}

type Input struct {
	Message *Message
	ErrChan chan error
}

type Output struct {
	Messages Messages
}

type Messages []*Message

func (ms *Messages) String() (str string) {
	parts := make([]string, len(*ms))
	for i, msg := range *ms {
		parts[i] = msg.String()
	}
	str = strings.Join(parts, "|")
	return str
}

type Requests Messages

func (rqs *Requests) Search(msg *Message) (idx int, exists bool) {
	ms := *rqs
	left, right := 0, len(ms)-1
	for left <= right {
		i := (left + right) / 2
		if ms[i].LessThan(msg) {
			left = i + 1
		} else {
			right = i - 1
		}
	}
	idx = left
	if idx >= 0 && idx < len(ms) && msg.Equal(ms[idx]) {
		exists = true
	}
	return idx, exists
}

func (rqs *Requests) Insert(msg *Message) (inserted bool) {
	i, exists := rqs.Search(msg)
	if exists {
		return false
	}
	ms := *rqs
	ms = append(ms, &Message{})
	copy(ms[i+1:], ms[i:])
	ms[i] = msg
	*rqs = ms
	return true
}

func (rqs *Requests) Remove(msg *Message) (removed bool) {
	i, exists := rqs.Search(msg)
	if !exists {
		return false
	}
	ms := *rqs
	copy(ms[i:], ms[i+1:])
	ms = ms[:len(ms)-1]
	*rqs = ms
	return true
}

type Message struct {
	Timestamp
	To   int64
	Type MessageType
	Data any
}

func (m *Message) String() (str string) {
	str = "nil"
	if m != nil {
		str = fmt.Sprintf("time=%d,from=%d,to=%d,type=%s",
			m.Time, m.From, m.To, m.Type)
	}
	return str
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

type outMsgs []Messages

func newOutMsgs(peers int64) outMsgs {
	om := make(outMsgs, peers)
	for i := range om {
		om[i] = make(Messages, 0)
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
