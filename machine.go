package mutex

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type Machine struct {
	Peers         int64
	ID            int64
	InChan        chan Input
	OutChan       chan Output
	OutAckChan    chan struct{}
	store         Store
	sender        Sender
	savedStateMtx sync.RWMutex
	savedState    *State
	state         *State
}

type Sender interface {
	// Send send msg to machine with id to. It returns nil error if the message is successfully
	// input to the machine, otherwise it returns an error.
	Send(msg *Message, to int64) error
}

func NewMachine(id, peers int64, store Store) (m *Machine, err error) {
	m = &Machine{
		Peers:      peers,
		ID:         id,
		InChan:     make(chan Input),
		OutChan:    make(chan Output),
		OutAckChan: make(chan struct{}),
		store:      store,
		state:      &State{},
	}
	m.savedState, err = m.store.Load()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Machine) Prepare(sender Sender) (err error) {
	if m.sender != nil {
		return fmt.Errorf("sender already set")
	}
	m.sender = sender
	return nil
}

func (m *Machine) Input(ctx context.Context, msg *Message) (err error) {
	in := Input{Message: msg, ErrChan: make(chan error)}

	select {
	case m.InChan <- in:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err = <-in.ErrChan:
	case <-ctx.Done():
		err = ctx.Err()
	}

	return err
}

func (m *Machine) ReadState(state *State) {
	if state == nil {
		return
	}
	m.savedStateMtx.RLock()
	defer m.savedStateMtx.RUnlock()
	m.savedState.CopyTo(state)
}

func (m *Machine) Run() {
	go func() {
		if m.sender == nil {
			panic("sender not set")
		}
		for {
			out := <-m.OutChan
			msgs := out.Messages
			for len(msgs) > 0 {
				next := 0
				for _, msg := range msgs {
					err := m.sender.Send(msg, msg.To)
					if err != nil {
						// TODO add log and log error
						msgs[next] = msg
						next++
					}
				}
				msgs = msgs[:next]
				if len(msgs) == 0 {
					break
				}
				time.Sleep(time.Second)
			}
			m.OutAckChan <- struct{}{}
		}
	}()

	var (
		out      Output
		outCh    chan Output
		outAckCh chan struct{}
	)

	for {
		outCh = nil
		if outAckCh == nil && m.savedState.OutMsgs.Len() > 0 {
			out.Messages = m.savedState.OutMsgs.Peek()
			outCh = m.OutChan
		}

		select {
		case in := <-m.InChan:
			msg, errCh := in.Message, in.ErrChan
			errCh <- m.process(msg)
			close(errCh)
		case outCh <- out:
			outAckCh = m.OutAckChan
		case <-outAckCh:
			m.savedState.OutMsgs.Pop()
			outAckCh = nil
		}
	}

}

func (m *Machine) process(msg *Message) (err error) {
	m.ReadState(m.state)

	if m.hasProcessed(msg) {
		return nil
	}

	if msg.From == m.ID {
		msg.Time = m.state.Clock.Assign()
	} else {
		m.state.Clock.Sync(msg.Time)
	}

	switch msg.Type {
	case MsgAcquire:
		addRequest, acquired := m.acquire()
		if acquired {
			m.bcast(addRequest)
		}
	case MsgAddRequest:
		added := m.addRequest(msg)
		if added {
			m.ack(msg)
		}
	case MsgAckRequest:
		m.addAck(msg)
	case MsgRelease:
		removeRequest, ok := m.release()
		if ok {
			m.bcast(removeRequest)
		}
	case MsgRemoveRequest:
		m.removeRequest(msg)
	case MsgTriggerAcquire:
		m.triggerAcquire(msg)
	}

	m.addProcessed(msg)

	err = m.store.Update(m.savedState, m.state)
	if err != nil {
		return err
	}

	m.writeState(m.state)

	return nil
}

func (m *Machine) acquire() (msg *Message, acquired bool) {
	state := m.state
	if state.Request != nil {
		return nil, false
	}
	msg = &Message{
		Timestamp: Timestamp{Time: state.Clock.Assign(), From: m.ID},
		Type:      MsgAddRequest,
	}
	state.Requests.Insert(msg)
	state.Request = msg
	return msg, true
}

func (m *Machine) addRequest(msg *Message) (added bool) {
	msg.MustType(MsgAddRequest)
	added = m.state.Requests.Insert(msg)
	return added
}

func (m *Machine) ack(msg *Message) {
	msg.MustType(MsgAddRequest)
	ack := Message{
		Timestamp: Timestamp{Time: m.state.Clock.Assign(), From: m.ID},
		Type:      MsgAckRequest,
		Data:      msg,
	}
	m.send(ack, msg.From)
}

func (m *Machine) addAck(msg *Message) {
	msg.MustType(MsgAckRequest)
	req := msg.Data.(*Message)
	req.MustType(MsgAddRequest)
	state := m.state
	_, exists := state.Requests.Search(req)
	if exists && state.Acks[msg.From] == nil {
		state.Acks[msg.From] = msg
	}
}

func (m *Machine) release() (msg *Message, ok bool) {
	state := m.state
	if state.Request == nil {
		return nil, false
	}
	msg = state.Request
	msg.MustType(MsgAddRequest)
	state.Requests.Remove(msg)
	state.Acks = make(Messages, m.Peers)
	state.Request = nil
	msg = &Message{
		Timestamp: Timestamp{Time: state.Clock.Assign(), From: m.ID},
		Type:      MsgRemoveRequest,
		Data:      msg,
	}
	return msg, true
}

func (m *Machine) removeRequest(msg *Message) (removed bool) {
	msg.MustType(MsgRemoveRequest)
	req := msg.Data.(*Message)
	req.MustType(MsgAddRequest)
	removed = m.state.Requests.Remove(req)
	return removed
}

func (m *Machine) triggerAcquire(msg *Message) {
	msg.MustType(MsgTriggerAcquire)
	to := msg.Data.(int64)
	_msg := Message{
		Timestamp: Timestamp{Time: m.state.Clock.Assign(), From: m.ID},
		Type:      MsgAcquire,
	}
	m.send(_msg, to)
}

func (m *Machine) bcast(msg *Message) {
	for i := int64(0); i < m.Peers; i++ {
		if i != m.ID {
			m.send(*msg, i)
		}
	}
}

func (m *Machine) send(msg Message, to int64) {
	msg.To = to
	m.state.OutMsgs.Push(&msg, to)
}

func (m *Machine) hasProcessed(msg *Message) bool {
	if msg.From != m.ID {
		_msg := m.state.Processed[msg.From]
		if _msg == nil {
			return false
		}
		return _msg.Equal(msg)
	}
	return false
}

func (m *Machine) addProcessed(msg *Message) {
	if msg.From != m.ID {
		m.state.Processed[msg.From] = msg
	}
}

func (m *Machine) writeState(state *State) {
	if state == nil {
		return
	}
	m.savedStateMtx.Lock()
	defer m.savedStateMtx.Unlock()
	state.CopyTo(m.savedState)
}
