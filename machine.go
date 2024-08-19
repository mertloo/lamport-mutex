package mutex

import (
	"context"
	"sync"
)

type Machine struct {
	Peers         int64
	ID            int64
	InChan        chan Input
	OutChan       chan Output
	OutAckChan    chan struct{}
	store         Store
	savedStateMtx sync.RWMutex
	savedState    *State
	*State
}

func NewMachine(id, peers int64, store Store) (m *Machine, err error) {
	m = &Machine{
		Peers:      peers,
		ID:         id,
		InChan:     make(chan Input),
		OutChan:    make(chan Output),
		OutAckChan: make(chan struct{}),
		store:      store,
		State:      &State{},
	}
	m.savedState, err = m.store.Load()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Machine) Process(ctx context.Context, msg *Message) (err error) {
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

func (m *Machine) Run() {
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
	m.ReadState(m.State)

	if m.hasProcessed(msg) {
		return nil
	}

	if msg.From == m.ID {
		msg.Time = m.Clock.Assign()
	} else {
		m.Clock.Sync(msg.Time)
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
	}

	m.addProcessed(msg)

	err = m.store.Update(m.savedState, m.State)
	if err != nil {
		return err
	}

	m.writeState(m.State)

	return nil
}

func (m *Machine) acquire() (msg *Message, acquired bool) {
	if m.Request != nil {
		return nil, false
	}
	msg = &Message{
		Timestamp: Timestamp{Time: m.Clock.Assign(), From: m.ID},
		Type:      MsgAddRequest,
	}
	i := m.Requests.Search(msg)
	m.Requests.Insert(i, msg)
	m.Request = msg
	return msg, true
}

func (m *Machine) addRequest(msg *Message) (added bool) {
	msg.MustType(MsgAddRequest)
	i := m.Requests.Search(msg)
	if !m.Requests[i].Equal(msg) {
		m.Requests.Insert(i, msg)
		added = true
	}
	return added
}

func (m *Machine) ack(msg *Message) {
	msg.MustType(MsgAddRequest)
	ack := &Message{
		Timestamp: Timestamp{Time: m.Clock.Assign(), From: m.ID},
		Type:      MsgAckRequest,
		Data:      msg,
	}
	m.send(ack, msg.From)
}

func (m *Machine) addAck(msg *Message) {
	msg.MustType(MsgAckRequest)
	req := msg.Data.(*Message)
	i := m.Requests.Search(req)
	if m.Requests[i].Equal(req) {
		if m.Acks[msg.From] == nil {
			m.Acks[msg.From] = msg
		}
	}
}

func (m *Machine) release() (msg *Message, ok bool) {
	if m.Request == nil {
		return nil, false
	}
	msg = m.Request
	i := m.Requests.Search(msg)
	m.Requests.Remove(i)
	m.Acks = make(Messages, m.Peers)
	m.Request = nil
	return msg, true
}

func (m *Machine) removeRequest(msg *Message) (removed bool) {
	msg.MustType(MsgRemoveRequest)
	i := m.Requests.Search(msg)
	if m.Requests[i].Equal(msg) {
		m.Requests.Remove(i)
		removed = true
	}
	return removed
}

func (m *Machine) bcast(msg *Message) {
	for i := int64(0); i < m.Peers; i++ {
		if i != m.ID {
			m.send(msg, i)
		}
	}
}

func (m *Machine) send(msg *Message, to int64) {
	msg.To = to
	m.OutMsgs.Push(msg, to)
}

func (m *Machine) hasProcessed(msg *Message) bool {
	if msg.From != m.ID {
		return m.Processed[msg.From].Equal(msg)
	}
	return false
}

func (m *Machine) addProcessed(msg *Message) {
	if msg.From != m.ID {
		m.Processed[msg.From] = msg
	}
}

func (m *Machine) ReadState(state *State) {
	if state == nil {
		return
	}
	m.savedStateMtx.RLock()
	defer m.savedStateMtx.RUnlock()
	m.savedState.CopyTo(state)
}

func (m *Machine) writeState(state *State) {
	if state == nil {
		return
	}
	m.savedStateMtx.Lock()
	defer m.savedStateMtx.Unlock()
	state.CopyTo(m.savedState)
}
