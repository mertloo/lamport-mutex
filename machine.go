package mutex

import (
	"context"
	"sync/atomic"
)

type Machine struct {
	Peers      int64
	ID         int64
	InChan     chan Input
	OutChan    chan Output
	OutAckChan chan struct{}
	store      Store
	savedState *State
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
	}
	m.savedState, err = m.store.Load()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (m *Machine) Acquired() bool {
	return atomic.LoadUint64(&m.savedState.acquired) == 1
}

func (m *Machine) Granted() bool {
	return atomic.LoadUint64(&m.savedState.granted) == 1
}

func (m *Machine) Acquire(ctx context.Context) (err error) {
	msg := &Message{Type: MsgAcquire, Timestamp: Timestamp{From: m.ID}}
	return m.waitProcessed(ctx, msg)
}

func (m *Machine) Release(ctx context.Context) (err error) {
	msg := &Message{Type: MsgRelease, Timestamp: Timestamp{From: m.ID}}
	return m.waitProcessed(ctx, msg)
}

func (m *Machine) Run() {
	var (
		out      Output
		outCh    chan Output
		outAckCh chan struct{}
	)

	for {
		outCh = nil
		if outAckCh == nil && m.savedState.outMsgs.Len() > 0 {
			out.Messages = m.savedState.outMsgs.Peek()
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
			m.savedState.outMsgs.Pop()
			outAckCh = nil
		}
	}

}

func (m *Machine) process(msg *Message) (err error) {
	m.State = m.savedState.Copy()

	if m.hasProcessed(msg) {
		return nil
	}

	if msg.From == m.ID {
		msg.Time = m.clock.Assign()
	} else {
		m.clock.Sync(msg.Time)
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

	err = m.saveState(m.State)
	if err != nil {
		return err
	}

	return nil
}

func (m *Machine) acquire() (msg *Message, acquired bool) {
	if atomic.LoadUint64(&m.acquired) == 1 {
		return nil, false
	}
	msg = &Message{
		Timestamp: Timestamp{Time: m.clock.Assign(), From: m.ID},
		Type:      MsgAddRequest,
	}
	i := m.requests.Search(msg)
	m.requests.Insert(i, msg)
	m.request = msg
	atomic.StoreUint64(&m.acquired, 1)
	return msg, true
}

func (m *Machine) addRequest(msg *Message) (added bool) {
	msg.MustType(MsgAddRequest)
	i := m.requests.Search(msg)
	if !m.requests[i].Equal(msg) {
		m.requests.Insert(i, msg)
		added = true
	}
	return added
}

func (m *Machine) ack(msg *Message) {
	msg.MustType(MsgAddRequest)
	ack := &Message{
		Timestamp: Timestamp{Time: m.clock.Assign(), From: m.ID},
		Type:      MsgAckRequest,
		Data:      msg,
	}
	m.send(ack, msg.From)
}

func (m *Machine) addAck(msg *Message) {
	msg.MustType(MsgAckRequest)
	req := msg.Data.(*Message)
	i := m.requests.Search(req)
	if m.requests[i].Equal(req) {
		if m.acks[msg.From] == nil {
			m.acks[msg.From] = msg
			m.updateGranted()
		}
	}
}

func (m *Machine) updateGranted() {
	if len(m.requests) == 0 || !m.requests[0].Equal(m.request) {
		return
	}
	for i, ack := range m.acks {
		if i != int(m.ID) && ack == nil {
			return
		}
	}
	atomic.StoreUint64(&m.granted, 1)
}

func (m *Machine) release() (msg *Message, ok bool) {
	if atomic.LoadUint64(&m.acquired) != 1 {
		return nil, false
	}
	msg = m.request
	i := m.requests.Search(msg)
	m.requests.Remove(i)
	atomic.StoreUint64(&m.granted, 0)
	atomic.StoreUint64(&m.acquired, 0)
	m.acks = make(Messages, m.Peers)
	m.request = nil
	return msg, true
}

func (m *Machine) removeRequest(msg *Message) (removed bool) {
	msg.MustType(MsgRemoveRequest)
	i := m.requests.Search(msg)
	if m.requests[i].Equal(msg) {
		m.requests.Remove(i)
		m.updateGranted()
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
	m.outMsgs.Push(msg, to)
}

func (m *Machine) hasProcessed(msg *Message) bool {
	if msg.From != m.ID {
		return m.processed[msg.From].Equal(msg)
	}
	return false
}

func (m *Machine) addProcessed(msg *Message) {
	if msg.From != m.ID {
		m.processed[msg.From] = msg
	}
}

func (m *Machine) waitProcessed(ctx context.Context, msg *Message) (err error) {
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

func (m *Machine) saveState(state *State) (err error) {
	err = m.store.Update(m.savedState, state)
	if err != nil {
		return err
	}
	m.savedState = state
	return nil
}
