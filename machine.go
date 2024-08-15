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
	msg := Message{Type: MsgAcquire, Timestamp: Timestamp{From: m.ID}}
	return m.waitProcessed(ctx, msg)
}

func (m *Machine) Release(ctx context.Context) (err error) {
	msg := Message{Type: MsgRelease, Timestamp: Timestamp{From: m.ID}}
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

func (m *Machine) process(msg Message) (err error) {
	m.State = m.savedState.Copy()

	if m.hasProcessed(msg) {
		return nil
	}

	m.clock.Sync(msg.Time)

	switch msg.Type {
	case MsgAcquire:
		req, ok := m.acquire()
		if ok {
			m.bcastAdd(req)
		}
	case MsgAddRequest:
		req := msg.Data.(*Request)
		ok := m.addRequest(req)
		if ok {
			m.ack(req)
		}
	case MsgAckRequest:
		ack := msg.Data.(*RequestAck)
		m.addAck(ack)
	case MsgRelease:
		req, ok := m.release()
		if ok {
			m.bcastRemove(req)
		}
	case MsgRemoveRequest:
		req := msg.Data.(*Request)
		m.removeRequest(req)
	}

	m.addProcessed(msg)

	err = m.saveState(m.State)
	if err != nil {
		return err
	}

	return nil
}

func (m *Machine) acquire() (req *Request, ok bool) {
	if atomic.LoadUint64(&m.acquired) == 1 {
		return nil, false
	}
	req = &Request{
		Timestamp: Timestamp{Time: m.clock.Assign(), From: m.ID},
	}
	i := m.requests.Search(req)
	m.requests.Insert(i, req)
	atomic.StoreUint64(&m.acquired, 1)
	m.request = req
	return req, true
}

func (m *Machine) addRequest(req *Request) (added bool) {
	i := m.requests.Search(req)
	if !m.requests[i].Equal(req) {
		m.requests.Insert(i, req)
		added = true
	}
	return added
}

func (m *Machine) ack(req *Request) {
	ack := &RequestAck{}
	ack.Timestamp = Timestamp{Time: m.clock.Assign(), From: m.ID}
	ack.Request = *req
	msg := Message{
		Timestamp: ack.Timestamp,
		Type:      MsgAckRequest,
		Data:      ack,
	}
	m.send(msg, req.From)
}

func (m *Machine) addAck(ack *RequestAck) {
	i := m.requests.Search(&ack.Request)
	if m.requests[i].Equal(&ack.Request) {
		if m.acks[ack.From] == nil {
			m.acks[ack.From] = ack
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

func (m *Machine) release() (req *Request, ok bool) {
	if atomic.LoadUint64(&m.acquired) == 0 {
		return nil, false
	}
	req = m.request
	i := m.requests.Search(req)
	m.requests.Remove(i)
	atomic.StoreUint64(&m.granted, 0)
	atomic.StoreUint64(&m.acquired, 0)
	m.acks = make(RequestAcks, m.Peers)
	m.request = nil
	return req, true
}

func (m *Machine) removeRequest(req *Request) {
	i := m.requests.Search(req)
	if m.requests[i].Equal(req) {
		m.requests.Remove(i)
		m.updateGranted()
	}
}

func (m *Machine) bcastAdd(req *Request) {
	msg := Message{
		Timestamp: req.Timestamp,
		Type:      MsgAddRequest,
		Data:      req,
	}
	m.bcast(msg)
}

func (m *Machine) bcastRemove(req *Request) {
	msg := Message{
		Timestamp: Timestamp{Time: m.clock.Assign(), From: m.ID},
		Type:      MsgRemoveRequest,
		Data:      req,
	}
	m.bcast(msg)
}

func (m *Machine) bcast(msg Message) {
	for i := int64(0); i < m.Peers; i++ {
		if i != m.ID {
			m.send(msg, i)
		}
	}
}

func (m *Machine) send(msg Message, to int64) {
	msg.To = to
	m.outMsgs.Push(msg, to)
}

func (m *Machine) hasProcessed(msg Message) bool {
	if msg.From != m.ID {
		return m.processed[msg.From] == msg.Timestamp
	}
	return false
}

func (m *Machine) addProcessed(msg Message) {
	if msg.From != m.ID {
		m.processed[msg.From] = msg.Timestamp
	}
}

func (m *Machine) waitProcessed(ctx context.Context, msg Message) (err error) {
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
