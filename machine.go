package mutex

import (
	"context"
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
	savedStateMtx sync.RWMutex
	savedState    *State
	tmpState      *State
	state         *State

	senderMtx sync.Mutex
	sender    Sender
}

type Sender interface {
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
		tmpState:   &State{},
		state:      &State{},
	}
	m.savedState, err = m.store.Load()
	if err != nil {
		return nil, err
	}
	return m, nil
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

func (m *Machine) SetSender(sender Sender) {
	m.senderMtx.Lock()
	defer m.senderMtx.Unlock()
	m.sender = sender
}

func (m *Machine) GetSender() (sender Sender) {
	m.senderMtx.Lock()
	defer m.senderMtx.Unlock()
	return m.sender
}

func (m *Machine) Run() {
	go func() {
		var sender Sender
		for {
			for {
				sender = m.GetSender()
				if sender != nil {
					break
				}
				time.Sleep(time.Second)
			}

			out := <-m.OutChan
			msgs := out.Messages
			for len(msgs) > 0 {
				next := 0
				for _, msg := range msgs {
					err := sender.Send(msg, msg.To)
					if err != nil {
						msgs[next] = msg
						next++
					}
				}
				msgs = msgs[next:]
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
	m.state.CopyTo(m.tmpState)

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
	}

	m.addProcessed(msg)

	err = m.store.Update(m.tmpState, m.state)
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
	i := state.Requests.Search(msg)
	state.Requests.Insert(i, msg)
	state.Request = msg
	return msg, true
}

func (m *Machine) addRequest(msg *Message) (added bool) {
	msg.MustType(MsgAddRequest)
	state := m.state
	i := state.Requests.Search(msg)
	if !state.Requests[i].Equal(msg) {
		state.Requests.Insert(i, msg)
		added = true
	}
	return added
}

func (m *Machine) ack(msg *Message) {
	msg.MustType(MsgAddRequest)
	state := m.state
	ack := &Message{
		Timestamp: Timestamp{Time: state.Clock.Assign(), From: m.ID},
		Type:      MsgAckRequest,
		Data:      msg,
	}
	m.send(ack, msg.From)
}

func (m *Machine) addAck(msg *Message) {
	msg.MustType(MsgAckRequest)
	req := msg.Data.(*Message)
	state := m.state
	i := state.Requests.Search(req)
	if state.Requests[i].Equal(req) {
		if state.Acks[msg.From] == nil {
			state.Acks[msg.From] = msg
		}
	}
}

func (m *Machine) release() (msg *Message, ok bool) {
	state := m.state
	if state.Request == nil {
		return nil, false
	}
	msg = state.Request
	i := state.Requests.Search(msg)
	state.Requests.Remove(i)
	state.Acks = make(Messages, m.Peers)
	state.Request = nil
	return msg, true
}

func (m *Machine) removeRequest(msg *Message) (removed bool) {
	msg.MustType(MsgRemoveRequest)
	state := m.state
	i := state.Requests.Search(msg)
	if state.Requests[i].Equal(msg) {
		state.Requests.Remove(i)
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
	m.state.OutMsgs.Push(msg, to)
}

func (m *Machine) hasProcessed(msg *Message) bool {
	if msg.From != m.ID {
		return m.state.Processed[msg.From].Equal(msg)
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
