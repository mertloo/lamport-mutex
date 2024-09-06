package mutex

import "fmt"

type State struct {
	Clock     Clock
	Processed Messages
	Requests  Requests
	Acks      Messages
	Request   *Message
	OutMsgs   outMsgs
}

func NewState(peers int64) (state *State) {
	state = &State{}
	state.Processed = make(Messages, peers)
	state.Requests = make(Requests, 0)
	state.Acks = make(Messages, peers)
	state.OutMsgs = newOutMsgs(peers)
	return state
}

func (s *State) String() (str string) {
	str = fmt.Sprintf(`
Clock: %d
Processed: %s
Requests: %s
Acks: %s
Request: %s
OutMsgs.Len: %d
`,
		s.Clock.LastTime,
		&s.Processed,
		s.Requests,
		&s.Acks,
		s.Request,
		s.OutMsgs.Len(),
	)
	return str
}

func (s *State) CopyTo(state *State) {
	if state == nil {
		return
	}
	state.Clock = s.Clock
	state.Processed = s.Processed
	state.Requests = s.Requests
	state.Acks = s.Acks
	state.OutMsgs = s.OutMsgs
	state.Request = s.Request
}

func (s *State) Granted() (granted bool) {
	first := s.Request != nil && len(s.Requests) > 0 && s.Requests[0].Equal(s.Request)
	if !first {
		return false
	}
	for i, ack := range s.Acks {
		fromPeer := i != int(s.Request.From)
		if fromPeer && ack == nil {
			return false
		}
	}
	return true
}
