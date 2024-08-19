package mutex

type State struct {
	Clock     Clock
	Processed Messages
	Requests  Messages
	Acks      Messages
	Request   *Message
	OutMsgs   outMsgs
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
	if len(s.Requests) == 0 || !s.Requests[0].Equal(s.Request) {
		return false
	}
	for i, ack := range s.Acks {
		if i != int(s.Request.From) && ack == nil {
			return false
		}
	}
	return true
}
