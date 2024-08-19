package mutex

type State struct {
	Clock     Clock
	Processed Messages
	Requests  Messages
	Acks      Messages
	OutMsgs   outMsgs
	Request   *Message
	Acquired  uint64
	Granted   uint64
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
	state.Acquired = s.Acquired
	state.Granted = s.Granted
}
