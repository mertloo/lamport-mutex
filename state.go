package mutex

type State struct {
	clock     Clock
	processed Messages
	requests  Messages
	acks      Messages
	outMsgs   outMsgs
	request   *Message
	acquired  uint64
	granted   uint64
}

func (s *State) Copy() (state *State) {
	state = &State{
		clock:     s.clock,
		processed: s.processed,
		requests:  s.requests,
		acks:      s.acks,
		outMsgs:   s.outMsgs,
		request:   s.request,
		acquired:  s.acquired,
		granted:   s.granted,
	}
	return state
}
