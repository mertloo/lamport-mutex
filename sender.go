package mutex

type Sender struct {
	node   *Node
	client Client
}

type Client interface {
	WaitProcessed(to int64, msg Message) error
}

func NewSender(node *Node, client Client) *Sender {
	return &Sender{
		node:   node,
		client: client,
	}
}

func (s *Sender) Run() {
	for {
		outMsgs := <-s.node.OutChan
		s.sendUntilSuccess(outMsgs)
		s.node.OutAckChan <- struct{}{}
	}
}

func (s *Sender) sendUntilSuccess(outMsgs []Message) {
	for len(outMsgs) > 0 {
		next := 0
		for _, msg := range outMsgs {
			err := s.client.WaitProcessed(msg.To, msg)
			if err != nil {
				outMsgs[next] = msg
				next++
			}
		}
		outMsgs = outMsgs[:next]
	}
}
