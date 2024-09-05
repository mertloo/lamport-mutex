package main

import (
	"context"
	"fmt"
	"mutex"
	"sync"
)

type Network struct {
	machines sync.Map
}

func (n *Network) AddMachine(m *mutex.Machine) {
	n.machines.Store(m.ID, m)
}

func (n *Network) Send(msg *mutex.Message, to int64) error {
	v, ok := n.machines.Load(to)
	if !ok {
		return fmt.Errorf("machine %v not found", to)
	}
	m := v.(*mutex.Machine)
	ctx := context.Background()
	err := m.Input(ctx, msg)
	if err != nil {
		return fmt.Errorf("machine %v input err: %v", to, err)
	}
	return nil
}
