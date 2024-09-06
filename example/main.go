package main

import (
	"context"
	"fmt"
	"mutex"
	"time"
)

func main() {
	machines := bootstrap(3)

	acquire := &mutex.Message{Type: mutex.MsgAcquire}
	acquire.From = machines[0].ID
	input(machines[0], acquire)
	triggerAcquire := &mutex.Message{Type: mutex.MsgTriggerAcquire}
	triggerAcquire.From = machines[0].ID
	triggerAcquire.Data = int64(1)
	input(machines[0], triggerAcquire)
	wait()
	assert("m0 granted",
		granted(machines[0]),
		!granted(machines[1]),
		!granted(machines[2]),
	)

	release0 := &mutex.Message{Type: mutex.MsgRelease}
	release0.From = machines[0].ID
	input(machines[0], release0)
	wait()
	assert("m1 granted",
		!granted(machines[0]),
		granted(machines[1]),
		!granted(machines[2]),
	)

	release1 := &mutex.Message{Type: mutex.MsgRelease}
	release1.From = machines[1].ID
	input(machines[1], release1)
	wait()
	assert("no one granted",
		!granted(machines[0]),
		!granted(machines[1]),
		!granted(machines[2]),
	)

	// TODO mv to test and add more cases
}

func bootstrap(peers int64) (machines []*mutex.Machine) {
	network := &Network{}
	stores := make([]*mutex.MemoryStore, peers)
	machines = make([]*mutex.Machine, peers)
	for i := int64(0); i < peers; i++ {
		store := mutex.NewMemoryStore(peers)
		machine, err := mutex.NewMachine(i, peers, store)
		if err != nil {
			panic(err)
		}
		machine.Prepare(network)
		network.AddMachine(machine)
		stores[i] = store
		machines[i] = machine
	}
	for i := int64(0); i < peers; i++ {
		go machines[i].Run()
	}
	return machines
}

func input(machine *mutex.Machine, msg *mutex.Message) {
	ctx := context.Background()
	err := machine.Input(ctx, msg)
	if err != nil {
		panic(err)
	}
}

func granted(machine *mutex.Machine) bool {
	state := &mutex.State{}
	machine.ReadState(state)
	return state.Granted()
}

func wait() {
	time.Sleep(100 * time.Millisecond)
}

func assert(info string, conds ...bool) {
	for i, cond := range conds {
		if !cond {
			fmt.Printf("assertion %d failed: %s\n", i, info)
			panic(i)
		}
	}
	fmt.Println("assertion passed: ", info)
}
