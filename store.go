package mutex

type Store interface {
	Update(src, dst *State) error
	Load() (state *State, err error)
}

type MemoryStore struct {
	state *State
}

func NewMemoryStore(peers int64) (ms *MemoryStore) {
	ms = &MemoryStore{}
	ms.state = NewState(peers)
	return ms
}

func (ms *MemoryStore) Update(src, dst *State) error {
	_ = src
	ms.state = dst
	return nil
}

func (ms *MemoryStore) Load() (state *State, err error) {
	state = &State{}
	ms.state.CopyTo(state)
	return state, nil
}
