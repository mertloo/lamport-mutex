package mutex

type Store interface {
	Update(src, dst *State) error
	Load() (state *State, err error)
}

type MemoryStore struct {
	state *State
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		state: &State{},
	}
}

func (ms *MemoryStore) Update(src, dst *State) error {
	*src = *dst
	return nil
}

func (ms *MemoryStore) Load() (state *State, err error) {
	state = &State{}
	ms.state.CopyTo(state)
	return state, nil
}
