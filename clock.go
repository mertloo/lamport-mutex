package mutex

type Clock struct {
	LastTime int64
}

func (c *Clock) Sync(t int64) {
	c.LastTime = max(c.LastTime, t)
}

func (c *Clock) Assign() int64 {
	c.LastTime++
	return c.LastTime
}
