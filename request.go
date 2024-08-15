package mutex

type Request struct {
	Timestamp
}

func (r *Request) Equal(other *Request) bool {
	return r.Timestamp == other.Timestamp
}

func (r *Request) LessThan(other *Request) bool {
	return r.Timestamp.LessThan(other.Timestamp)
}

type RequestAcks []*RequestAck

type RequestAck struct {
	Timestamp
	Request Request
}

type Timestamp struct {
	Time int64
	From int64
}

func (ts *Timestamp) LessThan(other Timestamp) bool {
	return ts.Time < other.Time || (ts.Time == other.Time && ts.From < other.From)
}

type Requests []Request

func (r Requests) Search(req *Request) int {
	left, right := 0, len(r)-1
	for left <= right {
		i := (left + right) / 2
		if r[i].LessThan(req) {
			left = i + 1
		} else {
			right = i - 1
		}
	}
	return left
}

func (r Requests) Insert(i int, req *Request) {
	r = append(r, Request{})
	copy(r[i+1:], r[i:])
	r[i] = *req
}

func (r Requests) Remove(i int) {
	copy(r[i:], r[i+1:])
	r = r[:len(r)-1]
}
