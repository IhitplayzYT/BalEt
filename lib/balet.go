package lib

import (
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
)

type Server struct {
	ip string
	_w float32 // Hidden weight per Server for Weighted Algorithms
}

type Request struct {
	method    string
	param_url string
	body      string
}

type Algo int

const (
	RoundRobin Algo = iota
	LeastConnections
	WeightedConnections
)

type BalEt struct {
	ticks         uint64
	timeout       uint64
	servers       []Server
	reqs          chan Request
	mtx           sync.Mutex
	algo          Algo
	_cur          uint64 // Internal param only for Round Robin
	srvr_root_pfx string
	srvr_cnt      uint64
}

func (BalEt) New(timeout uint64, buff_l uint64, srvr_root_pfx string) BalEt {
	return BalEt{ticks: 0, timeout: timeout, servers: make([]Server, 0), reqs: make(chan Request, buff_l), mtx: sync.Mutex{}, _cur: 0, srvr_cnt: 0, srvr_root_pfx: IF(srvr_root_pfx == nil, "http://localhost:3000", srvr_root_pfx)}
}

func IF[T any](cond bool, if_true T, if_false T) T {
	if cond {
		return if_true
	} else {
		return if_false
	}

}

func (b *BalEt) submit(r Request) {
	b.reqs <- r
}

func (b *BalEt) add_server(s Server) {
	b.servers = append(b.servers, s)
	b.srvr_cnt += 1
}

func (b *BalEt) run() {
	for {
		select {
		case k := <-b.reqs:
			b._proc_req(&k)
		default:
		}
	}

}

func (b *BalEt) _proc_req(r *Request) {
	switch b.algo {
	case RoundRobin:
		b._handle_rr(r)
	case LeastConnections:
		b._handle_lc(r)
	case WeightedConnections:
		b._handle_wc(r)
	default:
	}

}

func (b *BalEt) _handle_rr(r *Request) {
	// Use srvr_roor_pfx to prefix every route req
	prev := b._cur

	for i := {
		if resp, err := http.Get(b.servers[b._cur].ip + "/health"); err != nil {
			defer resp.Body.Close()

			if body, err := io.ReadAll(resp.Body); err != nil {
				if strings.Contains(strings.ToLower(string(body)), "ok") && resp.StatusCode == http.StatusOK {
					// We found live server
					break
				}
			}

		}

	}

	// Send /health
	// Route to _cnt Server
	switch strings.ToLower(r.method) {
	case "post":
	case "put":
	case "delete":
	case "get":

	}
	// Update _cnt -> (_cnt + 1) % srvr_cnt
}

func (b *BalEt) _handle_lc(r *Request) {
}

func (b *BalEt) _handle_wc(r *Request) {
}
