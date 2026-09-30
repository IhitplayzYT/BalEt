package lib

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
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

type LogManager struct{
	fpath 	  string
	buff  	  string	
	mx_l  	  uint64
	flsh_freq uint64
	_cur 	  uint64
}

func NewLogManager(fpath string,flsh uint64,mx_l uint64) LogManager{
	return LogManager{fpath: fpath,buff: "",mx_l: mx_l,flsh_freq: flsh,_cur: 0}
}

func (l * LogManager) Log(reqstr string,resp string){
	hr,min,sec := time.Now().Clock()
	t_str := fmt.Sprintf("%v:%v:%v",hr,min,sec)
	to_write := fmt.Sprintf("\n[%v]: %v\n%v\n",t_str,reqstr,resp)
	lw := len(to_write)
	lb := len(l.buff)

	if l.flsh_freq != 0 && l._cur == l.flsh_freq{
		if fp,err := os.OpenFile(l.fpath,os.O_CREATE | os.O_APPEND,0644);err != nil{
			defer fp.Close()
			fmt.Fprintf(os.Stderr,"Log manager failure : %v",err)
			os.Exit(-1)
		}else{
			defer fp.Close()
			if n,err := fp.WriteString(l.buff); err != nil{
				fmt.Fprintf(os.Stderr,"Log manager failure : %v",err)
				os.Exit(-1)
			}else{
				if n == lb{
					l.buff = ""
					l._cur = 0
				}
			}
			l.buff += to_write
			l._cur += 1
		}
	}


	if l.mx_l != 0 && lw + lb <= int(l.mx_l){
		if fp,err := os.OpenFile(l.fpath,os.O_CREATE | os.O_APPEND,0644);err != nil{
			defer fp.Close()
			fmt.Fprintf(os.Stderr,"Log manager failure : %v",err)
			os.Exit(-1)
		}else{
			defer fp.Close()
			if n,err := fp.WriteString(l.buff+to_write); err != nil{
				fmt.Fprintf(os.Stderr,"Log manager failure : %v",err)
				os.Exit(-1)
			}else{
				if n == lb+lw {
					l.buff = ""
				}
			}

		}	

	}

}














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
	log_mnger 	  LogManager  	  
}

func (BalEt) New(timeout uint64, buff_l uint64, srvr_root_pfx string) BalEt {
	return BalEt{ticks: 0, timeout: timeout, servers: make([]Server, 0), reqs: make(chan Request, buff_l), mtx: sync.Mutex{}, _cur: 0, srvr_cnt: 0, srvr_root_pfx: IF(srvr_root_pfx == "", "http://localhost:3000", srvr_root_pfx),log_mnger: NewLogManager("",0,256)}
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
	l := uint64(len(b.servers))
	i := uint64(0)
	for ;i < l;i++{
		if resp, err := http.Get(b.servers[b._cur].ip + "/health"); err != nil {
			defer resp.Body.Close()
			if body, err := io.ReadAll(resp.Body); err != nil {
				if resp.StatusCode == http.StatusOK && strings.Contains(strings.ToLower(string(body)), "ok") {
					b._cur = (b._cur + i) % l
					break
				}
			}

		}

	}

	// Route to _cnt Server
	b._send_to(i,r)
}

func (b *BalEt) _handle_lc(r *Request) {
}

func (b *BalEt) _handle_wc(r *Request) {
}


func (b * BalEt) _send_to(i uint64,r * Request){
	ipstr := "http://" + b.servers[i].ip + IF(strings.HasPrefix(r.param_url,"/"),"","/") + r.param_url

	switch strings.ToLower(r.method){
	case "post":
		if resp,err := http.Get(ipstr); err != nil {
			fmt.Fprintf(os.Stderr,"Request GET failed for %v: %v",ipstr,err)
		}else{
				
		}

	case "get":
	case "put":
	case "delete":
	default:
		return
	}
}




