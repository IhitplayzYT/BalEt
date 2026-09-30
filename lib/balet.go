package lib

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/btree"
)

type Server struct {
	ip string
	_w float64 // Hidden weight per Server for Weighted Algorithms
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
	mtx       sync.Mutex
}

func NewLogManager(fpath string,flsh uint64,mx_l uint64) LogManager{
	return LogManager{fpath: fpath,buff: "",mx_l: mx_l,flsh_freq: flsh,_cur: 0}
}

func (l * LogManager) dump(){
	if fp,err := os.OpenFile(l.fpath,os.O_CREATE | os.O_APPEND,0644);err != nil{
		defer fp.Close()
		fmt.Fprintf(os.Stderr,"Log manager failure : %v",err)
		os.Exit(-1)
	}else{
		defer fp.Close()
		if _,err := fp.WriteString(l.buff);err != nil {
			fmt.Fprintf(os.Stderr,"Log manager failure : %v",err)
			os.Exit(-1)
		}
	}
}



func (l * LogManager) Log(reqstr string,resp string){
	hr,min,sec := time.Now().Clock()
	t_str := fmt.Sprintf("%v:%v:%v",hr,min,sec)
	to_write := fmt.Sprintf("\n[%v]: %v\n%v\n",t_str,reqstr,resp)
	if fp,err := os.OpenFile(l.fpath,os.O_CREATE | os.O_APPEND,0644);err != nil{
		defer fp.Close()
		fmt.Fprintf(os.Stderr,"Log manager failure : %v",err)
		os.Exit(-1)
	}else{
		defer fp.Close()
		if _,err := fp.WriteString(to_write);err != nil{
			fmt.Fprintf(os.Stderr,"Log manager failure : %v",err)
			os.Exit(-1)		
		}

	}
}


func (l * LogManager) LazyLog(reqstr string,resp string){
	l.mtx.Lock()
	defer l.mtx.Unlock()
	
	hr,min,sec := time.Now().Clock()
	t_str := fmt.Sprintf("%v:%v:%v",hr,min,sec)
	to_write := fmt.Sprintf("\n[%v]: %v\n%v\n",t_str,reqstr,resp)
	lw := len(to_write)
	lb := len(l.buff)

	if l.flsh_freq != 0{
		if l._cur == l.flsh_freq{
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
		}else{
			l.buff += to_write
			l._cur += 1
		}
	}


	if l.mx_l != 0{
		if lw + lb <= int(l.mx_l){
			l.buff += to_write
		}else{
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

}




type BalEt struct {
	timeout       uint64
	servers       []Server
	reqs          chan Request
	mtx           sync.Mutex
	algo          Algo
	_cur          uint64 // Internal param only for Round Robin
	srvr_root_pfx string
	srvr_cnt      uint64
	log_mnger 	  LogManager 
	_conn_map     btree.Map[uint64,uint64] // Used for LC
	_w_map        map[uint64]float64
}

func NewBalET(timeout uint64, buff_l uint64, srvr_root_pfx string) BalEt {
	return BalEt{timeout: timeout, servers: make([]Server, 0), reqs: make(chan Request, buff_l), mtx: sync.Mutex{}, _cur: 0, srvr_cnt: 0, srvr_root_pfx: IF(srvr_root_pfx == "", "http://localhost:3000", srvr_root_pfx),log_mnger: NewLogManager("",0,256),_conn_map: btree.Map[uint64,uint64]{},_w_map: make(map[uint64]float64)}
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
	b.mtx.Lock()
	b.servers = append(b.servers, s)
	b._conn_map.Set(b.srvr_cnt,0)
	b._w_map[b.srvr_cnt] = s._w
	b.srvr_cnt += 1
	b.mtx.Unlock()
}

func (b *BalEt) run() {
	for {
		select {
		case k := <-b.reqs:
			go b._proc_req(&k)
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
	b.mtx.Lock()
	l := uint64(len(b.servers))
	b.mtx.Unlock()
	i := uint64(0)
	for ;i < l;i++{
		b.mtx.Lock()
		cur := b._cur
		b.mtx.Unlock()
		b.mtx.Lock()
		serverIP := b.servers[cur].ip
		b.mtx.Unlock()
		if resp, err := http.Get(serverIP + "/health"); err != nil {
			defer resp.Body.Close()
			if body, err := io.ReadAll(resp.Body); err != nil {
				if resp.StatusCode == http.StatusOK && strings.Contains(strings.ToLower(string(body)), "ok") {
					b.mtx.Lock()
					b._cur = (b._cur + i) % l
					b.mtx.Unlock()
					break
				}
			}
		}
	}
	b.mtx.Lock()
	v,ok := b._conn_map.Get(i)
	if ok{
		b._conn_map.Set(i,v+1)
	}else{
		b._conn_map.Set(i,1)
	}
	b.mtx.Unlock()
	
	// Route to _cnt Server
	b._send_to(i,r)

	b.mtx.Lock()
	v,ok = b._conn_map.Get(i)
	if ok && v > 0{
		b._conn_map.Set(i,v-1)
	}else{
		b._conn_map.Set(i,0)
	}
	b.mtx.Unlock()
}

func (b *BalEt) _handle_lc(r *Request) {
	b._conn_map.Descend(0,func(key uint64, value uint64) bool{
		b.mtx.Lock()
		cur := b._cur
		serverIP := b.servers[cur].ip
		b.mtx.Unlock()
		if resp, err := http.Get(serverIP + "/health"); err != nil {
			defer resp.Body.Close()
			if body, err := io.ReadAll(resp.Body); err != nil {
				if resp.StatusCode == http.StatusOK && strings.Contains(strings.ToLower(string(body)), "ok") {
					b.mtx.Lock()
					v,ok := b._conn_map.Get(key)
					if ok{
						b._conn_map.Set(key,v+1)
					}else{
						b._conn_map.Set(key,1)
					}
					b.mtx.Unlock()
					
					// Route to _cnt Server
					b._send_to(key,r)

					b.mtx.Lock()
					v,ok = b._conn_map.Get(key)
					if ok && v > 0{
						b._conn_map.Set(key,v-1)
					}else{
						b._conn_map.Set(key,0)
					}
					b.mtx.Unlock()
					return false 
				}
			}

		}
		return true
	})
}

func (b *BalEt) _handle_wc(r *Request) {
	var scores btree.Map[uint64,float64]
	b.mtx.Lock()
	for i,v := range b.servers{
		conns,_ := b._conn_map.Get(uint64(i))
		scores.Set(uint64(i),float64(conns)/v._w)
	}
	b.mtx.Unlock()

	scores.Ascend(0,func(key uint64, value float64) bool {
		b.mtx.Lock()
		cur := b._cur
		serverIP := b.servers[cur].ip
		b.mtx.Unlock()
		if resp, err := http.Get(serverIP + "/health"); err != nil {
			defer resp.Body.Close()
			if body, err := io.ReadAll(resp.Body); err != nil {
				if resp.StatusCode == http.StatusOK && strings.Contains(strings.ToLower(string(body)), "ok") {
					b.mtx.Lock()
					v,ok := b._conn_map.Get(key)
					if ok{
						b._conn_map.Set(key,v+1)
					}else{
						b._conn_map.Set(key,1)
					}
					b.mtx.Unlock()
					
					// Route to _cnt Server
					b._send_to(key,r)

					b.mtx.Lock()
					v,ok = b._conn_map.Get(key)
					if ok && v > 0{
						b._conn_map.Set(key,v-1)
					}else{
						b._conn_map.Set(key,0)
					}
					b.mtx.Unlock()
					return false 
				}
			}

		}
		return true

	})



}

func (b * BalEt) _send_to(i uint64,r * Request){
	b.mtx.Lock()
	ipstr := "http://" + b.servers[i].ip + IF(strings.HasPrefix(r.param_url,"/"),"","/") + r.param_url
	b.mtx.Unlock()

	switch strings.ToLower(r.method){
	case "post":
		io_r := strings.NewReader(r.body)
		if resp,err := http.Post(ipstr,"application/json",io_r); err != nil {
			fmt.Fprintf(os.Stderr,"Request GET failed for %v: %v",ipstr,err)
		}else{
			defer resp.Body.Close()
			if body,err := io.ReadAll(resp.Body);err != nil{
				fmt.Fprintf(os.Stderr,"Response body not found")
				os.Exit(-1)
			}else{
				b.log_mnger.LazyLog(ipstr,string(body))
			}
		}
	case "get":
		if resp,err := http.Get(ipstr); err != nil {
			fmt.Fprintf(os.Stderr,"Request GET failed for %v: %v",ipstr,err)
		}else{
			defer resp.Body.Close()
			if body,err := io.ReadAll(resp.Body);err != nil{
				fmt.Fprintf(os.Stderr,"Response body not found")
				os.Exit(-1)
			}else{
				b.log_mnger.LazyLog(ipstr,string(body))
			}
		}
	case "put":
		io_r := strings.NewReader(r.body)
		if resp,err := http.NewRequest("PUT",ipstr,io_r); err != nil {
			fmt.Fprintf(os.Stderr,"Request GET failed for %v: %v",ipstr,err)
		}else{
			defer resp.Body.Close()
			if body,err := io.ReadAll(resp.Body);err != nil{
				fmt.Fprintf(os.Stderr,"Response body not found")
				os.Exit(-1)
			}else{
				b.log_mnger.LazyLog(ipstr,string(body))
			}
		}
	case "delete":
		io_r := strings.NewReader(r.body)
		if resp,err := http.NewRequest("DELETE",ipstr,io_r); err != nil {
			fmt.Fprintf(os.Stderr,"Request GET failed for %v: %v",ipstr,err)
		}else{
			defer resp.Body.Close()
			if body,err := io.ReadAll(resp.Body);err != nil{
				fmt.Fprintf(os.Stderr,"Response body not found")
				os.Exit(-1)
			}else{
				b.log_mnger.LazyLog(ipstr,string(body))
			}
		}
	default:
		return
	}
}




