package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// 服务注册中心，可以注册服务，支持心跳保活，返回所有可用服务，自动删除不可用服务
type Registry struct {
	// 默认超时时间 5 min，即任何注册的服务超过 5 min 视为不可用状态
	timeout time.Duration
	mutex sync.Mutex
	// 注册中心服务列表
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr string
	// 服务启动时间
	start time.Time
}

const (
	defaultPath = "/_rpc_/registry"
	defaultTimeout = time.Minute * 5
)

func New(timeout time.Duration) *Registry {
	return &Registry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultRegister = New(defaultTimeout)

// 添加服务实例，如果服务已经存在，则更新 start
func (r *Registry) putServer(addr string) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	s := r.servers[addr]
	if s == nil {
		// 服务不存在，添加
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		// 服务存在，更新启动时间
		s.start = time.Now()
	}
}

// 返回可用的服务列表，如果存在超时的服务，则删除
func (r *Registry) aliveServers() []string {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// Registry 采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中
// 运行在 /_rpc_/registry
func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// 返回所有可用的服务列表，通过自定义字段 X-rpc-Servers 承载
		w.Header().Set("X-rpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		// 添加服务实例或发送心跳，通过自定义字段 X-rpc-server
		addr := req.Header.Get("X-rpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default :
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HTTP handler for Registry messages on registryPath
func (r *Registry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultRegister.HandleHTTP(defaultPath)
}

// 心跳算法，用于服务启动时定时向注册中心发送心跳
// 默认周期比注册中心设置的过期时间少 1 min
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// 确保在服务从注册中心删除之前有足够的时间发送心跳
		duration = defaultTimeout - time.Duration(1) * time.Minute
	}

	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<- t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

// 发送心跳
func sendHeartbeat(registry string, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-rpc-Server", addr)
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}