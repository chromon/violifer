package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// 负载均衡策略
type SelectMode int

const (
	// 随机选择策略，从服务列表中随机选择一个
	RandomSelect SelectMode = iota
	// 轮询策略，一次调度不同的服务器，每次调度执行 i = (i + 1) mode n
	RoundRobinSelect
)

// 服务发现所需要的基本方法接口
type Discovery interface {
	// 从注册中心更新服务列表
	Refresh() error
	// 手动更新服务列表
	Update(servers []string) error
	// 根据负载均衡策略选择一个服务实例
	Get(mode SelectMode) (string, error)
	// 返回所有服务实例
	GetAll() ([]string, error)
}

// 服务发现，不需要注册中心，服务列表由手动维护
type MultiServersDiscovery struct {
	// 随机数
	random *rand.Rand
	mutex sync.RWMutex
	servers []string
	// 记录轮询算法已经轮询到的位置，为了避免每次都从 0 开始，初始化时随机设定一个值
	index int
}

func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	discovery := &MultiServersDiscovery{
		// 初始化时使用时间戳设定随机数种子，避免每次产生相同的随机数序列
		random:  rand.New(rand.NewSource(time.Now().UnixNano())),
		servers: servers,
	}
	// 随机初始化轮询算法位置
	discovery.index = discovery.random.Intn(math.MaxInt32 - 1)
	return discovery
}

var _ Discovery = (*MultiServersDiscovery)(nil)

// 由于没有注册中心，服务列表手动维护，所以刷新没有意义
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// 更新服务
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.servers = servers
	return nil
}

// 根据负载均衡策略选择一个服务实例
func (d MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	n := len(d.servers)
	// 不存在可以服务节点
	if n == 0 {
		return "", errors.New("rpc discovery - no available servers")
	}

	switch mode {
	case RandomSelect:
		// 从服务列表中随机选择一个
		return d.servers[d.random.Intn(n)], nil
	case RoundRobinSelect:
		// 随机的轮询位置第一次使用需要取模
		s := d.servers[d.index % n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery - not supported select mode")
	}
}

// 返回所有服务实例
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	// 复制（浅拷贝） d.servers, 防止被外部访问到
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}