package xclient

import (
	"context"
	"io"
	"reflect"
	"sync"
	// 使用 . 操作引入包时，可以省略包前缀
	. "violifer"
)

// 支持负载均衡的客户端
type XClient struct {
	// 服务发现所需要的基本方法接口
	d Discovery
	// 负载均衡策略
	mode SelectMode
	// 协议选项
	opt *Option
	mutex sync.Mutex
	// 保存创建成功的 Client 实例
	clients map[string]*Client
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{
		d:       d,
		mode:    mode,
		opt:     opt,
		clients: make(map[string]*Client),
	}
}

func (xc *XClient) Close() error {
	xc.mutex.Lock()
	defer xc.mutex.Unlock()

	for key, client := range xc.clients {
		_ = client.Close()
		delete(xc.clients, key)
	}
	return nil
}

func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mutex.Lock()
	defer xc.mutex.Unlock()

	// 检查 xc.clients 是否有缓存的 Client
	client, ok := xc.clients[rpcAddr]
	// 能够获取到客户端，但客户端不可用
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}

	// 没有返回缓存的 Client，则说明需要创建新的 Client，缓存并返回
	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context,
		serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

// 调用指定的函数，等待完成
func (xc *XClient) Call(ctx context.Context, serviceMethod string,
		args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast 将请求广播到所有的服务实例
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	var mutex sync.Mutex
	var e error

	replyDone := reply == nil
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()

			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)

			mutex.Lock()
			// 如果任意一个实例发生错误，则返回其中一个错误
			if err != nil && e == nil {
				e = err
				// 如果任何调用失败，则取消未完成的调用
				cancel()
			}
			// 如果调用成功，则返回其中一个的结果
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mutex.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}