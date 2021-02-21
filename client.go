package violifer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"violifer/codec"
)

// 远程调用方式
// err = client.Call("T.MethodName", args, &replay)
// 一个函数能够被远程调用，需要满足如下格式
// func (t *T) MethodName(argType T1, replyType *T2) error

// 一次 RPC 调用所需要的全部信息
type Call struct {
	// 序列号
	Seq uint64
	// <服务名>.<方法名>
	ServiceMethod string
	// 参数
	Args interface{}
	// 返回值
	Reply interface{}
	// 错误信息
	Error error
	// 支持异步调用管道
	Done chan *Call
}

// 当调用结束时，调用 done 方法同时调用方
func (call *Call) done() {
	call.Done <- call
}

// RPC 客户端
// 一个客户端可以关联多个 RPC 调用，一个客户端也可能被多个协程同时使用
type Client struct {
	// 消息编解码器，用于序列化将要发送的请求，以及反序列化接收的响应
	cc codec.Codec
	// 通信相关协议信息
	opt *Option
	// 互斥锁，保证请求有序发送，防止多个请求报文混淆
	sendingMutex sync.Mutex
	// 请求消息头
	header codec.Header
	// 互斥锁，用于保证客户端相关操作加锁
	mutex sync.Mutex
	// 序列号，用于给发送的请求编号，每个请求都有唯一编号
	seq uint64
	// 存储未完成的请求，序列号为键
	pending map[uint64]*Call
	// 表明客户端不可用，用户主动关闭的（调用完 Close 方法）
	closing bool
	// 表明客户端不可用，有错误发生，被动关闭
	shutdown bool
}

// 创建 client 实例
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	// 读取编解码器构造函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client - codec error:", err)
		return nil, err
	}

	// RPC 客户端固定采用 JSON 编码协议交换信息 Option，并发送到 conn 中
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client - options error:", err)
		// 关闭连接
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil

}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client {
		// 序列号从 1 开始，0 表示无效 call
		seq: 1,
		cc: cc,
		opt: opt,
		pending: make(map[uint64]*Call),
	}

	// 创建子协程调用 receive 方法接收响应
	go client.receive()
	return client
}

var _ io.Closer = (*Client)(nil)

var ErrShutdown = errors.New("connection is shutdown")

// 关闭连接
func (client *Client) Close() error {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.closing {
		return ErrShutdown
	}

	client.closing = true
	return client.cc.Close()
}

// 检查客户端是否可用
func (client *Client) IsAvailable() bool {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	return !client.shutdown && !client.closing
}

// 将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}

	// 为请求序列号赋值
	call.Seq = client.seq
	// 将请求添加到存储尚未完成请求 map 中
	client.pending[call.Seq] = call
	// 客户端序列号更新
	client.seq++
	return call.Seq, nil
}

// 根据 call.seq，从 client.pending 中移除对应的 call，并返回 call
func (client *Client) removeCall(seq uint64) *Call {
	client.mutex.Lock()
	defer client.mutex.Unlock()

	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 服务端或客户端发生错误时调用，将 shutdown 设置为 true，且将错误信息通知所有 pending 状态的 call
func (client *Client) terminateCalls(err error) {
	client.sendingMutex.Lock()
	defer client.sendingMutex.Unlock()

	client.mutex.Lock()
	defer client.mutex.Unlock()

	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

// 客户端接收响应
func (client *Client) receive() {
	var err error
	for err == nil {
		// 响应 header
		var h codec.Header
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}

		// 移除已响应完成的请求
		call := client.removeCall(h.Seq)
		switch {
		case call == nil:
			// 请求 call 不存在，可能是请求没有发送完整，或者因为其他原因被取消
			// 但是服务端依然完成请求处理
			err = client.cc.ReadBody(nil)
		case h.Error != "":
			// 请求 call 存在，但服务端处理出错，h.Error 不为空
			// 返回给 call 错误信息，并结束请求
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		default:
			// 请求 call 存在，服务端正常处理，可以从 body 中读取 reply 值
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}

	// 接收请求出错，终止
	client.terminateCalls(err)
}

// 处理用户传入的 option 信息
func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		// 用户输入的 opt 信息为空，使用默认 option
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}

	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

// 用于处理客户端连接超时封装了客户端和超时错误信息
type clientResult struct {
	client *Client
	err error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

// 处理连接超时
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	// 解析 options
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}

	// 建立与 RPC 服务端连接，net.Dial 替换为 net.DialTimeout，如果连接创建超时，将返回错误
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	// 如果新建客户端为 nil，关闭连接
	// defer 语句执行在 return 语句赋值之后，方法结束之前
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()

	// 连接超时不设限，直接返回结果
	if opt.ConnectTimeout == 0 {
		result := <- ch
		return result.client, result.err
	}

	select {
	case <- time.After(opt.ConnectTimeout):
		// 连接超时
		return nil, fmt.Errorf("rpc client - connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <- ch:
		return result.client, result.err
	}
}

// 使用用户传入的服务端地址信息，创建 Client 实例
func Dial(network, address string, opts ...*Option) (client *Client, err error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// 发送请求
func (client *Client) send(call *Call) {
	// 确保 client 能够发送完整请求
	client.sendingMutex.Lock()
	defer client.sendingMutex.Unlock()

	// 注册请求
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备请求 header
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 编码并发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)

		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// RPC 服务调用接口，是一个异步接口，返回 call 实例
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client - done channel is unbuffered")
	}

	call := &Call {
		ServiceMethod: serviceMethod,
		Args: args,
		Reply: reply,
		Done: done,
	}

	client.send(call)
	return call
}

// Call 是对 Go 的封装，阻塞 call.Done，等待响应返回，是一个同步接口
// Client.Call 的超时处理机制，使用 context 包实现，控制权交给用户，控制更为灵活
// 用户可以使用 context.WithTimeout 创建具备超时检测能力的 context 对象来控制
// 例如：
// ctx, _ := context.WithTimeout(context.Background(), time.Second)
// var reply int
// err := client.Call(ctx, "Foo.Sum", &Args{1, 2}, &reply)
func (client *Client) Call(ctx context.Context, serviceMethod string , args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <- ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client - call failed: " + ctx.Err().Error())
	case call := <- call.Done:
		return call.Error
	}
}

// 客户端 HTTP 协议支持
// 服务端已经能够接受 CONNECT 请求，并返回了 200 状态码 HTTP/1.0 200 Connected to RPC
// 客户端要做的，发起 CONNECT 请求，检查返回状态码即可成功建立连接

// 通过 HTTP 协议创建客户端实例
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))

	// HTTP 协议转换为 RPC 协议之前，需要服务端成功的 HTTP 响应
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response:" + resp.Status)
	}
	return nil, err
}

// 连接到指定的 HTTP RPC server，并在默认的 HTTP RPC path 监听
func DialHTTP(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// 根据 RPCAddr 调用不同函数连接 RPC server
// rpcAddr 格式： protocol@addr
// http@10.0.0.1:7001, tcp@10.0.0.1:8001
func XDial(rpcAddr string, opts ...*Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}

	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		// http 协议
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp，unix 等协议
		return Dial(protocol, addr, opts...)
	}

}