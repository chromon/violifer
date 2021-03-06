package violifer

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
	"violifer/codec"
)

// 标记这是一个 Violifer RPC 请求
const MagicNumber = 0x7a736b

// 客户端与服务端通信协议协商的相关信息（协议选项），如消息的编解码方式
type Option struct {
	MagicNumber int
	// 消息的编解码方式
	CodecType codec.Type
	// 连接超时时间，默认 10 秒
	ConnectTimeout time.Duration
	// 处理超时时间，默认值为 0， 即不设限
	HandleTimeout time.Duration
}

// 默认协议信息
var DefaultOption = &Option {
	MagicNumber: MagicNumber,
	CodecType: codec.GobType,
	ConnectTimeout: time.Second * 10,
}

/*
RPC 客户端固定采用 JSON 编码 Option，后续的 header 和 body 的编码方式由 Option 中的 CodeType 指定，
服务端首先使用 JSON 解码 Option，然后通过 Option 得 CodeType 解码剩余的内容。

即报文将以这样的形式发送：
| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|

在一次连接中，Option 固定在报文的最开始，Header 和 Body 可以有多个，即报文可能是这样的
| Option | Header1 | Body1 | Header2 | Body2 | ...
 */

// RPC Server
type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

// 默认 Server 实例
var DefaultServer = NewServer()

// 使 listener 接收每一个进来的连接和服务请求
func (server *Server) Accept(listener net.Listener) {
	for {
		// 等待 socket 建立连接
		conn, err := listener.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}

		// 将连接交给 ServerConn 处理
		go server.ServeConn(conn)
	}
}

func Accept (listener net.Listener) {
	DefaultServer.Accept(listener)
}

// 处理连接得到编解码器
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()

	var opt Option
	// json 反序列化 option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server - options error:", err)
		return
	}

	// 检查 MagicNumber 和 CodecType 是否正确
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server - invalid magic number %x", opt.MagicNumber)
		return
	}
	// 由 CodecType 得到对应的编解码器
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server - invalid codec type %s", opt.CodecType)
		return
	}

	// 根据对应编解码器处理请求
	server.serveCodec(f(conn), &opt)
}

var invalidRequest = struct{}{}

// 请求处理（读取、处理、响应）
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	// 处理请求是并发的，必须确保回复请求（加锁）发送一个完整响应报文（并发会导致报文交叉，无法解析）
	sendingMutex := new(sync.Mutex)
	// 等待直到所有请求都被处理
	wg := new(sync.WaitGroup)

	// 在一次连接中，允许接收多个请求，即多个 request header 和 request body
	for {
		// 读取请求
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				// 解析失败，关闭连接
				break
			}
			req.h.Error = err.Error()
			// 回复错误信息
			server.sendResponse(cc, req.h, invalidRequest, sendingMutex)
			continue
		}
		wg.Add(1)
		// 并发处理请求
		go server.handleRequest(cc, req, sendingMutex, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// 封装一个请求的所有信息 header 和 argv/replyv 组成的 body
type request struct {
	// 请求 header
	h *codec.Header
	// 请求的参数
	argv reflect.Value
	// 请求的返回值
	replyv reflect.Value
	// 方法实例
	mtype *methodType
	// service 实例
	svc *service
}

// 读取请求 header
func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	// 从输入流中读取下一个值并存储到 h 中
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server - read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

// 读取请求，得到 header 和 body 中的请求参数
func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}

	req := &request{h: h}
	// 将传入的 service 和 method 反射
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	// 分别创建两个入参实例：参数实例、返回值实例
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		// 确保 argvi 是指针，因为 ReadBody 方法需要一个指针作为参数
		argvi = req.argv.Addr().Interface()
	}

	// 通过 ReadBody 将请求报文反序列化为第一个入参 argvi
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server - read body err: ", err)
		return req, err
	}

	return req, nil
}

// 发送响应
func (server *Server) sendResponse(cc codec.Codec, h *codec.Header,
		body interface{}, sendingMutex *sync.Mutex) {
	sendingMutex.Lock()
	defer sendingMutex.Unlock()

	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server - write response error:", err)
	}
}

// 处理请求
// 与客户端连接超时类似，使用 time.After() 结合 select + chan 完成服务端超时处理
func (server *Server) handleRequest(cc codec.Codec, req *request,
		sendingMutex *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()

	// 为确保 sendResponse 仅调用一次，因此将整个过程拆分为 called 和 sent 两个阶段
	called := make(chan struct{})
	sent := make(chan struct{})

	go func() {
		// 调用注册的 rpc 方法得到返回值 replyv
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			server.sendResponse(cc, req.h, invalidRequest, sendingMutex)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sendingMutex)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<- called
		<- sent
		return
	}

	// called 信道接收到消息，代表处理没有超时，继续执行 sendResponse
	// time.After() 先于 called 接收到消息，说明处理已经超时，called 和 sent 都将被阻塞
	// 在 case <-time.After(timeout) 处调用 sendResponse
	select {
	case <- time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server - request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sendingMutex)
	case <- called:
		<- sent
	}
}

// 注册 service
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	// LoadOrStore(key, value) 如果 key 存在，则返回 key 对应的元素
	// 如果 key 不存在，则返回设置的 value，并将 value 存入 map 中
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc - service already defined:" + s.name)
	}
	return nil
}

// 默认注册 service
func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

// 通过 serviceMethod 从 serviceMap 中查找对应的 service
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		// service.method 格式错误
		err = errors.New("rpc server - service/method request ill-formed: " + serviceMethod)
		return
	}

	// 分割 service 和 method
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot + 1:]
	// serviceMap 中加载对应的 service 实例
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		// 加载失败，实例不存在
		err = errors.New("rpc server - can't find service: " + serviceName)
	}
	// 从 service 实例的 method 中，找到对应的 methodType
	svc = svci.(*service)
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server - can't find method: " + methodName)
	}
	return
}

// 服务端 HTTP 协议支持
const (
	connected = "200 Connected to RPC"
	defaultRPCPath = "/_rpc_"
	defaultDebugPath = "/debug/rpc"
)

// 实现 http.Handler 的 ServeHTTP 方法来回答 RPC 请求
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 只接受 HTTP CONNECT 请求
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}

	// Hijack 让调用者接管连接，返回连接和关联到该连接的一个缓冲读写器
	// 调用本方法后，HTTP 服务端将不再对连接进行任何操作，调用者有责任管理、关闭返回的连接
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr,": ", err.Error())
		return
	}
	// 返回了 200 状态码 HTTP/1.0 200 Connected to RPC
	_, _ = io.WriteString(conn, "HTTP/1.0 " + connected + "\n\n")
	server.ServeConn(conn)
}

// HandleHTTP 在 rpcPath 上为 RPC 消息注册了一个 HTTP 处理程序
func (server *Server) HandleHTTP() {
	// func Handle(pattern string, handler Handler)
	// 注册 HTTP 处理器 handler 和对应的模式 pattern（注册到 DefaultServeMux）
	http.Handle(defaultRPCPath, server)

	// 注册 debugging handler 在 debugPath
	http.Handle(defaultDebugPath, debugHTTP{server})
	log.Println("rpc server debug path:", defaultDebugPath)
}

func HandleHTTP() {
	DefaultServer.HandleHTTP()
}