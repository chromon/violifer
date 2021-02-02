package violifer

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"reflect"
	"sync"
	"violifer/codec"
)

// 标记这是一个 Violifer RPC 请求
const MagicNumber = 0x7a736b

// 客户端与服务端通信协议协商的相关信息，如消息的编解码方式
type Option struct {
	MagicNumber int
	// 消息的编解码方式
	CodecType codec.Type
}

// 默认协议信息
var DefaultOption = &Option {
	MagicNumber: MagicNumber,
	CodecType: codec.GobType,
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
type Server struct {}

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
	server.serveCodec(f(conn))
}

var invalidRequest = struct{}{}

// 请求处理（读取、处理、响应）
func (server *Server) serveCodec(cc codec.Codec) {
	// 处理请求是并发的，必须确保回复请求（加锁）发送一个完整响应报文（并发会导致报文交叉，无法解析）
	sendingMutex := new(sync.Mutex)
	// 等待直到所有请求都被处理
	wg := new(sync.WaitGroup)

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
		go server.handleRequest(cc, req, sendingMutex, wg)
	}
	wg.Wait()
	_ = cc.Close()
}

// 封装一个请求的所有信息
type request struct {
	// 请求 header
	h *codec.Header
	// 请求的参数
	argv reflect.Value
	// 请求的返回值
	replyv reflect.Value
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

	// TODO 假定请求参数类型是 string
	req.argv = reflect.New(reflect.TypeOf(""))
	if err = cc.ReadBody(req.argv.Interface()); err != nil {
		log.Println("rpc server - read argv err:", err)
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
func (server *Server) handleRequest(cc codec.Codec, req *request,
		sendingMutex *sync.Mutex, wg *sync.WaitGroup) {
	// TODO 调用注册的 rpc 方法得到返回值 replyv
	defer wg.Done()
	log.Println(req.h, req.argv.Elem())
	req.replyv = reflect.ValueOf(fmt.Sprintf("rpc resp %d", req.h.Seq))
	server.sendResponse(cc, req.h, req.replyv.Interface(), sendingMutex)
}