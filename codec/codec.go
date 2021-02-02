package codec

import "io"

/*
RPC 调用过程：
err = client.Call("Arith.Multiply", args, &replay)
客户端发送的请求包括服务名 Arith，方法名 Multiply，参数 args，
服务端的响应包括错误 error 和返回值 reply
*/

// 发送请求的信息 header（参数和返回值存放在 body 中）
type Header struct {
	// 服务名和方法名
	ServiceMethod string
	// 序列号，请求的 ID
	Seq uint64
	// 服务端出错后返回的错误信息
	Error string
}

// 对消息体进行编解码的接口，抽象出来可以实现不同的 Codec
type Codec interface {
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
	io.Closer
}

// 抽象出 Codec 的构造函数，客户端和服务端可以通过 Codec 的 Type 得到构造函数，从而创建 Codec 实例
type NewCodecFunc func(closer io.ReadWriteCloser) Codec

type Type string

// 编码类型
const (
	GobType Type = "application/gob"
	JsonType Type = "application/json"
)

// 编码类型与构造函数映射关系 map
var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	// gob 编码与相关构造函数映射
	NewCodecFuncMap[GobType] = NewGobCodec
}