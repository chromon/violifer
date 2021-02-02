package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
	"violifer"
	"violifer/codec"
)

func startServer(addr chan string) {
	// 返回在 addr 上监听的 listener
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	// 将服务端监听地址写入到通道
	addr <- l.Addr().String()
	violifer.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	// 连接网络地址并返回 conn
	// 从通道中获取服务端地址
	conn, _ := net.Dial("tcp", <- addr)
	defer func() {
		_ = conn.Close()
	}()

	time.Sleep(time.Second)
	// 将 DefaultOption 的 json 编码写入 conn 中
	_ = json.NewEncoder(conn).Encode(violifer.DefaultOption)
	cc := codec.NewGobCodec(conn)

	for i := 0; i < 5; i++ {
		h := &codec.Header{
			ServiceMethod: "foo.Sum",
			Seq: uint64(i),
		}

		// 将数据写入到 conn
		_ = cc.Write(h, fmt.Sprintf("rpc req %d", h.Seq))
		_ = cc.ReadHeader(h)
		var reply string
		_ = cc.ReadBody(&reply)
		log.Println("reply:", reply)
	}

}