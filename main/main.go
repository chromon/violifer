package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	"violifer"
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
	log.SetFlags(0)
	addr := make(chan string)
	go startServer(addr)

	client, _ := violifer.Dial("tcp", <- addr)
	defer func() {
		_ = client.Close()
	}()

	time.Sleep(time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			args := fmt.Sprintf("rpc req %d", i)
			var reply string
			if err := client.Call("Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Println("reply:", reply)
		}(i)
	}
	wg.Wait()
}