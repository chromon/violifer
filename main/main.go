package main

import (
	"log"
	"net"
	"sync"
	"time"
	"violifer"
)

type Foo int

type Args struct {
	Num1 int
	Num2 int
}

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.Num1 + args.Num2
	return nil
}

func startServer(addr chan string) {

	var foo Foo
	// 注册 Foo 到 Server
	if err := violifer.Register(&foo); err != nil {
		log.Fatal("register error:", err)
	}

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

			args := &Args{Num1: i, Num2: i * i}
			var reply int
			if err := client.Call("Foo.Sum", args, &reply); err != nil {
				log.Fatal("call Foo.Sum error:", err)
			}
			log.Printf("%d + %d = %d", args.Num1, args.Num2, reply)
		}(i)
	}
	wg.Wait()
}