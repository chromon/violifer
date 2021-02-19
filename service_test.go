package violifer

import (
	"fmt"
	"reflect"
	"testing"
)

// 定义结构体 Foo，实现 2 个方法，导出方法 Sum 和非导出方法 sum

type Foo int

type Args struct {
	num1 int
	num2 int
}

func (f Foo) Sum(args Args, reply *int) error {
	*reply = args.num1 + args.num2
	return nil
}

func (f Foo) sum(args Args, reply *int) error {
	*reply = args.num1 + args.num2
	return nil
}

func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed:" + msg, v...))
	}
}

// 测试 newService 方法
func TestNewService(t *testing.T) {
	var foo Foo
	s := newService(&foo)
	_assert(len(s.method) == 1, "wrong service Method, expect 1, but got %d", len(s.method))
	mType := s.method["Sum"]
	_assert(mType != nil, "wrong Method, Sum shouldn't nil")
}

// 测试 call 方法
func TestMethodType_Call(t *testing.T) {
	var foo Foo
	s := newService(&foo)
	mType := s.method["Sum"]

	argv := mType.newArgv()
	replyv := mType.newReplyv()
	argv.Set(reflect.ValueOf(Args{num1: 1, num2: 3}))
	err := s.call(mType, argv, replyv)
	_assert(err == nil && *replyv.Interface().(*int) == 4 && mType.NumCalls() == 1, "failed to call Foo.Sum")
}