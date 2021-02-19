package violifer

import (
	"go/ast"
	"log"
	"reflect"
	"sync/atomic"
)

// 通过反射实现结构体与服务的映射关系
// 服务.方法

// methodType 实例包含了一个方法的完整信息
// func (t *T) MethodName(argType T1, replyType *T2) error
type methodType struct {
	// 方法本身实例
	method reflect.Method
	// 第一个参数（参数 Type）
	ArgType reflect.Type
	// 第二个参数（返回值 Type）
	ReplyType reflect.Type
	// 统计方法调用次数
	numCalls uint64
}

func (m *methodType) NumCalls() uint64 {
	return atomic.LoadUint64(&m.numCalls)
}

// 创建参数实例
func (m *methodType) newArgv() reflect.Value {
	var argv reflect.Value
	if m.ArgType.Kind() == reflect.Ptr {
		// 参数值是指针类型，通过 reflect.Elem() 方法获取这个指针指向的元素类型
		argv = reflect.New(m.ArgType.Elem())
	} else {
		// 参数类型为值类型，使用 reflect.New() 方法
		argv = reflect.New(m.ArgType).Elem()
	}
	return argv
}

// 创建返回值实例
func (m *methodType) newReplyv() reflect.Value {
	// 返回值必须是指针类型
	replyv := reflect.New(m.ReplyType.Elem())

	switch m.ReplyType.Elem().Kind() {
	case reflect.Map:
		replyv.Elem().Set(reflect.MakeMap(m.ReplyType.Elem()))
	case reflect.Slice:
		replyv.Elem().Set(reflect.MakeSlice(m.ReplyType.Elem(), 0, 0))
	}

	return replyv
}

// 服务
type service struct {
	// 映射的结构体名称，T
	name string
	// 映射的结构体类型
	typ reflect.Type
	// 映射的结构体实例本身，在调用的时候需要作为第 0 个参数
	rcvr reflect.Value
	// 存储映射的结构体的所有符合条件的方法
	method map[string]*methodType
}

// 构造 service
// rcvr 是任意需要映射为服务的结构体的实例
func newService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.name = reflect.Indirect(s.rcvr).Type().Name()
	s.typ = reflect.TypeOf(rcvr)

	if !ast.IsExported(s.name) {
		log.Fatalf("rpc server - %s is not a valid service name", s.name)
	}

	s.registerMethods()
	return s
}

// 遍历所有符合条件的方法，存储到 method 中
func (s *service) registerMethods() {
	s.method = make(map[string]*methodType)
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type

		if mType.NumIn() != 3 || mType.NumOut() != 1 {
			// 两个导出或内置类型的入参，反射时为 3 个，第 0 个是自身
			// 类似于 python 的 self，java 中的 this
			// 返回值有且只有 1 个，类型为 error
			continue
		}

		if mType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
			// 返回值类型必须为 error
			continue
		}

		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			// 参数和返回值必须是可导出的
			continue
		}

		s.method[method.Name] = &methodType {
			method: method,
			ArgType: argType,
			ReplyType: replyType,
		}
		log.Printf("rpc server - register %s.%s\n", s.name, method.Name)
	}
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	// PkgPath 是非导出字段的包路径，对导出字段该字段为 ""
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}

// 通过反射值调用方法
func (s *service) call(m *methodType, argv, replyv reflect.Value) error {
	atomic.AddUint64(&m.numCalls, 1)
	// Func Value 表示方法的值
	f := m.method.Func
	// func (v Value) Call(in []Value) []Value
	// Call 方法使用输入的参数 in 调用 v 持有的函数，如果 v 的 Kind 不是 Func 会 panic
	// 返回函数所有输出结果的 Value 封装的切片
	returnValues := f.Call([]reflect.Value{s.rcvr, argv, replyv})
	// func (v Value) Interface() (i interface{})
	// 返回 v 当前持有的值（表示为/保管在 interface{} 类型）
	if errInter := returnValues[0].Interface(); errInter != nil {
		return errInter.(error)
	}
	return nil
}