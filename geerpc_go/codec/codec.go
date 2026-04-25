package codec

import (
	"io"
)

// 一个典型的 RPC 调度如下
// err = client.Call("Arith.Multiply", args, &reply)
// 服务名 Arith，方法名 Multiply，参数 args
// 服务端的响应包括错误 error，返回值 reply 2
// 我们将请求和响应中的参数和返回值抽象为 body，剩余的信息放在 header 中
type Header struct {
	ServiceMethod string // "Service.Method"
	Seq           uint64 // 请求的序号/ID，用于区分不同的请求
	Error         string // 错误信息，由服务端设置
	Metadata      map[string]string
}

// Codec 抽象消息编解码行为
type Codec interface {
	io.Closer                         // 继承 Close() error，关闭底层连接或资源
	ReadHeader(*Header) error         // 读 Header
	ReadBody(interface{}) error       // 读 Body（interface{}）
	Write(*Header, interface{}) error // 写 Header 和 Body（interface{}）
}

// Codec 的构造函数
// 类似于工厂模式，但返回的是构造函数而不是实例
type NewCodecFunc func(io.ReadWriteCloser) Codec

// Type 用于标识编解码协议类型。
// 它会出现在 Option 中，作为客户端和服务端协商结果的一部分。
type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json" // 预留的 JSON 类型，目前还没有对应实现
)

// NewCodecFuncMap 保存“协议类型 -> 构造函数”的映射关系。
// 客户端和服务端都会拿着 Option.CodecType 来这里查询真正的编解码器实现。
var NewCodecFuncMap map[Type]NewCodecFunc

func init() {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
