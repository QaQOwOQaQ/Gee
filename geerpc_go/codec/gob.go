package codec

// GobCodec 的读写模型：
//
//	Write(Header, Body)
//	  │
//	  ├── gob 编码 Header
//	  ├── gob 编码 Body
//	  └── Flush 到底层连接
//
//	ReadHeader / ReadBody
//	  │
//	  └── 按同样顺序从连接中依次解码
//
// Gob 是 GeeRPC 默认的消息编码方式，原因很简单：
// 1. 它是 Go 标准库自带的二进制编码方案
// 2. 非常适合这个教学项目快速实现“结构化对象 <-> 字节流”的转换
// 3. 不需要引入额外依赖，就能把协议层重点放在 RPC 本身而不是序列化细节上

import (
	"bufio"
	"encoding/gob"
	"io"
	"log"
)

// GobCodec 是基于 gob 的 Codec 实现（上层封装）
// 读操作直接从底层连接解码；写操作会先进入缓冲区，再统一 Flush 到网络，
// 这样可以减少小块写入带来的系统调用开销。
type GobCodec struct {
	conn io.ReadWriteCloser // conn 是由构建函数传入，通常是通过 TCP 或者 Unix 建立 socket 时得到的链接实例，dec 和 enc 对应 gob 的 Decoder 和
	buf  *bufio.Writer      // 为了防止阻塞而创建的带缓冲的 Writer，一般这么做能提升性能。
	dec  *gob.Decoder       // decoder
	enc  *gob.Encoder       // encoder
}

var _ Codec = (*GobCodec)(nil)

// NewGobCodec 根据双向连接创建一个 gob 编解码器。
// 注意这里读写分别使用 Decoder / Encoder，且写侧包了一层 bufio.Writer，
// 这样可以把一次消息的多个小写操作合并后再刷到网络。

// 读（dec ← conn）：解码时直接从连接读就行，数据是对方已经发过来的完整字节流，不需要缓冲，
// gob.Decoder 会自己按需从 conn 中读取。

// 写（enc ← buf）：编码时一次 Write 调用会先编码 Header、再编码 Body，这会产生多次小块写入。
// 如果直接写 conn，每次小写入都会触发一次系统调用（syscall），效率很低。
// 所以中间加了一层 bufio.Writer：
//
//	enc.Encode(h)    →  写入 buf（内存）
//	enc.Encode(body) →  写入 buf（内存）
//	buf.Flush()      →  一次性刷到 conn（网络）
//
// 这样多次小写合并成一次网络写入，减少系统调用开销，提升性能。
func NewGobCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &GobCodec{
		conn: conn,
		buf:  buf,
		dec:  gob.NewDecoder(conn), // 读：直接从 conn 读
		enc:  gob.NewEncoder(buf),  // 写：写入 buf
	}
}

// ReadHeader 读取并解码一条 RPC 头部。
// 头部总是先于消息体读取，因为上层需要先知道这条消息属于哪个方法、哪个请求序号。
func (c *GobCodec) ReadHeader(h *Header) error {
	return c.dec.Decode(h)
}

// ReadBody 读取并解码一条 RPC 消息体。
// 上层会在 Header 解析完成后，按预先准备好的目标对象类型把 body 解进去。
func (c *GobCodec) ReadBody(body interface{}) error {
	return c.dec.Decode(body)
}

// Write 按照“头部在前、消息体在后”的顺序写出一条完整消息。
// 无论成功失败都会尝试 Flush；如果编码失败，则顺手关闭底层连接，
// 避免继续在一个可能已经损坏的流上读写。
func (c *GobCodec) Write(h *Header, body interface{}) (err error) {
	defer func() {
		_ = c.buf.Flush()
		if err != nil {
			_ = c.Close()
		}
	}()
	if err = c.enc.Encode(h); err != nil {
		log.Println("rpc: gob error encoding header:", err)
		return
	}
	if err = c.enc.Encode(body); err != nil {
		log.Println("rpc: gob error encoding body:", err)
		return
	}
	return
}

// Close 关闭底层连接。
func (c *GobCodec) Close() error {
	return c.conn.Close()
}
