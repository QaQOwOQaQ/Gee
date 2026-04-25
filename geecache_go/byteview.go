package geecache

// ByteView 表示缓存值的只读字节视图，封装了一个不可变的字节切片。
// 对外暴露只读方法，防止外部代码直接修改缓存中的数据。
type ByteView struct {
	b []byte // 实际存储的字节数据（私有，外部不可直接访问）
}

// Len 返回字节视图的长度（字节数），实现了 lru.Value 接口。
func (v ByteView) Len() int {
	return len(v.b)
}

// ByteSlice 返回数据的字节切片副本。
// 返回副本而非原始切片，保证外部修改不会影响缓存内部数据。
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

// String 将字节数据转换为字符串并返回。
func (v ByteView) String() string {
	return string(v.b)
}

// cloneBytes 创建并返回字节切片 b 的深拷贝，用于防止数据被外部修改。
// ByteView 的本质就是以时间（深拷贝）换取安全性
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
