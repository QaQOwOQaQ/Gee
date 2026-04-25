### 编码器与解码器实现状态

- **已完成**：`gob` 格式
- **待实现**：`json` 格式

------

### `encoding/gob` 的使用规则与核心限制

在使用 Go 标准库的 `encoding/gob` 进行数据序列化和反序列化时，数据模型必须严格遵守以下三项主要限制：

**1. 仅支持可导出字段（Exported Fields）** 在结构体中，只有以**大写字母开头**的公开字段才会被 `gob` 引擎识别并处理。以小写字母开头的私有字段对 `gob` 是不可见的，会被直接忽略。

```go
// ✅ 可以被 gob 编解码（大写开头，可导出）
type Header struct {
    ServiceMethod string 
    Seq           uint64
    Error         string
}

// ❌ 小写字段会被忽略（gob 无法访问该字段）
type Bad struct {
    name string 
}
```

**2. 支持的数据类型存在局限性**

- **支持的类型**：基本数据类型（如 `int`、`string`、`bool`、`float` 等）、切片（Slice）、映射（Map）、结构体（Struct）等。
- **不支持的类型**：通道（Channel）、函数（Function）、非空接口（Interface）。

**3. 编解码两端的类型必须兼容** 发送方（编码端）和接收方（解码端）使用的结构体定义不需要百分之百相同，但是**同名字段的底层数据类型必须兼容**，否则在解析时会报错。