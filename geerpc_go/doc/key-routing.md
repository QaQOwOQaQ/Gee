# CallByKey 与一致性哈希使用说明

`XClient.CallByKey` 适合把“同一个业务实体”的请求稳定地路由到同一个服务节点。

## 什么时候适合使用

- 用户维度路由：`user_id`
- 租户维度路由：`tenant_id`
- 分片维度路由：`shard_key`
- 会话维度路由：`session_id`

这些 key 的共同点是：在一段时间内相对稳定，同一个实体会重复使用同一个值。

## 什么时候不适合使用

- 请求 ID：`request_id`
- 当前时间戳：`timestamp`
- 随机数：`rand`
- 每次调用动态拼出来、不会重复出现的临时字符串

这些 key 每次都变化，结果是请求会不断被打散到不同节点，无法形成稳定路由。

## 当前实现的约束

- 空 key 会被直接拒绝，避免所有流量退化到同一个固定路由。
- 当前项目通过一致性哈希做按 key 路由。
- 当服务节点发生增删时，只会有一部分 key 被重新映射，而不是全部 key 一起重排。
- `CallByKey` 当前不会自动接入故障转移。
- 这样做是为了优先保住“稳定业务 key 尽量稳定落到同一节点”的语义边界。
- 如果你的场景更看重可用性而不是稳定路由，当前推荐优先使用普通 `Call`，并显式开启 `XClient` 的 failover。

## 代码入口

- `XClient.CallByKey`：`xclient/xclient.go`
- `ConsistentHashDiscovery.GetByKey`：`xclient/discovery_consistent_hash.go`
- 基于注册中心的动态按 key 路由：`xclient/discovery_gee.go`
- 运行示例：`main/main.go`

## 示例

推荐：

```go
var reply string
err := xc.CallByKey(ctx, tenantID, "Foo.Identity", &Args{}, &reply)
```

不推荐：

```go
var reply string
err := xc.CallByKey(ctx, requestID, "Foo.Identity", &Args{}, &reply)
```

上面这种写法虽然能跑，但因为 `requestID` 每次都不同，无法体现一致性哈希的价值。
