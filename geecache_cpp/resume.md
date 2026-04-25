```tex
\datedsubsection{\textbf{1.分布式缓存系统}}{2024.03 - 2024.05}
\Content
{\textbf{项目简介：}基于 C++ 实现分布式应用内缓存组件，支持多节点部署、基于 key 的 owner 路由、peer 间 HTTP 通信与分布式失效广播，用于降低后端数据源压力并提升热点读请求性能。}
{\textbf{一致性哈希路由：}设计并实现一致性哈希与虚拟节点机制，将 key 稳定映射到 owner 节点，在节点增删时减少重映射范围，改善 key 分布均匀性并缓解缓存抖动。}
{\textbf{热点副本优化：}针对非 owner 节点频繁跨节点读取的问题，引入 hotCache 本地副本策略，对远端读取结果按 key 做概率性回填，减少重复跨节点访问开销。}
{\textbf{并发控制：}实现 singleflight 请求合并，同一 key 的并发 miss 仅触发一次加载；缓存层基于读写锁封装并发安全访问，避免高并发下重复回源与状态竞争。}
{\textbf{一致性保障：}采用 Cache Aside 写路径，更新后删除本地与远端缓存；针对失效与加载并发导致的旧值回填问题，引入版本校验机制，并结合 TTL 兜底缩短脏数据窗口。}
```


这个项目是一个基于 C++ 实现的 <font color=blue>分布式应用内缓存组件</font>，目标是把高频读请求尽量拦在缓存层，减少后端数据源压力。

整体上它支持 <font color=blue>多节点部署</font>，节点之间通过 <font color=blue>一致性哈希</font> 做 <font color=blue>owner 路由</font>，并引入 <font color=blue>虚拟节点机制</font> 来改善 key 分布均匀性。读请求会先查本地缓存，如果 miss，就根据 key 找 owner 节点；如果 owner 在远端，就通过 <font color=blue>HTTP</font> 去远端取值，否则就本地回源加载。

为了处理高并发场景，我用 <font color=blue>singleflight</font> 合并同一个 key 的并发 miss，避免 <font color=blue>缓存击穿</font>；本地缓存访问则用 <font color=blue>读写锁</font> 封装，保证并发安全。

除此之外，我还实现了一个 <font color=blue>hotCache</font>，用来对远端读回的数据做概率性本地副本回填，减少后续重复跨节点访问。

在一致性上，这个项目采用的是 <font color=blue>Cache Aside 写路径</font>，数据更新后会主动失效本地和远端缓存；针对删除与加载并发导致的旧值回填问题，我加了 <font color=blue>版本校验</font> 来保证回填安全；同时我也在缓存层加入了 <font color=blue>TTL 兜底机制</font>，即使失效通知存在延迟或异常，旧数据也会在过期后自动淘汰。这部分我也写了测试做验证。

----

项目简介：基于 C++ 实现的 <font color=blue>分布式应用内缓存</font>，支持 <font color=blue>多节点部署</font>、<font color=blue>节点间数据路由</font>、<font color=blue>远端节点访问</font> 与 <font color=blue>分布式失效广播</font>，目标是降低后端数据源压力并提升热点请求响应。

<font color=blue>一致性哈希路由</font>：设计并实现 <font color=blue>一致性哈希</font> 与 <font color=blue>虚拟节点机制</font>，将 key 映射到 owner 节点；在节点增删时减少整体重映射范围，改善 key 分布均匀性并缓解缓存抖动。

<font color=blue>远端数据本地副本</font>：针对非 owner 节点读取需跨节点访问的问题，引入 <font color=blue>hotCache 本地副本策略</font>；在远端读取成功后按 key 做概率性回填，减少重复跨节点请求并提升后续访问命中率。

<font color=blue>并发与击穿防护</font>：实现 <font color=blue>singleflight 请求合并</font>，同一 key 的并发 miss 仅触发一次回源加载；缓存层基于读写锁封装并发安全访问，避免高并发下重复回源和状态竞争。

<font color=blue>一致性与写路径控制</font>：采用 <font color=blue>Cache Aside 写路径</font>，数据更新后通过失效接口删除本地与远端缓存；针对删除与加载并发导致的旧值回填问题加入 <font color=blue>版本校验</font> 保证回填安全，同时在缓存层增加 <font color=blue>TTL 兜底机制</font>，在失效广播延迟或异常时限制旧数据停留时间。
