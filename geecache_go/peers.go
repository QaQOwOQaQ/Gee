package geecache

import pb "geecache/geecachepb"

// PeerPicker 是节点选择器接口。
// 根据缓存 key 定位到负责该 key 的远程节点（peer）。
type PeerPicker interface {
	// PickPeer 根据 key 选择对应的远程节点。
	// 返回该节点的 PeerGetter 实现，以及是否找到了合适节点。
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// PeerGetter 是远程节点客户端接口。
// 每个远程节点都需要实现此接口，用于从该节点获取/失效缓存数据。
type PeerGetter interface {
	// Get 从远程节点获取指定 group 和 key 的缓存值。
	// 使用 protobuf 编码的请求和响应，减少网络传输开销。
	Get(in *pb.Request, out *pb.Response) error
	// Delete 请求远程节点失效指定 group 和 key 的缓存值。
	Delete(in *pb.Request) error
}
