package consistent_hash

import "context"

type HashRing interface {
	// 锁住整个哈希环，在分布式场景下需要使用分布式锁
	Lock(ctx context.Context, expireSeconds int) error
	// 解锁哈希环
	Unlock(ctx context.Context) error
	// 将一个节点添加到哈希环中, 其中 virtualScore 为虚拟节点在哈希环中的位置，nodeID 为真实节点的 index
	Add(ctx context.Context, virtualScore int32, nodeID string) error
	//在哈希环中找到virtualScore 顺时针往下的第一个虚拟节点的位置
	Ceiling(ctx context.Context, virtualScore int32) (int32, error)
	// 在哈希环中好到 virtualScore 逆时针往上的第一个虚拟节点位置
	Floor(ctx context.Context, virtualScore int32) (int32, error)
	// 在哈希环 virtualScore 位置移除一个真实节点
	Rem(ctx context.Context, virtualScore int32, nodeID string) error
	// 查询哈希环中全量的真实节点，返回的结果为 map，其中 key 为真实节点 index，val 为真实节点对应的虚拟节点个数
	Nodes(ctx context.Context) (map[string]int, error)
	// 设置一个真实节点对应的虚拟节点个数，同时该操作背后的含义是将一个真实节点添加到一致性哈希模块中
	AddNodeToReplica(ctx context.Context, nodeID string, replicas int) error
	// 删除一个真实节点对应的虚拟节点个数，同时该操作背后的含义是将一个真实节点从一致性哈希模块中删除
	DeleteNodeToReplica(ctx context.Context, nodeID string) error
	// 查询哈希环 virtualScore 位置上对应的真实节点列表
	Node(ctx context.Context, virtualScore int32) ([]string, error)
	// 查询某个真实节点存储的状态数据的key集合
	DataKeys(ctx context.Context, nodeID string) (map[string]struct{}, error)
	// 将一系列状态数据的 key 添加与某个真实节点建立映射关系
	AddNodeToDataKeys(ctx context.Context, nodeID string, dataKeys map[string]struct{}) error
	// 将一系列状态数据的key删除与某个真实节点的映射关系
	DeleteNodeToDataKeys(ctx context.Context, nodeID string, dataKeys map[string]struct{}) error
}
