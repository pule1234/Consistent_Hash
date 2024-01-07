package consistent_hash

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
)

type ConsistentHash struct {
	// 哈希环，是核心存储模块，包括虚拟节点到真实节点的映射关系，真实节点对应的虚拟节点个数，以及哈希环上各个节点的位置
	hashRing HashRing
	//数据迁移器， 当节点数量发生变更时，会使用迁移器完成数据的迁移
	migrator Migrator
	// 哈希散列函数，用于将原始输入内容映射到哈希环上的摸个位置
	encryptor Encryptor
	// 用于自定义配置项
	opts ConsistentHashOptions
}

func NewConsistentHash(hashRing HashRing, encryptor Encryptor, migrator Migrator, opts ...ConsistentHashOption) *ConsistentHash {
	ch := ConsistentHash{
		hashRing:  hashRing,
		migrator:  migrator,
		encryptor: encryptor,
	}

	for _, opt := range opts {
		opt(&ch.opts)
	}

	repair(&ch.opts)
	return &ch
}

// 添加节点触发数据迁移
// 1加锁，  2 校验节点是否存在，  3 通过传入的权重值确定对应的虚拟节点个数（replicas） 4 添加虚拟节点 5 执行数据迁移
func (c *ConsistentHash) AddNode(ctx context.Context, nodeID string, weight int) error {
	// 加全局分布式锁
	if err := c.hashRing.Lock(ctx, c.opts.lockExpireSeconds); err != nil {
		return err
	}

	defer func() {
		_ = c.hashRing.Unlock(ctx)
	}()

	// 如果节点已经存在，直接返回重复添加节点的错误
	nodes, err := c.hashRing.Nodes(ctx)
	if err != nil {
		return err
	}

	for node := range nodes {
		if node == nodeID {
			return errors.New("repeat node")
		}
	}

	// 根据用户传入的节点的权重值weight以及配置项中配置好放大系数replicas 计算出这个真实节点对应的虚拟节点的个数
	replicas := c.getValidWeight(weight) * c.opts.replicas

	// 将计算得到的replicas个数与nodeID 的映射关系放到hash ring 中， 同时也能标识出当前nodeID已经存在
	if err = c.hashRing.AddNodeToReplica(ctx, nodeID, replicas); err != nil {
		return err
	}

	// 按照虚拟节点的个数将虚拟节点添加到哈希环中
	var migraeTasks []func()
	for i := 0; i < replicas; i++ {
		// 使用encryptor推算出对应的k个虚拟节点的数值
		nodeKey := c.getRawNodeKey(nodeID, i)
		virtualScore := c.encryptor.Encrypt(nodeKey)

		// 将一个虚拟节点添加到hash ring当中
		if err := c.hashRing.Add(ctx, virtualScore, nodeKey); err != nil {
			return err
		}

		// 调用migrateIn方法，获取需要执行的数据迁移任务信息
		// from 数据迁移起点的节点id
		// to 数据迁移终点的节点id
		// data : 需要迁移的状态数据的key
		from, to, datas, err := c.migrateIn(ctx, virtualScore, nodeID)
		if err != nil {
			return err
		}

		// 倘若等待迁移的数据长度为0 ，直接跳过
		if len(datas) == 0 {
			continue
		}
		// 数据迁移任务不是立即执行，只是追加到list中，最后会在batchExecuteMigrator方法中一起执行
		migraeTasks = append(migraeTasks, func() {
			_ = c.migrator(ctx, datas, from, to)
		})
	}

	// 批量执行数据迁移任务
	c.batchExecuteMigrator(migraeTasks)
	return nil
}

// 删除节点 也会造成数据迁移
// 1加锁，  2 检验哈希环是否存在， 3 获取对应虚拟节点的个数  4 一次删除虚拟节点  5 执行数据迁移
func (c *ConsistentHash) RemoveNode(ctx context.Context, nodeID string) error {
	if err := c.hashRing.Lock(ctx, c.opts.lockExpireSeconds); err != nil {
		return err
	}

	defer func() {
		_ = c.hashRing.Unlock(ctx)
	}()

	// 查询哈希环中所有存在的节点
	nodes, err := c.hashRing.Nodes(ctx)
	if err != nil {
		return err
	}

	// 检验待删除的节点是否存在
	var (
		nodeExist bool
		replicas  int
	)
	for node, _replicas := range nodes {
		if node == nodeID {
			nodeExist = true
			replicas = _replicas
			break
		}
	}

	// 如果删除的节点不存在，直接返回
	if !nodeExist {
		return errors.New("invalid node id")
	}

	// 从哈希环中删除节点与虚拟节点个数的映射信息，这个操作背后的含义就是从哈希环中删除这个真实节点
	if err = c.hashRing.DeleteNodeToReplica(ctx, nodeID); err != nil {
		return err
	}

	var migrateTasks []func()
	// 根据真实节点对应的虚拟节点个数，开始执行对应虚拟节点的删除操作
	for i := 0; i < replicas; i++ {
		//使用encrptor，推算出对应的k个虚拟节点数值
		virtualScore := c.encryptor.Encrypt(fmt.Sprintf("%d_%d", nodeID, i))
		// 调用migrateout方法，获取迁移任务明细
		from, to, datas, err := c.migrateOut(ctx, virtualScore, nodeID)
		if err != nil {
			return err
		}

		// 从哈希环对应虚拟节点数值virtualScore的位置删除这个真实节点nodeID
		nodeKey := c.getRawNodeKey(nodeID, i)
		if err = c.hashRing.Rem(ctx, virtualScore, nodeKey); err != nil {
			return err
		}

		// 倘若待迁移的数据长度为0，则直接跳过
		if len(datas) == 0 {
			continue
		}

		migrateTasks = append(migrateTasks, func() {
			_ = c.migrator(ctx, datas, from, to)
		})

	}
	c.batchExecuteMigrator(migrateTasks)
	return nil

}

func (c *ConsistentHash) batchExecuteMigrator(migrateTasks []func()) {
	// 执行所有数据迁移任务
	var wg sync.WaitGroup
	for _, migrateTask := range migrateTasks {
		migrateTask := migrateTask
		wg.Add(1)
		go func() {
			defer func() {
				if err := recover(); err != nil {
					log.Fatal(err)
				}
				wg.Done()
			}()
			migrateTask()
		}()
	}
	wg.Wait()
}

// 执行一笔状态数据的读写请求时，需要通过一致性哈希模块，检索到数据所对应的真实节点
// 1 加锁， 2 通过hash编码器，找到数据在哈希环上的位置  3 找到顺时针往下的第一个虚拟节点   4 找到虚拟节点对应的真实节点  5 建立真实节点与状态数据之间的映射关系
func (c *ConsistentHash) GetNode(ctx context.Context, dataKey string) (string, error) {
	if err := c.hashRing.Lock(ctx, c.opts.lockExpireSeconds); err != nil {
		return "", err
	}

	defer func() {
		_ = c.hashRing.Unlock(ctx)
	}()

	//输入一个数据的key 根据encryptor计算出其从属与哈希环的位置dataScore
	dataScore := c.encryptor.Encrypt(dataKey)
	// 执行ceiling 找到当前datakey对应dataScore的下一个虚拟节点数值ceilingScore
	ceilingScore, err := c.hashRing.Ceiling(ctx, dataScore)
	if err != nil {
		return "", err
	}

	// 倘若未找到目标，则说明没有可用的目标节点
	if ceilingScore == -1 {
		return "", errors.New("no node available")
	}

	// 查询ceilingScore对应的真实节点列表
	nodes, err := c.hashRing.Node(ctx, ceilingScore)
	if err != nil {
		return "", err
	}

	// 倘若真实节点列表为空直接返回错误
	if len(nodes) == 0 {
		return "", errors.New("no node available with empty score")
	}

	// 为datakey选中真实节点后， 需要将datakey添加到真实节点的状态数据key列表中
	if err = c.hashRing.AddNodeToDataKeys(ctx, c.getNodeID(nodes[0]), map[string]struct{}{
		dataKey: {},
	}); err != nil {
		return "", err
	}

	//返回选中的目标节点
	return nodes[0], nil
}

func (c *ConsistentHash) getValidWeight(weight int) int {
	if weight <= 0 {
		return 1
	}

	if weight >= 10 {
		return 10
	}

	return weight
}

func (c *ConsistentHash) getRawNodeKey(nodeID string, index int) string {
	return fmt.Sprintf("%d_%d", nodeID, index)
}

func (c *ConsistentHash) getNodeID(rawNodeKey string) string {
	index := strings.LastIndex(rawNodeKey, "_")
	return rawNodeKey[:index]
}
