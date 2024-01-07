package consistent_hash

import "C"
import (
	"context"
	"errors"
	"math"
)

// 用户需要注册好闭包函数进来，核心是执行数据迁移操作
type Migrator func(ctx context.Context, dataKeys map[string]struct{}, from, to string) error

// 在AddNode 添加流程节点中，获取需要执行的数据迁移的任务明细
func (c *ConsistentHash) migrateIn(ctx context.Context, virtualScore int32, nodeID string) (from, to string, datas map[string]struct{}, _err error) {
	// 若使用方没有注入迁移函数 ， 则直接返回
	if c.migrator == nil {
		return
	}

	// 根据虚拟节点数值virtualScore 直接查询哈希环，查看其映射的真实节点列表
	nodes, err := c.hashRing.Node(ctx, virtualScore)
	if err != nil {
		_err = err
		return
	}

	// 倘若virtualScore对应的节点数量大于1 ，则说明当前节点不是列表中index =0的收个节点，则说明不需要执行数据迁移操作，因为状态数据只会被分配到列表的首个节点中
	if len(nodes) > 1 {
		return
	}

	// 执行floor操作，获取当前虚拟节点数值virtualScore逆时针往上的第一个虚拟节点数值lastScore
	lastScore, err := c.hashRing.Floor(ctx, c.decrScore(virtualScore))
	if err != nil {
		_err = err
		return
	}

	// 倘若哈希环上并不存在其他的虚拟节点，则不无需执行数据迁移操作
	if lastScore == -1 || lastScore == virtualScore {
		return
	}

	// 执行ceiling操作，获取当前虚拟节点数值virtualScore顺时针往下的第一个虚拟节点数值nextScore
	nextScore, err := c.hashRing.Ceiling(ctx, c.incrScore(virtualScore))
	if err != nil {
		_err = err
		return

	}

	// 7 倘若哈希环上不存在其他虚拟节点，则无需执行数据迁移操作
	if nextScore == -1 || nextScore == virtualScore {
		return
	}

	// 以下为两种特殊情况
	//  patternOne: last-0-cur-next
	patternOne := lastScore > virtualScore
	//  patternTwo: last-cur-0-next
	patternTwo := nextScore < virtualScore
	if patternOne {
		lastScore -= math.MaxInt32
	}

	if patternTwo {
		virtualScore -= math.MaxInt32
		lastScore -= math.MaxInt32
	}

	// 获取到nextScore对应的真实节点列表
	nextNodes, err := c.hashRing.Node(ctx, nextScore)
	if err != nil {
		_err = err
		return
	}

	//
	if len(nextNodes) == 0 {
		return
	}

	//获取到nextScore首个真实节点对应的状态数据的key列表
	dataKeys, err := c.hashRing.DataKeys(ctx, c.getNodeID(nextNodes[0]))

	datas = make(map[string]struct{})
	// 遍历状态数据key列表，将其中满足迁移条件的部分添加到datas中
	for dataKey := range dataKeys {
		// 依次将每个状态数据的 key 映射到哈希环上的某个位置
		dataVirtualScore := c.encryptor.Encrypt(dataKey)
		//  对应于 patternOne，需要将 (last,max] 范围内的数据统一减去哈希环的长度
		if patternOne && dataVirtualScore > (lastScore+math.MaxInt32) {
			dataVirtualScore -= math.MaxInt32
		}

		// 对应于 patternTwo，将数据统一减去哈希环的长度
		if patternTwo {
			dataVirtualScore -= math.MaxInt32
		}

		//  倘若数据不属于 (lastScore,virtuaslScore] 的范围，则无需迁移
		if dataVirtualScore <= lastScore || dataVirtualScore > virtualScore {
			continue
		}

		datas[dataKey] = struct{}{}
	}

	// 从nextScore对应的首个真实节点中删除这部分需要迁移的数据key
	if err = c.hashRing.DeleteNodeToDataKeys(ctx, c.getNodeID(nextNodes[0]), datas); err != nil {
		return "", "", nil, err
	}

	// 将这部分数据添加到nodeID中
	if err = c.hashRing.AddNodeToDataKeys(ctx, nodeID, datas); err != nil {
		return "", "", nil, err
	}

	// 返回结果
	return c.getNodeID(nextNodes[0]), nodeID, datas, nil
}

// 获取在删除节点流程中，需要执行数据迁移任务的明细
func (c *ConsistentHash) migrateOut(ctx context.Context, virtualScore int32, nodeID string) (from, to string, datas map[string]struct{}, err error) {
	// 没有注入迁函数
	if c.migrator == nil {
		return
	}

	defer func() {
		if err != nil {
			return
		}
		if to == "" || len(datas) == 0 {
			return
		}

		if err = c.hashRing.DeleteNodeToDataKeys(ctx, nodeID, datas); err != nil {
			return
		}
		err = c.hashRing.AddNodeToDataKeys(ctx, to, datas)
	}()

	from = nodeID
	nodes, _err := c.hashRing.Node(ctx, virtualScore)
	if err != nil {
		err = _err
		return
	}

	// 如果真实节点不存在，直接返回
	if len(nodes) == 0 {
		return
	}

	//如果待删除节点不是列表中的首个节点，直接返回， 因为非首个节点不会存放对应于该virtualScore的状态数据
	if c.getNodeID(nodes[0]) != nodeID {
		return
	}

	// 查看待删除节点中存放的状态数据key列表
	var allDatas map[string]struct{}
	if allDatas, err = c.hashRing.DataKeys(ctx, nodeID); err != nil {
		return
	}

	if len(allDatas) == 0 {
		return
	}

	// 查询哈希环中虚拟节点数值virtualScore逆时针往前的第一个虚拟节点数值lastScore
	lastScore, _err := c.hashRing.Floor(ctx, c.decrScore(virtualScore))
	if _err != nil {
		err = _err
		return
	}

	// 倘若哈希环中不存在其他的虚拟节点， 则需要校验当前真实节点是否是环中的唯一真实节点，否则会需要将全量数据委托给列表的下一个真实节点
	var onlyScore bool
	if lastScore == -1 || lastScore == virtualScore {
		if len(nodes) == 1 {
			err = errors.New("no other no")
			return
		}
		onlyScore = true
	}

	// 判断是否是lastScore-00virtualScore-nextScore的组成形式
	patten := lastScore > virtualScore
	if patten {
		lastScore -= math.MaxInt32
	}

	datas = make(map[string]struct{})
	// 遍历待删除节点的状态数据key，将需要进行迁移的数据添加到datas中
	for data := range allDatas {
		// 如果整个哈希环只有一个virtualScore,且对应的真实节点数量为多个，则直接将全量数据存储到下一个真实节点
		if onlyScore {
			datas[data] = struct{}{}
			continue
		}

		// 将位置位于 (lastScore, virtualScore] 的数据添加到 datas，需要进行迁移
		dataScore := c.encryptor.Encrypt(data)
		if patten && dataScore > lastScore+math.MaxInt32 {
			dataScore -= math.MaxInt32
		}
		if dataScore <= lastScore || dataScore > virtualScore {
			continue
		}

		datas[data] = struct{}{}
	}

	// 如果当前virtualScore下存在多个节点，则直接委托给下一个节点
	if len(nodes) > 1 {
		to = c.getNodeID(nodes[1])
		return
	}

	// 寻找后继节点
	if to, err = c.getvaildNextNode(ctx, virtualScore, nodeID, nil); err != nil {
		err = _err
		return
	}

	if to == "" {
		err = errors.New("no other node")
	}
	return
}

// 寻找后继节点， 一方面需要考虑位置关系，另一方面要考虑后继节点不能和待删除节点是同一个真实节点
func (c *ConsistentHash) getvaildNextNode(ctx context.Context, score int32, nodeID string, ranged map[int32]struct{}) (string, error) {
	nextScore, err := c.hashRing.Ceiling(ctx, c.incrScore(score))
	if err != nil {
		return "", err
	}

	if nextScore == -1 {
		return "", nil
	}

	// 倘若已经检索了一整轮还没有找到目标
	if _, ok := ranged[nextScore]; ok {
		return "", nil
	}

	// 后继节点的key必须不与自己相同，否则继续往下寻找
	nextNodes, err := c.hashRing.Node(ctx, nextScore)
	if err != nil {
		return "", err
	}
	//  倘若不存在真实节点，抛错返回(这是不符合预期的. 一个 virtualScore 在真实节点列表为空时，会直接从环中移除)
	if len(nextNodes) == 0 {
		return "", errors.New("next node empty")
	}

	//nextScore对应的首个真实节点是否非当前待删除节点，如果满足条件直接返回，作为托付的后继节点
	if nextNode := c.getNodeID(nextNodes[0]); nextNode != nodeID {
		return nextNode, nil
	}

	//倘若nextScore对应的真实节点列表长度大于1， 且首个真实节点对应为待删除节点，则取第二个节点作为后继节点
	if len(nextNodes) > 1 {
		return c.getNodeID(nextNodes[1]), nil
	}

	if ranged == nil {
		ranged = make(map[int32]struct{})
	}
	ranged[score] = struct{}{}

	// 倘若当前找到的nextScore对应的真实节点为待删除节点本身，则递归向下检索
	return c.getvaildNextNode(ctx, nextScore, nodeID, ranged)
}

func (c *ConsistentHash) incrScore(score int32) int32 {
	if score == math.MaxInt32-1 {
		return 0
	}
	return score + 1
}

func (c *ConsistentHash) decrScore(score int32) int32 {
	if score == 0 {
		return math.MaxInt32 - 1
	}
	return score - 1
}
