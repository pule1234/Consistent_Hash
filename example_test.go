package consistent_hash

import (
	"context"
	"github.com/pule1234/consistent_hash/redis"
	"testing"
)

const (
	network  = "tcp"
	address  = "localhost:6379"
	password = ""

	hashRingKey = "哈希环唯一 id"
)

func Test_redis_consistent_hash(t *testing.T) {
	redisClient := redis.NewClient(network, address, password)
	hashRing := redis.NewRedisHashRing(hashRingKey, redisClient)
	consistentHash := NewConsistentHash(hashRing, NewMurmurHasher(), nil)
	test(t, consistentHash)
}

func test(t *testing.T, consistentHash *ConsistentHash) {
	ctx := context.Background()
	nodeA := "node_a"
	weightNodeA := 2
	nodeB := "node_b"
	weightNodeB := 1
	nodeC := "node_c"
	weightNodeC := 1
	if err := consistentHash.AddNode(ctx, nodeA, weightNodeA); err != nil {
		t.Error(err)
		return
	}

	if err := consistentHash.AddNode(ctx, nodeB, weightNodeB); err != nil {
		t.Error(err)
		return
	}

	dataKeyA := "data_a"
	dataKeyB := "data_b"
	dataKeyC := "data_c"
	dataKeyD := "data_d"
	node, err := consistentHash.GetNode(ctx, dataKeyA)
	if err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyA, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyB); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyB, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyC); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyC, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyD); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyD, node)
	if err := consistentHash.AddNode(ctx, nodeC, weightNodeC); err != nil {
		t.Error(err)
		return
	}
	if node, err = consistentHash.GetNode(ctx, dataKeyA); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyA, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyB); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyB, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyC); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyC, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyD); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyD, node)
	if err = consistentHash.RemoveNode(ctx, nodeC); err != nil {
		t.Error(err)
		return
	}
	if node, err = consistentHash.GetNode(ctx, dataKeyA); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyA, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyB); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyB, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyC); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyC, node)
	if node, err = consistentHash.GetNode(ctx, dataKeyD); err != nil {
		t.Error(err)
		return
	}
	t.Logf("data: %s belongs to node: %s", dataKeyD, node)
	t.Error("ok")
}
