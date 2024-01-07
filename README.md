# Consistent_Hash
采用redis ZSET作为哈希环实现的分布式哈希一致性算法
核心数据结构
![image](https://github.com/pule1234/Consistent_Hash/assets/112395669/3f9bab82-35d2-4bb6-92b6-e33f0f373fed)
hashRing接口中为哈希环需要实现的方法

哈希环上存储的为虚拟节点，每个虚拟节点都有自己对应的真实节点，当然有可能多个虚拟节点会因为hash冲突存储在同一个位置，那么这多个节点会组成一个链表，当数据存储在该节点时，指挥存储在该链表中的第一个节点

首先添加一个节点时，需要同时传入其权重，按照权重分配该节点对应虚拟节点的数量
![image](https://github.com/pule1234/Consistent_Hash/assets/112395669/b4120f26-2521-4b63-9cab-835fa010895c)
之后会在为每个虚拟节点计算hash值，得出其存储在哈希环上的位置，接着通过ZRANGE查找出该位置所有的虚拟节点，若该节点存在，则直接返回，否则将该虚拟节点添加到该位置对应的节点列表中
![image](https://github.com/pule1234/Consistent_Hash/assets/112395669/35dccd61-dc3c-4501-831c-ac291306d45b)
接着因添加节点引发的数据迁移需要进行处理，执行migrateIn函数获取需要执行的数据迁移任务信息
![image](https://github.com/pule1234/Consistent_Hash/assets/112395669/0aced921-cc52-427f-ac46-57160c680c36)

项目test在example_test.go中



 


