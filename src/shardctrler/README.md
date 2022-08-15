# Lab 4A: Sharded Key/Value Service[The Shard controller]

### 资料

* [Lab4A内容](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

## 总体思路

**Lab4A**主要是基于**Raft**实现一个**分片KV服务**的**Controller**部分。**Controller**部分是基于**Raft**的一个集群，主要保存整个**分片KV服务**的所有**配置信息**（某个**raft集群**负责哪一部分**KV服务**【对**raft**集群】，某个**KV服务**需要哪个**raft**集群提供【对**客户端**】）。其主要代码基本类似于**Lab3**的代码，只是不需要进行**快照**。

本实验**\*不需要\***实现**Raft集群内部server的增减**，本实验主要实现**增减Raft集群（replica groups），以及将各个分片均匀分配给Raft集群**，主要包括下面四个操作：

* **Join**：添加一个**Group（Raft集群）**，重新分片；
* **Leave**：删除一个**Group（Raft集群）**，重新分片；
* **Move**：将某个**Shard**分配到另一个**Group**上；
* **Query**：查询，返回最新的配置；



## 结构体

## 函数

## 测试

## 注意事项【踩坑记录】
* golang的map中key是没有顺序的，遍历的时候没有顺序，且不能使用**==**比较两个map是否相等；

## 问题

## 优化思路

