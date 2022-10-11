# Lab 4B: Sharded Key/Value Service[Sharded Key/Value Server]

### 资料

* [Lab4B内容](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

## 总体思路

* 进行**Shard**迁移时，单个**Shard**进行迁移；

### Client

### Server

发现**re config**后，

## 结构体

## 函数

## 测试

## 注意事项【踩坑记录】

1. 同一个**Group**可能会负责多个**Shard**；

## 问题

#### 1. 脑裂问题

如果某个**Group**由于网络问题分成多个部分（即出现脑裂的情况），拥有多个**Leader**（只有其中一个Leader及其所在分区是**update**的）且这些**Server**和**Controller**的网络通信都正常，那么当进行**Re-Configuration**时，如果是点对点进行**状态迁移**，那么无法保证**发送方**是否是处于正确**网络分区的Server**，就会造成错误。

**解决方法**：

进行数据迁移时，假设数据从**发送方**发送至**接收方**，且**发送方**Group中**Server**个数为**N**，接收方**Server**个数为**M**。则要求：每个**接收方**需要接收**多于N/2**个数据并选择最新的数据【比较**index**和**term**】，否则一直等待，每个**发送方**需要发送给每个**接收Server**，并保证发送成功的**多于M/2**个，否则一直重复发送。



## 优化思路

