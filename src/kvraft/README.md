# Lab3：Fault-tolerant Key/Value Service

### 资料

* [Lab3内容](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)

## 总体思路

### Client

1. **Client**随机生成自己的**64位ID**（碰撞的概率极低）；
2. **Client**对于每个发送到**Server**的请求都从1开始编号，并递增；
3. 收到**ErrWrongLeader**、**ErrTimeOut**：更换发送的**Server**；
4. 收到**ErrReDo**：对于*PutAppend*请求直接返回；对于*Get*请求，更新**请求编号**后重新发送；
5. 对于每个**Server**发生**ErrWrongLeader**、**ErrTimeOut**后，100ms内不会再成为**Leader**；



### Server

1. 收到**RPC**请求后，先调用**Raft**的**Start**提交日志，等**日志一致**后进行**具体操作**，完成后返回**RPC**结果；
2. 每个**RPC**请求**Start**提交后如果是**Leader**，都会生成一个**Channel**，**RPC请求**阻塞在该**Channel**等待结果，如果等到超时（5s）会放弃等待并返回**ErrTimeout**结果；
3. **applier**线程，等待**Raft**的**apply channel**，根据内容进行**snapshot**或者**具体操作**，操作完成后返回结果到**2**中的**Channel**；
4. **Server**会为每个**Client**维护一个**MaxCommandID**，如果进行**具体操作**时，该**command**的**Id**小于等于该**MaxCommandID**则返回**ErrReDo**；



## 结构体

### KVServer

* **snapshotInterval**：**Snapshot**线程的睡眠时间；
* **kvDatabase**：**KV**数据库；
* **waitChannel**：Server的**RPC**响应函数阻塞等待结果的**Channel**；
* **clientMaxCommandId**：每个**Client**目前提交的最大**command id**；
* **lastApplied**：上一次**Apply**的**log index**（不同于**raft**的**lastApplied**，用于生成**Snapshot**）；



### Clerk

* **leader**：保存最新的**Leader Id**；
* **clientId**：初始化随机生成的**Client Id**【参见**总体思路->1**】；
* **commandId**：目前提交的最新**Command Id**【递增】；
* **wrongLeaderTimer**：对于每个**Server**最近的发送请求后返回**ErrTimeout**或者**ErrWrongLeader**的**Timer**，短时多次重复发送；



## 函数

* **Get**：**Get RPC**的响应函数；
* **PutAppend**：**PutAppend RPC**响应函数；
* **doOp**：对**KV数据库**进行操作的函数；
* **applier**：接收**Raft**的**apply channel**并进行处理的线程；
* **Snapshot**：进行快照的线程（当**Raft**的日志达到**maxraftstate**的80%时），生成快照并发送给**Raft**持久化；
* **ReadSnapshotFromRaft**：从**Raft**获取快照并安装；

## 测试

## 注意事项【踩坑记录】

1. **close()**可以结束**for i := range ch**的循环等待；
2. **context.WithTimeout**和**select**一起使用可以设置超时退出（不用一直阻塞等待chan返回）；
3. 如果**Leader**在发送**Log Entry**给**Raft**后和返回**reply**之前变成**Follower**了，那么该**Entry**可能会提交也可能不会提交。所以**Client**需要重新发送请求。但是，如果我们直接等待**applyCh**返回，那么可能得到的是其他**Command**的返回结果而误以为是该**Client**的结果。但是如果我们对于每个**Command**都放置一个回复的**Channel**，在**Raft提交后，State Machine执行Command**时返回该**Channel**就不会出现上述问题。
4. **TestSpeed3A**测试会连续发送**1000**个**Append**请求，并要求每个请求处理处理时间不多于**33.3ms**，但是**HeartBeatInterval>100ms**，所以要通过该测试，必须在**Start**中发送**AppendEntry**，并且及时commit；
5. **状态机**默认**crash**后数据全部丢失；
6. **server**上记录**每个Client提交请求的最新ID**的数据（**clientMaxIdMap**）需要保存到**Snapshot**中，因为当**server**崩溃后，重新安装**Snapshot**后，之前的**clientMaxIdMap**就会丢失，可能会造成**重复提交**的问题；



## 问题

1. 多线程的话，`applych`返回值会给不同的线程

2. `PutAppend`和`Get`函数中，在`Start`提交日志到`applyCh`收到提交信息之间，如果**Leader**变成**Follower**，如果线程一直**阻塞等待applyCh**，那么不就会**一直无法进行其他操作**？

    比较Term。

3. 为每个**Client**的每个**Command**分配一个唯一的**Index**来避免**重复提交**，但是如果前一个**Command**阻塞了，在后面相同的**Command**提交给**Raft**后还未**提交**，那么不就会产生**重复提交**。

4. **Raft**的**applyCh**是周期性调用，还是commit后调用？



## 优化思路

1. 将**Get**请求分流至**Follower**。

