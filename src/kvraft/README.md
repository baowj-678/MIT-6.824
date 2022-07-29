# Lab3：Fault-tolerant Key/Value Service

### 资料

* [Lab3内容](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)

## 总体思路



## 结构体

## 函数

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

