# Raft

## 结构体/RPC
### AppendEntries
Leader发送的用于**日志同步**或者**心跳检测**的RPC。

### AppendEntriesReply
Peers对于AppendEntries RPC的回复。
* ConflictTerm、ConflictIndex: 根据原论文5.3结尾提出的优化策略添加的字段，该字段返回Follower和Leader日志不一致的Index和term，便于Leader重发AppendEntries。
## 函数

### Start

> `Start()` should return immediately, without waiting for the log appends to complete.

评测程序利用该函数进行**日志追加**请求。

如果该server是**Leader**则执行**本地追加**，并返回该条日志的**term、index**等信息；

如果该server是**Follower、Candidate**，则忽略该请求。



### sendRequestVote
Candidate向某个peer异步发送投票请求，如果请求成功：ch <- 1，否则：ch <- 0。

### election
进行一次选举的函数。



### RequestVote

响应RequestVote RPC的函数。

#### 原文关键点

<img src=".md/RequestVote.png" alt="img" style="zoom:50%;" />

* **up-to-date**

    > Raft determines which of two logs is more **up-to-date** by comparing the index and term of the last entries in the logs. If the logs have last entries with different terms, then the log with the later term is more up-to-date. If the logs end with the same term, then whichever log is longer is more up-to-date.



#### 函数流程





### AppendEntries
响应**AppendEntries RPC**的函数。

#### 原文关键点

<img src=".md/AppendEntries.png" alt="img.png" style="zoom:80%;" />

> While waiting for votes, a candidate may receive an AppendEntries RPC from another server claiming to be leader. If the leader’s term (included in its RPC) is at least as large as the candidate’s current term, then the candidate recognizes the leader as legitimate and returns to follower state. If the term in the RPC is smaller than the candidate’s current term, then the candidate rejects the RPC and continues in candidate state.


### HeartBeat
心跳函数，周期性(heartBeatInterval)调用sendAppendEntries发送AppendEntries RPC



### preVote【**deprecated**】
**预投票**，如果一个server无法与其他server通信，那么根据raft协议，该server的term会不断增加。之后，如果该server又重新接入网络，那么就会被选为leader，就会覆盖其他正常server的正常log。【注意：根据**Raft**协议，不会覆盖正常log，因为server正常接入后，发送**RPC请求**给其他server，其他server会**更新自身Term**，并转为**Follower**，但并**不立即给这个server投票**，而是将**votedFor置为-1**，只有在后面**确认server的日志up-to-date之后才会投票**】

Raft的作者在其博士论文《[CONSENSUS: BRIDGING THEORY AND PRACTICE](http://files.catwell.info/misc/mirror/2014-ongaro-raft-phd.pdf)》9.6节提到了解决方法，即增加preVote环节。

在Pre-Vote算法中，Candidate只有在确认大部分的节点愿意投票后，才会正式增加term并发起投票请求。






## 注意
1. Leader没有**election timeout**，所以**Leader不会主动退出**，当收到其他Leader发送的AppendEntries后自动退出。
2. 发送AppendEntries RPC一定要使用**子线程**（可以**用chan进行同步**），不然会阻塞在disconnect的server上。
3. up-to-date：最后一个**log的Term大的server更up-to-date**，否则如果term一样，则**log长度长的更up-to-date**。
4. 原文中提到**convert to follower**，只是将**currentState**转为**Follower**，只要把**votedFor**设为**-1**，**不需要为该server投票**。

### 5. Figure 8[不能提交小于currentTerm的log]
Raft论文在Figure 8(下图)介绍了原因：
![](./.md/Figure%208.png)
(a). S1是term(2)的Leader[S2,S3,S4,S5投票]，但在写完S1和S2的log后就挂了；

(b). S5是term(3)的Leader[S3,S4投票]，但在写完S5的log后就挂了；

(c). 【错误情况】S1是term(4)的Leader[S2,S3,S4投票]，在复制并commit term(2)的log【小于当前term】，以及写完S1的term(4)log后挂了；

(d). 【错误情况】此时S5是term(5)Leader[S2,S3,S4投票]，复制并commit term(3)的log【等于当前term】；
cd出现的问题是**index(2) commit了两次log，分别是term(2)和term(3)**，这违背了**Raft**基本假设。

(e). 【正常情况】c中的Leader commit term(4)【当前term】的日志。

6. Follower的AppendEntries RPC响应中：不要直接用切片的方式追加所以的日志，因为由于网络问题**网络包的达到顺序可能不同**（例如：append index=34的包可能比append index=33的包先到达），这样有可能not up-to-date的日志会覆盖掉up-to-date的日志；

7. Follower的AppendEntries RPC响应中：在追加日志时，有些Follower日志中最后的CommandTerm < Leader日志的最后CommandTerm，需要直接追加所有的entries；

8. Follower的AppendEntries RPC响应中：如果发现冲突entry，需要删除该entry以及之后的所以entry；

9. election中，Candidate收到RequestVote Reply后要**确认Term没有发生改变**（在发送RequestVote到收到Reply这段时间，Term可能发生改变）后才可以变为Leader；

10. mutex和channel死锁 TODO
## 问题
[常见问题](https://thesquareplanet.com/blog/students-guide-to-raft/)
1. 如果某个peer失联了，该peer term很大，恰好又增加了新log，那该peer重新加入后不就会被选为Leader，从而覆盖掉其他peer的正确log？

   个人理解：如果失联的peer是Follower，那么该Follower不会成为Leader，所以无法追加日志；如果该peer是Leader，失联后该Leader的term不会增加，相反其他Follower的term会增加从而选出新的Leader。
