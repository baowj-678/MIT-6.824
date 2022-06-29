# Raft
## 函数
### election
进行一次选举的函数。

### RequestVote
响应RequestVote RPC的函数。
* if (currentTerm < term)
  * be Follower(reset currentTerm, reset currentState, reset votedFor)
  * reset electionTimer
  * vote granted
* else if (currentTerm = term)
  * if (currentState = Follower)
    * if (not votedFor anyone or votedFor this one)
      * be/stay Follower(reset currentState, reset votedFor)
      * reset electionTimer
      * vote granted
    * else
      * vote not granted
  * else 
    * vote not granted
* else
  * set reply.Term
  * vote not granted

### AppendEntries
* if (currentTerm < term)
  * be Follower(reset currentTerm, reset currentState, reset votedFor)
  * reset electionTimer
* else if (currentTerm = term)
  * if (currentState = Follower and votedFor this peer)
    * reset electionTimer
  * else if (currentState = Candidate)
    * be Follower(reset currentState, reset votedFor)
    * reset electionTimer
* else
  * set reply.Term

### HeartBeat
心跳函数，周期性(heartBeatInterval)调用sendAppendEntries发送AppendEntries RPC


## 注意
1. Leader没有election timeout，所以Leader不会主动退出，当收到其他Leader发送的AppendEntries后自动退出。
2. 发送AppendEntries RPC一定要使用子线程（可以用chan进行同步），不然会阻塞在disconnect的peer上。