/**
 * @author: baowj
 * @time: 2022/6/20 17:06:25
 */
package raft

type State string

const (
	Leader    = "Leader"
	Follower  = "Follower"
	Candidate = "Candidate"
)
