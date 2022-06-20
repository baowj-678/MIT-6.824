/**
 * @author: baowj
 * @time: 2022/5/20 20:38:10
 */
package mr

type Status string            //  Prepare, Work, Free, Fail
type TaskType string          // Reduce, Map
type TaskStatus string        // None, Prepare, Work, Fail, Success
type CoordinatorStatus string // Prepare, Work, Free, Map, Reduce

const (
	Prepare = "Prepare"
	Work    = "Work"
	Free    = "Free"
	Success = "Success"
	Fail    = "Fail"
	None    = "None"
	Reduce  = "Reduce"
	Map     = "Map"
)
