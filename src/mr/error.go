/**
 * @author: baowj
 * @time: 2022/5/21 19:47:46
 */
package mr

type MRError struct {
	message string
}

func (err MRError) Error() string {
	return err.message
}
