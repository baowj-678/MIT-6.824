/**
 * @author: baowj
 * @time: 2022/5/21 18:54:56
 */
package main

func main() {
	m := map[int]*[]int{}
	m[0] = &[]int{1, 2}
	v, _ := m[0]
	*v = append(*v, 9)
	println(*m[0])
}
