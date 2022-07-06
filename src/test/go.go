/**
 * @author: baowj
 * @time: 2022/6/28 22:36:42
 */
package main

import (
	"fmt"
	"sort"
)

func main() {
	a := []int{9, 6, 7}
	append(a, 9)
	sort.Ints(a)
	fmt.Println(a)
}
