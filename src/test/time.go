/**
 * @author: baowj
 * @time: 2022/5/24 21:22:36
 */
package main

import (
	"fmt"
	"time"
)

func main() {
	a := time.Now().Unix()
	time.Sleep(time.Second * 10)
	b := time.Now().Unix()
	fmt.Printf("a: %v\n", a)
	fmt.Printf("b: %v\n", b)
	fmt.Printf("b-a: %v\n", b-a)

}
