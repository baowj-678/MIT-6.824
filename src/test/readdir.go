/**
 * @author: baowj
 * @time: 2022/5/21 17:02:16
 */
package main

import (
	"os"
)

func main() {
	dir, _ := os.ReadDir("./")
	for _, file := range dir {
		println(file.Name())
	}
}
