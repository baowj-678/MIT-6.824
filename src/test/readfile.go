/**
 * @author: baowj
 * @time: 2022/5/21 18:26:02
 */
package main

import (
	"io/ioutil"
	"os"
	"strings"
)

func main() {
	file, _ := os.Open("test.txt")
	content, _ := ioutil.ReadAll(file)
	file.Close()
	split := strings.Split(string(content), "\n")
	for _, str := range split {
		words := strings.Split(str, " ")
		println("(" + words[0] + "," + words[1] + ")")
	}

}
