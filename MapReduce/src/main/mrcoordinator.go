package main

import (
	"MapReduce/src/mr"
	"time"
)
import "os"
import "fmt"


func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		os.Exit(1)
	}
    //创建coordinator
	m := mr.MakeCoordinator(os.Args[1:], 10)//通过命令行运行go文件来读取到要读哪个文件的内容，10是表示worker最长工作上次10s。
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
