package main

import (
	"MapReduce/src/mr"
	"fmt"
	"sync"
	"time"
)
import "plugin"
import "os"

//import "fmt"
import "log"

func main() {
	//if len(os.Args) != 2 {
	//	fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
	//	os.Exit(1)
	//}
    var wg sync.WaitGroup
	mapf, reducef := loadPlugin(os.Args[1])
	mr.Mapresult=make(chan []mr.KeyValue,100)
	for _,name :=range os.Args[2:]{
		go mr.Worker(mapf, reducef,name,&wg)//这种方式就是手动启动多个worker，可以改成用go一次启动多个
	}
    time.Sleep(3*time.Second)
	//go mr.Worker(mapf, reducef, "worker2")
	//go mr.Worker(mapf, reducef, "worker3")

    wg.Wait()
}

//
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
//
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		println(err.Error())
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)
    fmt.Println("plugin finish")
	return mapf, reducef
}