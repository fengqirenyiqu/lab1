package main

import (
	"fmt"
	"os"
	"plugin"
)

func main(){
	p,err:=plugin.Open(os.Args[1])//读取命令行输入获取到插件
	check(err,1)
	xmapf,err:=p.Lookup("Test")//获取到Test函数
	check(err,2)
	mapf:=xmapf.(func(string) string)
	testFunction_runtime(mapf)//这样mapf并不会执行，需要在函数内部调用这个func才行
	fmt.Println("test:ok:",mapf("a"))
}
func testFunction_runtime(getString func(string) string){
   b:=getString("s")
   fmt.Println(b)
}
func check(err error,id int){
	if err!=nil{
		fmt.Println(err,id)
	}
}

