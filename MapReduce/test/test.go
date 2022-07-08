package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

func main(){
	if len(os.Args)>=2{
		filename := os.Args[1]
	//	for _,value := range filename{
	//		fmt.Println(value)
	//	}
	//}
		//file,err := os.Open(filename)
		//checkErr(err)
		//reader :=bufio.NewReader(file)
		//fmt.Println(reader.ReadString('\n')) //只能读到一行就结束，不行,常用于获取键盘输入一行行的读
		result,err := ioutil.ReadFile(filename)
		checkErr(err)
		resultString :=string(result)
		for _,linstr:=range strings.Split(resultString,"\n"){
			linstr =strings.TrimSpace(linstr)
			fmt.Printf(linstr)
		}
		}
}

func checkErr(err error){
	if err != nil{
		fmt.Println("err")
	}
}