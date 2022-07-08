package mr

import (
	"fmt"
	"net/http"
	"net/rpc"
)

//worker与coordinator建立连接后，worker调用注册的rpc，注册改到Watching Worker

func (this *Coordinator)rpcRegister(){
	rpc.RegisterName("Coordinator",this)
	rpc.HandleHTTP()
	fmt.Println("rpcServer register")
	err := http.ListenAndServe("127.0.0.1:9991",nil)
	if err != nil {
		fmt.Println("err:", err)
	}

}
func (t *Coordinator)SendJob(messageStruct struct{},reply *KeyValue) error{
	//空结构体暂时不管，只需要Worker拿到job的k，v就行。后续可以优化
	//其中空结构体传的信息时Worker的状态，内存等信息，根据这些信息，后续可以优化raft选举，即超时的job重新给谁更好。
	select {
	case job :=<-t.JobChan :
		reply.Key=job.Key
		reply.Value=job.Value
	default:
		reply.Key="null"
		reply.Value="null"
		}
	return nil

}
