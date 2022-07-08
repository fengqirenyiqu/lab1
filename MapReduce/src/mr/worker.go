package mr

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// KeyValue 获取到任务后即调用map处理任务，任务包含了(k,v)
//split v 拆分成word
//emit 输出一个key ，value；key就是word.value就是单词出现的次数,输出可以写到一个文件中
//reduce 获取所有map的输出即emit的输出统计key，value讲结果集输出到一个文件中。
//Done方法 当所有任务完成应该停止整个任务 ，可以用全局waitGroup实现,Add,执行完Done，coordinator主线程wait实现等待、
type KeyValue struct {
	Key string
	Value string
}

type WorkConnect struct {
	workername string
	conn net.Conn
	JobTime *time.Timer
}
type messageStruct struct {

}
var mx sync.Mutex
var ResultMap map[string][]string
var Mapresult chan []KeyValue//先用管道存储Map的结果，后续优化为存储到文件，存储到文件时候注意写锁
func Worker(mapf func(string, string) []KeyValue,reducef func(string, []string) string, workerId string,wg *sync.WaitGroup){
    wg.Add(1)
	fmt.Println(workerId)
    //跟coordinator通信获取任务，可以使用rpc来通信
    wc :=WorkConnect{workername:workerId,conn: connect(workerId)}
    wc.GetJobAndDoJob(mapf)
    ResultMap = make(map[string][]string)
loop:
	for  {
		select {
		case kvs :=<-Mapresult:
			for _,kv := range  kvs{
				mx.Lock()
				ResultMap[kv.Key]=append(ResultMap[kv.Key],kv.Value)//其实这里已经相当于开始reduce了
				mx.Unlock()
			}
		default:
			break loop
			}
	}
	for k,values := range ResultMap{
			fmt.Println(k,reducef(k,values))//多个g执行这个for只会输出一个，应该是插件的原因，
	}
	wg.Done()
}
func connect(workerName string)(net.Conn){
	conn,err := net.Dial("tcp","127.0.0.1:9990")
	if err != nil {
		fmt.Println("err:",err)
	}
	conn.Write([]byte(workerName))
	return conn
}
func (this *WorkConnect)GetJobAndDoJob(mapf func(string, string) []KeyValue){
	Mapresult = make(chan []KeyValue,100)
	for  {
		buf :=[512]byte{}
		n,err:=this.conn.Read(buf[0:])
		checkError(err)
		reply := KeyValue{}
		status :=string(buf[:n])
		if status=="ReadyForJob"{
			client,err := rpc.DialHTTP("tcp","127.0.0.1:9991")
			messageSt := new(messageStruct)
			err =client.Call("Coordinator.SendJob", messageSt,&reply)//限制获取条件，收到coordinator的什么信息才能接受job
			checkError(err)
			client.Close()
			if reply.Key=="null"{

			}else {
                this.StartJobMessage(reply.Key)
				this.JobTime = time.NewTimer(10*time.Second)
				k:=mapf(reply.Key,reply.Value)
                time.Sleep(2*time.Second)
				select {//超时判断，确保不会误传mapDone信息
				case <-this.JobTime.C:
					continue
				default:
					Mapresult<-k
					this.MapDone()
				}

			}

		}

		if status=="Reduce"{
			break
		}
		}
}
func (this *WorkConnect)MapDone(){
	this.conn.Write([]byte("MapDone"))
}
func (this *WorkConnect)StartJobMessage(filename string){
	this.conn.Write([]byte(filename))
}
