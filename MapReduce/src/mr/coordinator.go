package mr

import (
	"fmt"
	"io/ioutil"
	"net"
	"sync"
	"time"
)

// JobChan Coordinator MakeCoordinator Coordinator
//读取文件生成任务，
//读取rpc将任务分发给worker即传递(k,v)到map，k是文件名，v是文件的内容，
//并且要将工作时间超过10s的Job重新分给其他worker!!!!重点
//所有map执行完成后并且所有job都执行完，分配reduce给worker执行，
//worker跟Coordinator用tcp建立链接，使得Coordinator可以监测worker，

type Coordinator struct {
	fileNames []string//文件名
	Contents []string//文件的内容
	Complete bool
	MapDone bool
	JobCount int
	JobChan chan Job
	mx sync.Mutex

	
}

var (
	WorkerMap map[net.Conn]*worker //聊天点对点服务需要获取别人的conn,但是这里不需要，只需要通过轮询链接观察worker状态
	//收到worker发过来的消息就根据conn找到对应的worker设置好它的状态
    WorkerConns []net.Conn//遍历用

)


type worker struct {
	workerName string
	working_file int
	status string //轮询WorkerConnectMap的时候观察worker状态，status的取值有"READY","MAPPING","REDUCECING",
	//READY的时候就
	JobTime *time.Timer //时间限制
}
type Job struct {
	Key string//文件名
	Value string//文件的内容
}
func MakeCoordinator(filename []string,LimitTime int) (c *Coordinator){
    WorkerConns = []net.Conn{}
	WorkerMap = make(map[net.Conn]*worker)
	c =&Coordinator{
		Complete: false,
	    //MapDone: false,
	    JobChan: make(chan Job,10),
	    JobCount: 0,

	}
	for _,name := range filename{
		result,err := ioutil.ReadFile(name)
		if err != nil {
			fmt.Println("Read failed:",err)
		}
		content :=string(result)
		c.fileNames=append(c.fileNames,name)
		c.Contents=append(c.Contents,content)
		c.JobCount++
		fmt.Println("JobCount",c.JobCount)
	}
	c.CreatJob()//往任务队列中发送任务
    go c.ConnectWorker()
	return c
}
func (c *Coordinator)ConnectWorker(){
	go c.rpcRegister()
	listen, err := net.Listen("tcp", "127.0.0.1:9990") //与worker建立连接
	checkError(err)
	go c.MangeWorker()
	for {
		conn, err :=listen.Accept()
		checkError(err)
		buf :=[512]byte{}//不能为空的切片，空切片的话拿不到读过来的值
		n,err:=conn.Read(buf[0:])
		name :=string(buf[:n])
		fmt.Println(name)
		checkError(err)
		w:=worker{workerName:name,status: "READY",JobTime: time.NewTimer(600*time.Second)}
		//time.Timer需要初始化否则下面直接MangeWorkerStatus时读取C的channel会空指针异常
		c.mx.Lock()
		WorkerMap[conn]=&w//并发安全问题，下面的MangeWorker轮询的时候不能写，
		c.mx.Unlock()
		WorkerConns=append(WorkerConns,conn)
		go c.WatchingWorker(conn) //持续监听worker的消息
		go c.MangeWorkerStatus(conn)
	}
}
func (this *Coordinator)MangeWorker(){
	for this.JobCount!=0{
		for _,conn :=range WorkerConns{
			this.mx.Lock()
			w := WorkerMap[conn]
			if w.status =="READY"{
				conn.Write([]byte("ReadyForJob"))

				}
			w.status="MAPPING"
			this.mx.Unlock()
		}
		//TODO 要防止多发消息导致worker继续工作map 读取rpc消息的时候因为Jobchan已经空了导致阻塞
		//
	}
	fmt.Println("Job结束了")
	//发送消息告诉worker可以开始reduce了
	time.Sleep(1*time.Second)
	for _,conn := range WorkerConns{
		_,err:=conn.Write([]byte("Reduce"))
		checkError(err)
	}
	this.Complete=true

	fmt.Println("通知完成")
}
func (this *Coordinator)WatchingWorker(conn net.Conn){
	w:=WorkerMap[conn]
	for this.JobCount != 0 {
		buf :=[512]byte{}
		res,err:=conn.Read(buf[0:])
		checkError(err)
		message:= string(buf[:res])
		if message == "MapDone"{
			fmt.Println("Job--")
			this.mx.Lock()
			this.JobCount--
			w.status="READY"//因为指针类型浅拷贝，所以w的修改就是修改这个key对应的指针变量存储的地址
			w.JobTime=time.NewTimer(600*time.Second)//续期
			this.mx.Unlock()
		}else {
			//worker除了刚连接发消息后续只会发MapDone或者接受到任务开始信息读取了rpc后开始执行map前告诉coordinator我执行的文件名
			filename := message
			for idx,name :=range this.fileNames{
				if name == filename{
					w.working_file =idx
					w.JobTime = time.NewTimer(10*time.Second)
				}
			}
		}
	}
}
func (this *Coordinator)MangeWorkerStatus(conn net.Conn){
	w := WorkerMap[conn]
	for {
		select {
		case <-w.JobTime.C:
			this.mx.Lock()//加锁确保MangeWorker不会误判断
			fmt.Printf("%s : 超时了\n",w.workerName)
			this.JobChan <-Job{
				Key: this.fileNames[w.working_file],
				Value: this.Contents[w.working_file],
			}
			w.status = "Broken"
			this.mx.Unlock()
		default:
            continue
		}
	}
}
func (this *Coordinator)CreatJob(){
	for idx,_ := range this.fileNames{
		this.JobChan<-Job{
			Key: this.fileNames[idx],
			Value: this.Contents[idx],
		}
	}
}
func (this *Coordinator)Done() bool {
	 return  this.Complete
}


func checkError(err error){
	if err != nil {
		fmt.Println("err:",err)
	}
}