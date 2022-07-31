# lab1

启动
cd到src下的main目录 
1go run mrcoordinator.go pg-*.txt
2go build -buildmod=plugin ../mrapps/wc.go
3go run mrworker.go wc.so 1 2 3
