# MIT-Lab

## 1.Lab1 MapReduce
在分支 zhangkai_lab1_mapreduce 中,你可以通过下面几步接入 MapReduce :
```
cd 6.824/src/main
sh test-mr.sh
```
该实验具体实现代码在 6.824/src/mr 下的 master.go、rpc.go、worker.go 中.

我们可以通过 6.824/src/main 中的 mrmaster.go、mrworker.go,通过下面方式进行代码测试:

mrmaster
```
go run mrmaster.go pg*.txt
```
mrworker
```
go build -buildmode=plugin ../mrapps/wc.go
go run mrworker.go wc.so
```
