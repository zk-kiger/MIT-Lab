package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// ByKey for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// KeyValue
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for true {
		args := &MapReduceArgs{MessageType: RequestTask}
		reply := &MapReduceReply{}

		res := call("Master.WorkerCallHandler", args, reply)
		if !res {
			break
		}

		switch reply.Task.Type {
		case "Map":
			doMap(reply, mapf)
		case "Reduce":
			doReduce(reply, reducef)
		case "Wait":
			time.Sleep(1 * time.Second)
		}
	}
}

// doMap 调用用户自定义的 mapf 函数,处理从 master 获取到的 map 任务,完成任务之后将结果存入中间文件 reduceFiles,
// 便于后续对中间文件 reduce
func doMap(reply *MapReduceReply, mapf func(string, string) []KeyValue) {
	task := reply.Task
	nReduces := reply.NReduces

	// 1.根据 master 分配的 map 任务,读取对应的文件 k/v(filename/content) 传入 mapf
	mapFile := task.MapFile
	content, err := ioutil.ReadFile(mapFile)
	if err != nil {
		panic(err)
	}
	keyValues := mapf(mapFile, string(content))

	// 2.对产生的 keyValues 进行 shuffle,rehash 到不同的 intermediate file
	intermediate := make([][]KeyValue, nReduces)
	for _, keyValue := range keyValues {
		reduceIndex := ihash(keyValue.Key) % nReduces
		// 将 keyValue 放到对应 reducer 待处理的 keyValues,后面会序列化到临时文件
		intermediate[reduceIndex] = append(intermediate[reduceIndex], keyValue)
	}

	// 3.将 shuffle 之后的 keyValue,序列化到本地磁盘临时文件中,等待 reducer 处理
	for idx, kvs := range intermediate {
		// 3.1 生成临时文件名
		fileName := intermediateFileName(task, idx)
		// 3.2 将 keyValue 写入文件
		file, err := os.Create(fileName)
		if err != nil {
			panic(err)
		}
		kvJson, err := json.Marshal(kvs)
		if err != nil {
			panic(err)
		}
		if _, err = file.Write(kvJson); err != nil {
			panic(err)
		}
		if err = file.Close(); err != nil {
			panic(err)
		}
		task.ReduceFiles = append(task.ReduceFiles, fileName)
	}

	// 4.map 任务处理完成,向 master 提交任务
	args := MapReduceArgs{
		MessageType: FinishTask,
		Task:        task,
	}
	res := call("Master.WorkerCallHandler", args, reply)
	if !res {
		panic(err)
	}
}

// doReduce 调用用户自定义的 reducef 函数,处理从 master 获取到的 reduce 任务,完成任务之后将结果存入文件
func doReduce(reply *MapReduceReply, reducef func(string, []string) string) {
	task := reply.Task

	var allKeyValuePairs []KeyValue
	// 1.读取所有临时文件中的 k/v
	for _, reduceFile := range task.ReduceFiles {
		content, err := ioutil.ReadFile(reduceFile)
		if err != nil {
			panic(err)
		}
		var keyValuePairs []KeyValue
		if err = json.Unmarshal(content, &keyValuePairs); err != nil {
			panic(err)
		}
		allKeyValuePairs = append(allKeyValuePairs, keyValuePairs...)
	}

	// 2.对所有的 k/v,根据 k 进行排序
	sort.Sort(ByKey(allKeyValuePairs))

	// 3.将所有 k/v 以 map[string]string 的形式存储,方便 reducef 函数处理
	keyValueMap := map[string][]string{}
	for _, pair := range allKeyValuePairs {
		k, v := pair.Key, pair.Value
		keyValueMap[k] = append(keyValueMap[k], v)
	}

	// 4.创建 reducer 处理结果输出文件
	outputFile, err := os.Create(outputFileName(task))
	if err != nil {
		panic(err)
	}

	// 5.reducef 函数处理,最终将处理结果输出到结果文件
	for k, v := range keyValueMap {
		if _, err = fmt.Fprintf(outputFile, "%v %v\n", k, reducef(k, v)); err != nil {
			panic(err)
		}
	}
	if err = outputFile.Close(); err != nil {
		panic(err)
	}

	// 6.reduce 任务处理完成,向 master 提交任务
	args := MapReduceArgs{
		MessageType: FinishTask,
		Task:        task,
	}
	res := call("Master.WorkerCallHandler", args, reply)
	if !res {
		panic(err)
	}
}

// outputFileName 根据 reduceId 生成输出结果文件名: mr-out-<reduceTask_id>"
func outputFileName(reduceTask *MapReduceTask) (fileName string) {
	reduceTaskIds := strconv.Itoa(reduceTask.Index)
	fileName = "mr-out-" + reduceTaskIds
	return
}

// intermediateFileName 根据 workId 生成临时文件名: mr-<mapTask_id>-<reduceTask_id>
func intermediateFileName(mapTask *MapReduceTask, reduceTaskId int) (fileName string) {
	mapTaskIds := strconv.Itoa(mapTask.Index)
	reduceTaskIds := strconv.Itoa(reduceTaskId)
	fileName = "mr-" + mapTaskIds + "-" + reduceTaskIds
	return
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
