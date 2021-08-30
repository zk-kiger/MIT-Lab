package mr

import (
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	files    []string
	nReduces int

	// master 统一管理 map、reduce 任务信息
	mapTasks    []*MapReduceTask
	reduceTasks []*MapReduceTask

	mapFinished    bool
	reduceFinished bool

	// 防止多个 work 同时请求,导致 master 出现并发修改问题
	mutex sync.Mutex
}

type MapReduceTask struct {
	Type        string    // "Map", "Reduce", "Wait"
	Status      int       // 0-"Unassigned(未分配)", 1-"Assigned(已分配)", 2-"Finished(已完成)"
	Index       int       // 当前任务在 master 任务表中的位置(方便定位任务和更新任务)
	TimeStamp   time.Time // 任务开始时间,便于判断任务是否超时
	MapFile     string    // task for map
	ReduceFiles []string  // map 处理之后的中间文件
}

// Your code here -- RPC handlers for the worker to call.

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if m.reduceFinished {
		return true
	}
	return ret
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// 1.更新 master 处理文件及 reduce 个数
	m.files = append(m.files, files...)
	m.nReduces = nReduce

	// 2.初始化所有的 MapReduceTask
	for idx, file := range m.files {
		task := &MapReduceTask{
			Type:      "Map",
			Status:    0,
			Index:     idx,
			TimeStamp: time.Now(),
			MapFile:   file,
		}
		m.mapTasks = append(m.mapTasks, task)
	}

	m.server()
	return &m
}

// WorkerCallHandler
// 处理 worker 的 RPC 请求,统一处理 map/reduce 任务的请求和完成逻辑
// master 需要互斥处理多个 worker 的 RPC 请求
func (m *Master) WorkerCallHandler(args *MapReduceArgs, reply *MapReduceReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var task *MapReduceTask
	// 设置 task 为 Wait,防止在下面处理中无法获取到可以处理的 task
	task = &MapReduceTask{Type: "Wait"}

	reqType := args.MessageType
	switch reqType {
	// 1.请求任务
	case RequestTask:
		// 1.1 请求 map 任务,在 mapTasks 中选择一个 Unassigned 或者超时的任务
		if !m.mapFinished {
			t := m.getUnassignedMapTask()
			if t != nil {
				t.Status = 1
				task = t
			}
		} else {
			// 1.2 map 任务完成,在 reduceTasks 中选择一个 Unassigned 或者超时的任务
			if !m.reduceFinished {
				t := m.getUnassignedReduceTask()
				if t != nil {
					t.Status = 1
					task = t
				}
			}
		}
	// 2.完成任务
	case FinishTask:
		finishTask := args.Task

		// 2.1 完成 map 任务,更新 mapTasks 任务中对应的任务状态,判断 mapTasks 所有任务都完成则进入 reduce 阶段
		if !m.mapFinished {
			idx := finishTask.Index
			originalTask := m.mapTasks[idx]

			// 2.1.1 对比时间戳判断该任务是否处理超时
			if finishTask.TimeStamp.Sub(originalTask.TimeStamp) > 1*time.Second {
				// 重置任务状态 -> 0("Unassigned")、时间戳
				originalTask.Status = 0
				originalTask.TimeStamp = time.Now()
			}

			// 2.1.2 该 map 任务完成,修改 mapTasks 对应任务状态
			if originalTask.Status == 1 {
				originalTask.Status = 2
				originalTask.ReduceFiles = append(originalTask.ReduceFiles, finishTask.ReduceFiles...)

				// 判断 mapTasks 所有任务是否全部完成
				finished := m.isMapTasksFinished()
				if finished {
					// 进入 reduce 阶段,需要将同一个 reducer_id 的任务放入
					m.mapFinished = true
					m.reduceTasks = make([]*MapReduceTask, m.nReduces)
					for i := 0; i < m.nReduces; i++ {
						m.reduceTasks[i] = &MapReduceTask{
							Type:      "Reduce",
							Status:    0,
							Index:     i,
							TimeStamp: time.Now(),
						}
					}
					for _, mapTask := range m.mapTasks {
						// 遍历每个 mapTasks 产生的临时文件,并放入相应的 reduceTask
						for _, reduceFile := range mapTask.ReduceFiles {
							// "mr-<mapTask_id>-<reduceTask_id>"
							strS := strings.Split(reduceFile, "-")
							reduceId, err := strconv.Atoi(strS[2])
							if err != nil {
								panic(err)
							}
							m.reduceTasks[reduceId].ReduceFiles = append(m.reduceTasks[reduceId].ReduceFiles, reduceFile)
						}
					}
				}
			}
		} else {
			// 2.2 完成 reduce 任务,更新 reduceTasks 任务中对应的任务状态
			if !m.reduceFinished {
				idx := finishTask.Index
				originalTask := m.reduceTasks[idx]

				// 2.1.1 对比时间戳判断该任务是否处理超时
				if finishTask.TimeStamp.Sub(originalTask.TimeStamp) > 1*time.Second {
					// 重置任务状态 -> 0("Unassigned")、时间戳
					originalTask.Status = 0
					originalTask.TimeStamp = time.Now()
				}

				// 2.1.2 该 reduce 任务完成,修改 mapreduceTasks 对应任务状态
				if originalTask.Status == 1 {
					originalTask.Status = 2

					// 判断 mapTasks 所有任务是否全部完成
					finished := m.isReduceTasksFinished()
					if finished {
						// map/reduce 任务处理完成
						m.reduceFinished = true
					}
				}
			}
		}
	}

	reply.Task = task
	reply.NReduces = m.nReduces
	return nil
}

// getUnassignedMapTask 获取一个 Unassigned 任务(包括处理超时任务)
func (m *Master) getUnassignedMapTask() (task *MapReduceTask) {
	if len(m.mapTasks) <= 0 {
		return nil
	}
	for _, t := range m.mapTasks {
		if t.Status == 0 {
			task = t
			return
		}
	}
	return
}

// getUnassignedReduceTask 获取一个 Unassigned 或者超时的 reduce 任务
func (m *Master) getUnassignedReduceTask() (task *MapReduceTask) {
	if len(m.reduceTasks) <= 0 {
		return nil
	}
	for _, t := range m.reduceTasks {
		if t.Status == 0 {
			task = t
			return
		}
	}
	return
}

// isMapTasksFinished 判断 mapTasks 中的所有 map 任务是否完成
func (m *Master) isMapTasksFinished() (f bool) {
	f = true
	for _, task := range m.mapTasks {
		if task.Status != 2 {
			f = false
			return
		}
	}
	return
}

// isReduceTasksFinished 判断 reduceTasks 中的所有 reduce 任务是否完成
func (m *Master) isReduceTasksFinished() (f bool) {
	f = true
	for _, task := range m.reduceTasks {
		if task.Status != 2 {
			f = false
			return
		}
	}
	return
}

func (t MapReduceTask) isTimeout() (isTimeout bool) {
	stamp := t.TimeStamp
	// 处理时间超过 1s 就超时
	if time.Since(stamp) > 1*time.Second {
		isTimeout = true
	}
	return
}
