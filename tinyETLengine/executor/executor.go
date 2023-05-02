package executor

import (
	"bytes"
	"encoding/json"
	concurrentMap "github.com/orcaman/concurrent-map"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	componentsFactory "tinyETL/tinyETLengine/Components"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/utils"
)

type Executor struct {
	id                    string
	Components            map[string]abstractComponents.VirtualComponents
	adjacentList          map[string][]string
	inDegreeList          map[string][]string
	inDataPipeLine        map[string]*chan interface{}
	outDataPipeLine       map[string]*chan interface{}
	userId                string
	startTime             time.Time
	endTime               time.Time
	startCpuUsage         float64
	endCpuUsage           float64
	startMemUsage         float64
	endMemUsage           float64
	execMechine           string
	DataNum               int
	execNode              string
	status                int // 0: not started, 1: running, 2: finished
	finishedComponentsCnt int
	lock                  sync.Mutex // 防止并发访问finishedComponentsCnt变量
	CurrNodeTaskCnt       int
	CurrNodeComponentCnt  int
	CurrNodeDataCnt       int
}

var executors concurrentMap.ConcurrentMap
var lock = &sync.Mutex{}
var getCpuUsageTime time.Time
var getMemUsageTime time.Time
var cpuUsage float64
var memUsage float64

func GetLock() *sync.Mutex {
	return lock
}

func GetExecutor(id string) *Executor {
	exec, _ := executors.Get(id)
	return exec.(*Executor)
}

func GetExecutors() concurrentMap.ConcurrentMap {
	if executors == nil {
		lock.Lock()
		defer lock.Unlock()
		if executors == nil {
			executors = concurrentMap.New()
		}
	}
	return executors
}

func (e *Executor) SetComponents(params interface{}) error {
	componentParams := params.([]interface{})
	for _, v := range componentParams {
		id := v.(map[string]interface{})["id"].(string)
		componentType := v.(map[string]interface{})["type"].(string)
		componentParams := v.(map[string]interface{})["params"].(interface{})
		component, err := componentsFactory.GetComponents(componentType, id, componentParams)
		if err != nil {
			return err
		}
		e.Components[id] = component
	}
	return nil
}

func (e *Executor) GetStartTime() time.Time {

	return e.startTime
}

func (e *Executor) SetStartTime(startTime time.Time) {
	e.startTime = startTime
}

func (e *Executor) SetEndTime(endTime time.Time) {
	e.endTime = endTime
}

func (e *Executor) GetEndTime() time.Time {
	return e.endTime
}

func (e *Executor) SetAdjacentList(params interface{}) error {
	adjacentListParams := params.([]interface{})
	for _, v := range adjacentListParams {
		from := v.(map[string]interface{})["from"].(string)
		to := v.(map[string]interface{})["to"].(string)
		e.adjacentList[from] = append(e.adjacentList[from], to)
	}
	return nil
}

func (e *Executor) GetAdjacentList() map[string][]string {
	return e.adjacentList
}

func (e *Executor) GetCpuUsage(startFlag bool) {
	nowTime := time.Now()
	if getCpuUsageTime == nowTime {
		if startFlag {
			e.startCpuUsage = cpuUsage
		} else {
			e.endCpuUsage = cpuUsage
		}
		return
	}
	getCpuUsageTime = nowTime
	client := &http.Client{Timeout: 5 * time.Second}
	url := "http://192.168.102.21:30351/api/v1/query?query=100*(1-sum%20by(instance)(increase(node_cpu_seconds_total{instance=\"" + e.execNode + "\",mode=\"idle\"}[1m]))/sum%20by(instance)(increase(node_cpu_seconds_total{instance=\"" + e.execNode + "\"}[1m])))"
	resp, err := client.Get(url)
	if err != nil {
		e.startCpuUsage = -999999
		log.Println(err)
		return
	}
	defer resp.Body.Close()
	buffer := make([]byte, 2048)
	result := bytes.NewBuffer(nil)
	for {
		n, err := resp.Body.Read(buffer[0:])
		result.Write(buffer[0:n])
		if err != nil && err == io.EOF {
			break
		} else if err != nil {
			e.startCpuUsage = -999999
			log.Println(err)
			return
		}
	}
	var data map[string]interface{}
	err = json.Unmarshal(result.Bytes(), &data)
	if err != nil {
		log.Println(err)
		e.startCpuUsage = 0
		return
	}
	if len(data["data"].(map[string]interface{})["result"].([]interface{})) == 0 {
		e.startCpuUsage = -9999
		return
	}
	if len(data["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["value"].([]interface{})) == 0 {
		e.startCpuUsage = -9999
		return
	}
	tmpStr := data["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["value"].([]interface{})[1].(string)
	if startFlag {
		if cpuUsage, err = strconv.ParseFloat(tmpStr, 64); err != nil {
			log.Println(err)
			e.startCpuUsage = 0
			cpuUsage = 0
		} else {
			e.startCpuUsage = cpuUsage
		}
	} else {
		if cpuUsage, err = strconv.ParseFloat(tmpStr, 64); err != nil {
			log.Println(err)
			e.endCpuUsage = 0
			cpuUsage = 0
		} else {
			e.endCpuUsage = cpuUsage
		}
	}
}

func (e *Executor) GetMemUsage(startFlag bool) {
	nowTime := time.Now()
	if getMemUsageTime == nowTime {
		if startFlag {
			e.startMemUsage = memUsage
		} else {
			e.endMemUsage = memUsage
		}
		return
	}
	getMemUsageTime = nowTime
	client := &http.Client{Timeout: 5 * time.Second}
	url := "http://192.168.102.21:30351/api/v1/query?query=%28node_memory_MemTotal_bytes%7Binstance%3D%22" + e.execNode + "%22%7D-%28node_memory_MemFree_bytes%7Binstance%3D%22" + e.execNode + "%22%7D%2Bnode_memory_Cached_bytes%7Binstance%3D%22" + e.execNode + "%22%7D+%2B+node_memory_Buffers_bytes%7Binstance%3D%22" + e.execNode + "%22%7D%29%29%2Fnode_memory_MemTotal_bytes%7Binstance%3D%22" + e.execNode + "%22%7D*100"
	resp, err := client.Get(url)
	if err != nil {
		e.startMemUsage = -999999
		log.Println(err)
		return
	}
	defer resp.Body.Close()
	var buffer [512]byte
	result := bytes.NewBuffer(nil)
	for {
		n, err := resp.Body.Read(buffer[0:])
		result.Write(buffer[0:n])
		if err != nil && err == io.EOF {
			break
		} else if err != nil {
			e.startMemUsage = -999999
			log.Println(err)
			return
		}
	}
	var data map[string]interface{}
	err = json.Unmarshal(result.Bytes(), &data)
	if err != nil {
		log.Println(err)
		e.startCpuUsage = 0
		return
	}
	if len(data["data"].(map[string]interface{})["result"].([]interface{})) == 0 {
		e.startMemUsage = -9999
		return
	}
	if len(data["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["value"].([]interface{})) == 0 {
		e.startMemUsage = -9999
		return
	}
	tmpStr := data["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["value"].([]interface{})[1].(string)
	if startFlag {
		if memUsage, err = strconv.ParseFloat(tmpStr, 64); err != nil {
			e.startMemUsage = 0
			memUsage = 0
			log.Println(err)
		} else {
			e.startMemUsage = memUsage
		}
	} else {
		if memUsage, err = strconv.ParseFloat(tmpStr, 64); err != nil {
			e.endMemUsage = 0
			memUsage = 0
			log.Println(err)
		} else {
			e.endMemUsage = memUsage
		}
	}
}

func (e *Executor) SetInDegreeList(Components map[string]abstractComponents.VirtualComponents, adjacentList map[string][]string) {
	for k, _ := range Components {
		e.inDegreeList[k] = make([]string, 0)
	}
	for k, v := range adjacentList {
		for _, w := range v {
			e.inDegreeList[w] = append(e.inDegreeList[w], k)
		}
	}
}

func (e *Executor) SetCurrNodeStatus() {
	executors := GetExecutors()
	dataCnt := 0
	componentNum := 0
	taskNum := 0
	tmpe := executors.Items()
	for _, v := range tmpe {
		dataCnt += v.(*Executor).DataNum
		componentNum += len(v.(*Executor).Components)
		taskNum += 1
	}
	e.CurrNodeDataCnt = dataCnt
	e.CurrNodeComponentCnt = componentNum
	e.CurrNodeTaskCnt = taskNum
}

func (e *Executor) SetChans() {
	for component, _ := range e.Components {
		if e.Components[component].GetChanNum() == 1 {
			indata := make(chan interface{}, 100000)
			outdata := make(chan interface{}, 100000)
			e.inDataPipeLine[component] = &indata
			e.outDataPipeLine[component] = &outdata
		} else {
			outdata := make(chan interface{}, 100000)
			for _, v := range e.inDegreeList[component] {
				indata := make(chan interface{}, 100000)
				e.inDataPipeLine[component+v] = &indata
			}
			e.outDataPipeLine[component] = &outdata
		}
	}
}

func GenerateExecutor(params string, execMechine string, dataNum int) (*Executor, error) {
	var executor Executor
	var config map[string]interface{}
	executor.execNode = execMechine
	executor.DataNum = dataNum
	executor.execMechine = execMechine
	if executor.execNode == "" {
		executor.execNode = "k8s-node1"
	}
	if executor.execMechine == "" {
		executor.execMechine = "k8s-node1"
	}
	executor.Components = make(map[string]abstractComponents.VirtualComponents)
	executor.adjacentList = make(map[string][]string)
	executor.inDegreeList = make(map[string][]string)
	executor.inDataPipeLine = make(map[string]*chan interface{})
	executor.outDataPipeLine = make(map[string]*chan interface{})
	executor.status = 0
	executor.finishedComponentsCnt = 0
	executor.lock = sync.Mutex{}
	if err := json.Unmarshal([]byte(params), &config); err != nil {
		return nil, err
	}
	if uid, err := utils.GenerateUUID(); err != nil {
		return nil, err
	} else {
		executor.id = uid
	}
	if config["userId"] != nil {
		executor.userId = config["userId"].(string)
	} else {
		executor.userId = ""
	}
	if config["nodeList"] != nil {
		if err := executor.SetComponents(config["nodeList"]); err != nil {
			return nil, err
		}
	}
	if config["lineList"] != nil {
		if err := executor.SetAdjacentList(config["lineList"]); err != nil {
			return nil, err
		}
	}
	executor.SetInDegreeList(executor.Components, executor.adjacentList)
	executor.SetCurrNodeStatus()
	executor.SetChans()
	return &executor, nil
}

func (e *Executor) Run() {
	// e.GetCpuUsage(true)
	// e.GetMemUsage(true)
	e.status = 1
	e.SetStartTime(time.Now())
	go e.taskScheduling("")
}

// 保存任务执行日志进入数据库
func (e *Executor) saveTaskExecLog() {
	// e.GetCpuUsage(false)
	// e.GetMemUsage(false)
	targetUrl := "http://192.168.102.21:8000/tinyETL/tasklog/"
	payload := strings.NewReader("{\"taskId\":\"" + e.id +
		"\",\"userId\":\"" + e.userId +
		"\",\"startTime\":\"" +
		e.startTime.Format("2006-01-02T15:04:05Z07:00") +
		"\",\"endTime\":\"" +
		e.endTime.Format("2006-01-02T15:04:05Z07:00") +
		"\",\"execTime\":" +
		strconv.Itoa(int(e.endTime.Sub(e.startTime).Seconds())) +
		",\"execNode\":\"" + e.execNode +
		"\",\"currNodeDataCnt\":" +
		strconv.Itoa(e.CurrNodeDataCnt) +
		",\"currNodeComponentCnt\":" +
		strconv.Itoa(e.CurrNodeComponentCnt) +
		",\"currNodeTaskCnt\":" +
		strconv.Itoa(e.CurrNodeTaskCnt) +
		",\"dataNum\":" +
		strconv.Itoa(e.DataNum) +
		",\"startCpuUsage\":" +
		strconv.FormatFloat(e.startCpuUsage, 'f', -1, 64) +
		",\"endCpuUsage\":" +
		strconv.FormatFloat(e.endCpuUsage, 'f', -1, 64) +
		",\"startMemUsage\":" +
		strconv.FormatFloat(e.startMemUsage, 'f', -1, 64) +
		",\"endMemUsage\":" +
		strconv.FormatFloat(e.endMemUsage, 'f', -1, 64) +
		",\"execMechine\":\"" + e.execMechine +
		"\",\"componentNum\":" + strconv.Itoa(len(e.Components)) + "}")
	req, _ := http.NewRequest("POST", targetUrl, payload)
	req.Header.Add("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Println("save task log error:")
		log.Println(err)
	}
	response.Body.Close()
	for _, component := range e.Components {
		payload = strings.NewReader("{\"taskId\":\"" + e.id + "\",\"componentName\":\"" + component.GetName() + "\",\"startTime\":\"" + component.GetStartTime().Format("2006-01-02T15:04:05Z07:00") + "\",\"endTime\":\"" + component.GetEndTime().Format("2006-01-02T15:04:05Z07:00") + "\",\"execTime\"" + ":" + strconv.Itoa(int(component.GetEndTime().Sub(component.GetStartTime()).Seconds())) + "}")
		targetUrl = "http://192.168.102.21:8000/tinyETL/componentlog/"
		req, _ = http.NewRequest("POST", targetUrl, payload)
		req.Header.Add("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println("save component log error:")
			log.Println(err)
		}
		response.Body.Close()
	}
}

// 启动所有节点,单节点任务调度，监听chan中的数据，当接收到数据时，判断
// 是否有后续节点，并判断后续节点是否已经启动，如果没有启动，则启动，
// 如果已经启动，则将数据发送给后续节点。
// Param: componentId
func (e *Executor) taskScheduling(componentId string) {
	// 如果componentId为空，则将所有入度为0的节点启动起来
	if componentId == "" {
		for k, v := range e.inDegreeList {
			if len(v) == 0 {
				dataMeta := make(map[string]map[string]interface{})
				// 对于所有的入度为0的节点，初始化数据管道，以及dataMeta
				go e.Components[k].Run(e.inDataPipeLine[k], e.outDataPipeLine[k], dataMeta)
				go e.taskScheduling(k)
			}
		}
	} else { // 如果componentId不为空，则将其后续节点启动起来
		for true {
			if data, ok := <-*e.outDataPipeLine[componentId]; !ok {
				e.lock.Lock()
				e.Components[componentId].SetStatus(2)
				for _, v := range e.adjacentList[componentId] {
					if e.Components[v].GetChanNum() == 2 {
						close(*e.inDataPipeLine[v+componentId])
					} else {
						closeFlag := true
						for _, vv := range e.inDegreeList[v] {
							if e.Components[vv].GetStatus() != 2 {
								closeFlag = false
								break
							}
						}
						if closeFlag {
							close(*e.inDataPipeLine[v])
						}
					}
				}
				e.finishedComponentsCnt++
				e.lock.Unlock()
				if e.finishedComponentsCnt == len(e.Components) {
					executors := GetExecutors()
					executors.Remove(e.id)
					log.Println("task finished:", e.id)
					e.SetEndTime(time.Now())
					e.saveTaskExecLog()
					e.status = 2
				}
				break
			} else {
				// 取到的数据会发送给后续的每一个节点
				for _, v := range e.adjacentList[componentId] {
					//if _, ok := e.inDataPipeLine[v]; !ok {
					//	indata := make(chan interface{}, 100)
					//	outdata := make(chan interface{}, 100)
					//	e.outDataPipeLine[v] = &outdata
					//	e.inDataPipeLine[v] = &indata
					//	go e.Components[v].Run(&indata, &outdata, e.Components[componentId].GetDataMeta())
					//	go e.taskScheduling(v)
					//}
					// 将取到的数据发送给后续节点
					if e.Components[v].GetStatus() == 0 {
						if e.Components[v].GetChanNum() == 1 {
							dataMeta := utils.DeepCopy(e.Components[componentId].GetDataMeta())
							go e.Components[v].Run(e.inDataPipeLine[v], e.outDataPipeLine[v], dataMeta.(map[string]map[string]interface{}))
							e.Components[v].SetStatus(1)
							go e.taskScheduling(v)
						} else {
							flag := true
							for _, f := range e.inDegreeList[v] {
								if e.Components[f].GetStatus() == 0 {
									flag = false
									break
								}
							}
							if flag {
								var firstDatameta map[string]map[string]interface{}
								var secondDatameta map[string]map[string]interface{}
								for _, f := range e.Components {
									if f.GetId() == e.Components[v].GetFirstInNode() {
										firstDatameta = utils.DeepCopy(f.GetDataMeta()).(map[string]map[string]interface{})
									} else if f.GetId() == e.Components[v].GetSecondInNode() {
										secondDatameta = utils.DeepCopy(f.GetDataMeta()).(map[string]map[string]interface{})
									}
								}
								firstNode := e.Components[v].GetFirstInNode()
								secondNode := e.Components[v].GetSecondInNode()
								go e.Components[v].Run(e.inDataPipeLine[v+firstNode], e.outDataPipeLine[v], firstDatameta, secondDatameta, e.inDataPipeLine[v+secondNode])
								e.Components[v].SetStatus(1)
								go e.taskScheduling(v)
							}
						}
					}
					if e.Components[v].GetChanNum() == 1 {
						*e.inDataPipeLine[v] <- data
					} else {
						*e.inDataPipeLine[v+componentId] <- data
					}
				}
			}
		}
	}
}

func (e *Executor) GetId() string {
	return e.id
}
