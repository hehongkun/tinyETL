package executor

import (
	"encoding/json"
	"sync"
	componentsFactory "tinyETL/tinyETLengine/components"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/utils"
)

type Executor struct {
	id              string
	components      map[string]abstractComponents.VirtualComponents
	adjacentList    map[string][]string
	inDegreeList    map[string][]string
	inDataPipeLine  map[string]chan interface{}
	outDataPipeLine map[string]chan interface{}
}

var executors map[string]*Executor
var lock = &sync.Mutex{}

func GetExecutor(id string) *Executor {
	return executors[id]
}

func GetExecutors() *map[string]*Executor {
	if executors == nil {
		lock.Lock()
		defer lock.Unlock()
		if executors == nil {
			executors = make(map[string]*Executor)
		}
	}
	return &executors
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
		e.components[id] = component
	}
	return nil
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

func (e *Executor) SetInDegreeList(components map[string]abstractComponents.VirtualComponents, adjacentList map[string][]string) {
	for k, _ := range components {
		e.inDegreeList[k] = make([]string, 0)
	}
	for k, v := range adjacentList {
		for _, w := range v {
			e.inDegreeList[w] = append(e.inDegreeList[w], k)
		}
	}
}

func GenerateExecutor(params string) (*Executor, error) {
	var executor Executor
	var config map[string]interface{}
	executor.components = make(map[string]abstractComponents.VirtualComponents)
	executor.adjacentList = make(map[string][]string)
	executor.inDegreeList = make(map[string][]string)
	executor.inDataPipeLine = make(map[string]chan interface{})
	executor.outDataPipeLine = make(map[string]chan interface{})
	if err := json.Unmarshal([]byte(params), &config); err != nil {
		return nil, err
	}
	if uid, err := utils.GenerateUUID(); err != nil {
		return nil, err
	} else {
		executor.id = uid
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
	executor.SetInDegreeList(executor.components, executor.adjacentList)
	return &executor, nil
}

func (e *Executor) Run() {
	go e.taskScheduling("")
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
				indata := make(chan interface{}, 10)
				outdata := make(chan interface{}, 10)
				dataMeta := make(map[string]map[string]interface{})
				e.outDataPipeLine[k] = outdata
				e.inDataPipeLine[k] = indata
				// 对于所有的入度为0的节点，初始化数据管道，以及dataMeta
				go e.components[k].Run(indata, outdata, dataMeta)
				go e.taskScheduling(k)
			}
		}
	} else { // 如果componentId不为空，则将其后续节点启动起来
		for {
			data,ok := <-e.outDataPipeLine[componentId]
			if !ok {
				break
			}
			// 取到的数据会发送给后续的每一个节点
			for _, v := range e.adjacentList[componentId] {
				if _, ok := e.inDataPipeLine[v]; !ok {
					indata := make(chan interface{}, 10)
					outdata := make(chan interface{}, 10)
					e.outDataPipeLine[v] = outdata
					e.inDataPipeLine[v] = indata
					go e.components[v].Run(indata, outdata, e.components[componentId].GetDataMeta())
					go e.taskScheduling(v)
				}
				// 将取到的数据发送给后续节点
				e.inDataPipeLine[v] <- data
			}
		}
	}
}

func (e *Executor) GetId() string {
	return e.id
}
