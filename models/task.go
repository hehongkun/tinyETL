package models

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinyETL/tinyETLengine/executor"
	"unsafe"

	"github.com/beego/beego/v2/client/orm"
)

type TaskData struct {
	Id           int       `orm:"column(id);auto"`
	Name         string    `orm:"column(name);size(255);null"`
	Data         string    `orm:"column(data);null"`
	Createtime   time.Time `orm:"column(createtime);type(timestamp);null"`
	Updatetime   time.Time `orm:"column(updatetime);type(timestamp);null"`
	UserId       int       `orm:"column(user_id)"`
	DataNum      int       `orm:"column(data_num)"`
	ExecMechine  string    `orm:"column(exec_mechine);size(255);null"`
	ScheduleType int       `orm:"column(schedule_type)"`
}

func (t *TaskData) TableName() string {
	return "task"
}

var pollNum int
var lock sync.Mutex

func init() {
	orm.RegisterModel(new(TaskData))
	pollNum = 0
	lock = sync.Mutex{}
}

// AddTaskData insert a new TaskData into database and returns
// last inserted Id on success.
func AddTaskData(m *TaskData) (id int64, err error) {
	o := orm.NewOrm()
	id, err = o.Insert(m)
	return
}

// GetTaskDataById retrieves TaskData by Id. Returns error if
// Id doesn't exist
func GetTaskDataById(id int) (v *TaskData, err error) {
	o := orm.NewOrm()
	v = &TaskData{Id: id}
	if err = o.Read(v); err == nil {
		return v, nil
	}
	return nil, err
}

// GetAllTaskList retrieves the id and name of TaskData by userid. Returns error if
// Id doesn't exist
func GetAllTaskList(userid int) (ml []interface{}, err error) {
	o := orm.NewOrm()
	qs := o.QueryTable(new(TaskData))
	var l []TaskData
	if _, err := qs.Filter("user_id", userid).All(&l); err != nil {
		return nil, err
	} else {
		for _, v := range l {
			ml = append(ml, v)
		}
	}
	return ml, nil
}

// GetAllTaskData retrieves all TaskData matches certain condition. Returns empty list if
// no records exist
func GetAllTaskData(query map[string]string, fields []string, sortby []string, order []string,
	offset int64, limit int64) (ml []interface{}, err error) {
	o := orm.NewOrm()
	qs := o.QueryTable(new(TaskData))
	// query k=v
	for k, v := range query {
		// rewrite dot-notation to Object__Attribute
		k = strings.Replace(k, ".", "__", -1)
		if strings.Contains(k, "isnull") {
			qs = qs.Filter(k, (v == "true" || v == "1"))
		} else {
			qs = qs.Filter(k, v)
		}
	}
	// order by:
	var sortFields []string
	if len(sortby) != 0 {
		if len(sortby) == len(order) {
			// 1) for each sort field, there is an associated order
			for i, v := range sortby {
				orderby := ""
				if order[i] == "desc" {
					orderby = "-" + v
				} else if order[i] == "asc" {
					orderby = v
				} else {
					return nil, errors.New("Error: Invalid order. Must be either [asc|desc]")
				}
				sortFields = append(sortFields, orderby)
			}
			qs = qs.OrderBy(sortFields...)
		} else if len(sortby) != len(order) && len(order) == 1 {
			// 2) there is exactly one order, all the sorted fields will be sorted by this order
			for _, v := range sortby {
				orderby := ""
				if order[0] == "desc" {
					orderby = "-" + v
				} else if order[0] == "asc" {
					orderby = v
				} else {
					return nil, errors.New("Error: Invalid order. Must be either [asc|desc]")
				}
				sortFields = append(sortFields, orderby)
			}
		} else if len(sortby) != len(order) && len(order) != 1 {
			return nil, errors.New("Error: 'sortby', 'order' sizes mismatch or 'order' size is not 1")
		}
	} else {
		if len(order) != 0 {
			return nil, errors.New("Error: unused 'order' fields")
		}
	}

	var l []TaskData
	qs = qs.OrderBy(sortFields...)
	if _, err = qs.Limit(limit, offset).All(&l, fields...); err == nil {
		if len(fields) == 0 {
			for _, v := range l {
				ml = append(ml, v)
			}
		} else {
			// trim unused fields
			for _, v := range l {
				m := make(map[string]interface{})
				val := reflect.ValueOf(v)
				for _, fname := range fields {
					m[fname] = val.FieldByName(fname).Interface()
				}
				ml = append(ml, m)
			}
		}
		return ml, nil
	}
	return nil, err
}

// UpdateTaskDataById UpdateTaskData updates TaskData by Id and returns error if
// the record to be updated doesn't exist
func UpdateTaskDataById(m *TaskData) (err error) {
	o := orm.NewOrm()
	v := TaskData{Id: m.Id}
	// ascertain id exists in the database
	if err = o.Read(&v); err == nil {
		var num int64
		if num, err = o.Update(m); err == nil {
			fmt.Println("Number of records updated in database:", num)
		}
	}
	return
}

// DeleteTaskData deletes TaskData by Id and returns error if
// the record to be deleted doesn't exist
func DeleteTaskData(id int) (err error) {
	o := orm.NewOrm()
	v := TaskData{Id: id}
	// ascertain id exists in the database
	if err = o.Read(&v); err == nil {
		var num int64
		if num, err = o.Delete(&TaskData{Id: id}); err == nil {
			fmt.Println("Number of records deleted in database:", num)
		}
	}
	return
}

// Run the task of the task data
func Run(taskData *TaskData) (id string, err error) {
	exectors := executor.GetExecutors()
	exec, err := executor.GenerateExecutor(taskData.Data, taskData.ExecMechine, taskData.DataNum)
	if err != nil {
		return "", err
	}
	(*exectors)[exec.GetId()] = exec
	exec.Run()
	return exec.GetId(), nil
}

// Run the task of the task data
func Schedule(taskData *TaskData) (err error) {
	switch taskData.ScheduleType {
	case 0:
		go Poll(taskData)
	case 1:
		Greedy(taskData)
	default:
		return errors.New("Schedule type error")
	}
	return nil
}

func Poll(taskData *TaskData) {
	lock.Lock()
	pollNum = (pollNum + 1) % 4
	tmpPollNum := pollNum
	lock.Unlock()
	url := "http://192.168.102.21:"
	if tmpPollNum == 0 {
		taskData.ExecMechine = "k8s-node1"
		url += "30081"
	} else if tmpPollNum == 1 {
		taskData.ExecMechine = "k8s-node2"
		url += "30082"
	} else if tmpPollNum == 2 {
		taskData.ExecMechine = "k8s-node3"
		url += "30083"
	} else {
		taskData.ExecMechine = "k8s-node4"
		url += "30084"
	}
	url += "/tinyETL/task/run"
	bytesData, _ := json.Marshal(taskData)
	fmt.Println(string(bytesData))
	res, err := http.Post(url,
		"application/json;charset=utf-8", bytes.NewBuffer([]byte(bytesData)))
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		return
	}
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
		return
	}
	str := (*string)(unsafe.Pointer(&content)) //转化为string,优化内存
	fmt.Println(*str)
	err = res.Body.Close()
	if err != nil {
		log.Println(err)
	}
}

func GetCpuUsage(node string) float64 {
	client := &http.Client{Timeout: 5 * time.Second}
	url := "http://192.168.102.21:30351/api/v1/query?" +
		"query=sum(rate(container_cpu_usage_seconds_total{node=\"" + node + "\"}[1m]))" +
		"/sum(machine_cpu_cores{node=\"" + node + "\"})"
	resp, err := client.Get(url)
	if err != nil {
		log.Println(err)
		return 0
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
			log.Println(err)
			return 0
		}
	}
	var data map[string]interface{}
	err = json.Unmarshal(result.Bytes(), &data)
	if err != nil {
		log.Println(err)
		return 0
	}
	tmpStr := data["data"].(map[string]interface{})["result"].([]interface{})[0].(map[string]interface{})["value"].([]interface{})[1].(string)

	if cpuUsage, err := strconv.ParseFloat(tmpStr, 64); err != nil {
		log.Println(err)
		return 0
	} else {
		return cpuUsage
	}
}

func Greedy(taskData *TaskData) {
	k8sNode1Cpu := GetCpuUsage("k8s-node1")
	k8sNode2Cpu := GetCpuUsage("k8s-node2")
	k8sNode3Cpu := GetCpuUsage("k8s-node3")
	k8sNode4Cpu := GetCpuUsage("k8s-node4")
	if k8sNode1Cpu < k8sNode2Cpu && k8sNode1Cpu < k8sNode3Cpu && k8sNode1Cpu < k8sNode4Cpu {
		taskData.ExecMechine = "k8s-node1"
	} else if k8sNode2Cpu < k8sNode1Cpu && k8sNode2Cpu < k8sNode3Cpu && k8sNode2Cpu < k8sNode4Cpu {
		taskData.ExecMechine = "k8s-node2"
	} else if k8sNode3Cpu < k8sNode1Cpu && k8sNode3Cpu < k8sNode2Cpu && k8sNode3Cpu < k8sNode4Cpu {
		taskData.ExecMechine = "k8s-node3"
	} else {
		taskData.ExecMechine = "k8s-node4"
	}
	url := "http://192.168.102.21:"
	if taskData.ExecMechine == "k8s-node1" {
		url += "30081"
	} else if taskData.ExecMechine == "k8s-node2" {
		url += "30082"
	} else if taskData.ExecMechine == "k8s-node3" {
		url += "30083"
	} else {
		url += "30084"
	}
	url += "/tinyETL/task/run"
	bytesData, _ := json.Marshal(taskData)
	res, err := http.Post(url,
		"application/json;charset=utf-8", bytes.NewBuffer([]byte(bytesData)))
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
	}
	defer res.Body.Close()
	content, err := ioutil.ReadAll(res.Body)
	if err != nil {
		fmt.Println("Fatal error ", err.Error())
	}
	str := (*string)(unsafe.Pointer(&content)) //转化为string,优化内存
	fmt.Println(*str)
}
