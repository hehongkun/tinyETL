package models

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/beego/beego/v2/client/orm"
)

type TaskLog struct {
	TaskId        string    `orm:"column(task_id);size(255);pk"`
	StartTime     time.Time `orm:"column(start_time);type(timestamp);null"`
	EndTime       time.Time `orm:"column(end_time);type(timestamp);null"`
	ExecTime      int64     `orm:"column(exec_time);null"`
	UserId        string    `orm:"column(user_id);size(255);null"`
	ExecMechine   string    `orm:"column(exec_mechine);size(255);null"`
	ExecNode      string    `orm:"column(exec_node);size(255);null"`
	StartCpuUsage float64   `orm:"column(start_cpu_usage);null"`
	EndCpuUsage   float64   `orm:"column(end_cpu_usage);null"`
	StartMemUsage float64   `orm:"column(start_mem_usage);null"`
	EndMemUsage   float64   `orm:"column(end_mem_usage);null"`
	ComponentNum  int       `orm:"column(component_num);null"`
	DataNum       int       `orm:"column(data_num);null"`
}

func (t *TaskLog) TableName() string {
	return "task_log"
}

func init() {
	orm.RegisterModel(new(TaskLog))
}

// AddTaskLog insert a new TaskLog into database and returns
// last inserted Id on success.
func AddTaskLog(m *TaskLog) (id int64, err error) {
	o := orm.NewOrm()
	id, err = o.Insert(m)
	return
}

// GetTaskLogById retrieves TaskLog by Id. Returns error if
// Id doesn't exist
func GetTaskLogById(id string) (v *TaskLog, err error) {
	o := orm.NewOrm()
	v = &TaskLog{TaskId: id}
	if err = o.Read(v); err == nil {
		return v, nil
	}
	return nil, err
}

// GetAllTaskLog retrieves all TaskLog matches certain condition. Returns empty list if
// no records exist
func GetAllTaskLog(query map[string]string, fields []string, sortby []string, order []string,
	offset int64, limit int64) (ml []interface{}, err error) {
	o := orm.NewOrm()
	qs := o.QueryTable(new(TaskLog))
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

	var l []TaskLog
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

// UpdateTaskLog updates TaskLog by Id and returns error if
// the record to be updated doesn't exist
func UpdateTaskLogById(m *TaskLog) (err error) {
	o := orm.NewOrm()
	v := TaskLog{TaskId: m.TaskId}
	// ascertain id exists in the database
	if err = o.Read(&v); err == nil {
		var num int64
		if num, err = o.Update(m); err == nil {
			fmt.Println("Number of records updated in database:", num)
		}
	}
	return
}

// DeleteTaskLog deletes TaskLog by Id and returns error if
// the record to be deleted doesn't exist
func DeleteTaskLog(id string) (err error) {
	o := orm.NewOrm()
	v := TaskLog{TaskId: id}
	// ascertain id exists in the database
	if err = o.Read(&v); err == nil {
		var num int64
		if num, err = o.Delete(&TaskLog{TaskId: id}); err == nil {
			fmt.Println("Number of records deleted in database:", num)
		}
	}
	return
}
