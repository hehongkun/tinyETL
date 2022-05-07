package controllers

import (
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"tinyETL/models"
	"tinyETL/utils"

	beego "github.com/beego/beego/v2/server/web"
)

// TaskDataController operations for TaskData
type TaskDataController struct {
	beego.Controller
}

// URLMapping ...
func (c *TaskDataController) URLMapping() {
	c.Mapping("Post", c.Post)
	c.Mapping("GetOne", c.GetOne)
	c.Mapping("GetAll", c.GetAll)
	c.Mapping("GetAllTaskList", c.GetAllTaskList)
	c.Mapping("Put", c.Put)
	c.Mapping("Delete", c.Delete)
}

// Post ...
// @Title Post
// @Description create TaskData
// @Param	body		body 	models.TaskData	true		"body for TaskData content"
// @Success 201 {int} models.TaskData
// @Failure 403 body is empty
// @router /add [post]
func (c *TaskDataController) Post() {
	var v models.TaskData
	ret := make(map[string]interface{})
	if user, err := utils.ValidateToken(c.Ctx.Input.Header("Authorization")); err != nil {
		c.Ctx.ResponseWriter.WriteHeader(401)
		c.Ctx.ResponseWriter.Write([]byte("no permission"))
	} else {
		if err := json.Unmarshal(c.Ctx.Input.RequestBody, &v); err == nil {
			v.UserId = user.Id
			if _, err := models.AddTaskData(&v); err == nil {
				c.Ctx.Output.SetStatus(201)
				ret["data"] = v
				ret["success"] = true
				c.Data["json"] = ret
			} else {
				ret["data"] = err.Error()
				ret["success"] = false
				c.Data["json"] = ret
			}
		} else {
			ret["data"] = err.Error()
			ret["success"] = false
			c.Data["json"] = ret
		}
	}
	c.ServeJSON()
}

// GetOne ...
// @Title Get One
// @Description get TaskData by id
// @Param	id		path 	string	true		"The key for staticblock"
// @Success 200 {object} models.TaskData
// @Failure 403 :id is empty
// @router /gettask:id [get]
func (c *TaskDataController) GetOne() {
	idStr := c.Ctx.Input.Param(":id")
	id, _ := strconv.Atoi(idStr)
	v, err := models.GetTaskDataById(id)
	if err != nil {
		c.Data["json"] = err.Error()
	} else {
		c.Data["json"] = v
	}
	c.ServeJSON()
}

// GetAllTaskList ...
// @Title Get All Task Of a User
// @Description get TaskData
// @Success 200 {object} models.TaskData
// @Failure 403
// @router /getalltasklist [get]
func (c *TaskDataController) GetAllTaskList() {
	if user, err := utils.ValidateToken(c.Ctx.Input.Header("Authorization")); err != nil {
		c.Ctx.ResponseWriter.WriteHeader(401)
		c.Ctx.ResponseWriter.Write([]byte("no permission"))
	} else {
		if taskInfo, err := models.GetAllTaskList(user.Id); err != nil {
			c.Ctx.ResponseWriter.WriteHeader(403)
			c.Ctx.ResponseWriter.Write([]byte("err occured while get taskinfo"))
		} else {
			c.Ctx.ResponseWriter.WriteHeader(200)
			c.Data["json"] = taskInfo
		}
	}
	c.ServeJSON()
}

// GetAll ...
// @Title Get All
// @Description get TaskData
// @Param	query	query	string	false	"Filter. e.g. col1:v1,col2:v2 ..."
// @Param	fields	query	string	false	"Fields returned. e.g. col1,col2 ..."
// @Param	sortby	query	string	false	"Sorted-by fields. e.g. col1,col2 ..."
// @Param	order	query	string	false	"Order corresponding to each sortby field, if single value, apply to all sortby fields. e.g. desc,asc ..."
// @Param	limit	query	string	false	"Limit the size of result set. Must be an integer"
// @Param	offset	query	string	false	"Start position of result set. Must be an integer"
// @Success 200 {object} models.TaskData
// @Failure 403
// @router /getall [get]
func (c *TaskDataController) GetAll() {
	var fields []string
	var sortby []string
	var order []string
	var query = make(map[string]string)
	var limit int64 = 10
	var offset int64

	// fields: col1,col2,entity.col3
	if v := c.GetString("fields"); v != "" {
		fields = strings.Split(v, ",")
	}
	// limit: 10 (default is 10)
	if v, err := c.GetInt64("limit"); err == nil {
		limit = v
	}
	// offset: 0 (default is 0)
	if v, err := c.GetInt64("offset"); err == nil {
		offset = v
	}
	// sortby: col1,col2
	if v := c.GetString("sortby"); v != "" {
		sortby = strings.Split(v, ",")
	}
	// order: desc,asc
	if v := c.GetString("order"); v != "" {
		order = strings.Split(v, ",")
	}
	// query: k:v,k:v
	if v := c.GetString("query"); v != "" {
		for _, cond := range strings.Split(v, ",") {
			kv := strings.SplitN(cond, ":", 2)
			if len(kv) != 2 {
				c.Data["json"] = errors.New("Error: invalid query key/value pair")
				c.ServeJSON()
				return
			}
			k, v := kv[0], kv[1]
			query[k] = v
		}
	}

	l, err := models.GetAllTaskData(query, fields, sortby, order, offset, limit)
	if err != nil {
		c.Data["json"] = err.Error()
	} else {
		c.Data["json"] = l
	}
	c.ServeJSON()
}

// Put ...
// @Title Put
// @Description update the TaskData
// @Param	id		path 	string	true		"The id you want to update"
// @Param	body		body 	models.TaskData	true		"body for TaskData content"
// @Success 200 {object} models.TaskData
// @Failure 403 :id is not int
// @router /edittask:id [put]
func (c *TaskDataController) Put() {
	idStr := c.Ctx.Input.Param(":id")
	id, _ := strconv.Atoi(idStr)
	v := models.TaskData{Id: id}
	if err := json.Unmarshal(c.Ctx.Input.RequestBody, &v); err == nil {
		if err := models.UpdateTaskDataById(&v); err == nil {
			c.Data["json"] = "OK"
		} else {
			c.Data["json"] = err.Error()
		}
	} else {
		c.Data["json"] = err.Error()
	}
	c.ServeJSON()
}

// Delete ...
// @Title Delete
// @Description delete the TaskData
// @Param	id		path 	string	true		"The id you want to delete"
// @Success 200 {string} delete success!
// @Failure 403 id is empty
// @router /deletetask/:id [delete]
func (c *TaskDataController) Delete() {
	idStr := c.Ctx.Input.Param(":id")
	id, _ := strconv.Atoi(idStr)
	if err := models.DeleteTaskData(id); err == nil {
		c.Data["json"] = "OK"
	} else {
		c.Data["json"] = err.Error()
	}
	c.ServeJSON()
}
