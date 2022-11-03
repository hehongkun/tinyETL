package abstractComponents

import "time"

type AbstractComponent struct {
	Id           string
	ReadCnt      int
	WriteCnt     int
	DataMeta     map[string]map[string]interface{} // 这里的dataMeta并不是指本节点的dataMeta,而是指本节点传入下一节点的dataMeta
	StartTime    time.Time
	EndTime      time.Time
	Name         string
	Status       int // 0:未开始 1:正在运行 2:运行结束
	ChanNum      int
	Datameta     []map[string]interface{}
	FirstInNode  string
	SecondInNode string
}

//dataMeta格式：dataMeta["colName"]["type"] = "int" // 列名，列类型
//dataMeta格式：dataMeta["colName"]["index"] = 3 	// 列名，列索引
//dataMeta格式：dataMeta["colName"]["format"] = "xxxx/xx/xx" // 列格式

func (c *AbstractComponent) SetStatus(status int) {
	c.Status = status
}

func (c *AbstractComponent) GetStatus() int {
	return c.Status
}

func (c *AbstractComponent) GetName() string {
	return c.Name
}

func (c *AbstractComponent) SetName(name string) {
	c.Name = name
}

func (c *AbstractComponent) SetId(id string) {
	c.Id = id
}

func (c *AbstractComponent) GetId() string {
	return c.Id
}

func (c *AbstractComponent) GetDataMeta() map[string]map[string]interface{} {
	return c.DataMeta
}

func (c *AbstractComponent) SetDataMeta(dataMeta map[string]map[string]interface{}) {
	c.DataMeta = dataMeta
}

func (c *AbstractComponent) SetStartTime() {
	c.StartTime = time.Now()
}

func (c *AbstractComponent) SetEndTime() {
	c.EndTime = time.Now()
}

func (c *AbstractComponent) GetStartTime() time.Time {
	return c.StartTime
}

func (c *AbstractComponent) GetEndTime() time.Time {
	return c.EndTime
}

func (c *AbstractComponent) GetChanNum() int {
	return c.ChanNum
}


func (c *AbstractComponent) GetFirstInNode() string {
	return c.FirstInNode
}

func (c *AbstractComponent) GetSecondInNode() string {
	return c.SecondInNode
}