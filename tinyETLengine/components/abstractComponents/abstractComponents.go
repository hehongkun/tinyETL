package abstractComponents

type AbstractComponent struct {
	Id       string
	ReadCnt  int
	WriteCnt int
	DataMeta map[string]map[string]interface{} // 这里的dataMeta并不是指本节点的dataMeta,而是指本节点传入下一节点的dataMeta
}

//dataMeta格式：dataMeta["colName"]["type"] = "int" // 列名，列类型
//dataMeta格式：dataMeta["colName"]["index"] = 3 	// 列名，列索引
//dataMeta格式：dataMeta["colName"]["format"] = "xxxx/xx/xx" // 列格式

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
