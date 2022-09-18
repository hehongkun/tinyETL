package removeDuplicateRecord

import (
	"reflect"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
	untilId "tinyETL/tinyETLengine/utils"
)

type RemoveDuplicateRecord struct {
	fields []string
	abstractComponents.AbstractComponent
}


func (c *RemoveDuplicateRecord) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}) {
	c.SetStartTime()
	defer close(*outdata)
	defer c.SetEndTime()
	c.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	recordAppear := make([]interface{}, len(c.fields))
	for {
		vaule, ok := <-*indata
		if !ok {
			break
		}
		c.ReadCnt++
		tmp := make([]interface{}, len(c.fields))
		for i, field := range c.fields {
			tmp[i] = vaule.([]interface{})[datameta[field]["index"].(int)]
		}
		flag := true
		for _,r := range recordAppear {
			if reflect.DeepEqual(r, tmp) {
				flag = false
				break
			}
		}
		if flag {
			recordAppear = append(recordAppear, tmp)
			*outdata <- vaule
			c.WriteCnt++
		}
	}
}


func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	c := &RemoveDuplicateRecord{
		fields: make([]string, 0),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Status: 0,
			ReadCnt: 0,
			WriteCnt: 0,
			Name: "removeDuplicateRecord",
		},
	}
	c.Id,_ = untilId.GenerateUUID()
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		c.fields = append(c.fields, field.(map[string]interface{})["field"].(string))
	}
	return c, nil
}