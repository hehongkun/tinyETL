package changeSequenceByValue

import (
	"strconv"
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type ChangeSequenceByValue struct {
	abstractComponents.AbstractComponent
	field        string
	startNum     int64
	step         int64
	targetFields []string
}

func (c *ChangeSequenceByValue) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	c.SetStartTime()
	defer close(*outdata)
	defer c.SetEndTime()
	c.DataMeta = datameta
	if _, ok := c.DataMeta[c.field]; !ok {
		c.DataMeta[c.field] = map[string]interface{}{
			"index":  len(c.DataMeta),
			"type":   "int",
			"format": "",
		}
	} else {
		c.DataMeta[c.field]["type"] = "int"
		c.DataMeta[c.field]["format"] = ""
	}
	c.SetStatus(1)
	data := make([][]interface{}, 0)
	record := make(map[string]int64)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		c.ReadCnt += len(dataBatch.([][]interface{}))
		for _, value := range dataBatch.([][]interface{}) {
			str := c.processRow(value)
			var tmpVal []interface{}
			tmpVal = append(tmpVal, value...)
			if _, ok := record[str]; !ok {
				record[str] = c.startNum

			} else {
				record[str] += c.step
			}
			tmpVal[c.DataMeta[c.field]["index"].(int)] = record[str]
			data = append(data, tmpVal)
		}
		*outdata <- data
		data = make([][]interface{}, 0)
		c.WriteCnt += len(data)
	}
}

func (c *ChangeSequenceByValue) processRow(value []interface{}) string {
	str := ""
	for _, field := range c.targetFields {
		if value[c.DataMeta[field]["index"].(int)] == nil {
			str += "nil"
			continue
		}
		if c.DataMeta[field]["type"] == "int" {
			str += strconv.FormatInt(value[c.DataMeta[field]["index"].(int)].(int64), 10)
		} else if c.DataMeta[field]["type"] == "float" {
			str += strconv.FormatFloat(value[c.DataMeta[field]["index"].(int)].(float64), 'f', -1, 64)
		} else if c.DataMeta[field]["type"] == "string" {
			str += value[c.DataMeta[field]["index"].(int)].(string)
		} else if c.DataMeta[field]["type"] == "time" {
			if c.DataMeta[field]["format"] == "YYYY-MM-DD" {
				str += value[c.DataMeta[field]["index"].(int)].(time.Time).Format("2006-01-02")
			} else {
				str += value[c.DataMeta[field]["index"].(int)].(time.Time).Format("2006-01-02 15:04:05")
			}
		}
	}
	return str
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	c := &ChangeSequenceByValue{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Name:     "ChangeSequenceByValue",
			Status:   0,
			ReadCnt:  0,
			WriteCnt: 0,
			ChanNum:  1,
		},
		field:        parameters.(map[string]interface{})["field"].(string),
		targetFields: []string{},
	}
	c.startNum, _ = strconv.ParseInt(parameters.(map[string]interface{})["startNum"].(string), 10, 64)
	c.step, _ = strconv.ParseInt(parameters.(map[string]interface{})["step"].(string), 10, 64)
	for _, v := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		c.targetFields = append(c.targetFields, v.(map[string]interface{})["targetField"].(string))
	}
	return c, nil
}
