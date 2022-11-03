package replaceString

import (
	"strings"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type ReplaceStrField struct {
	inputField  string
	outputField string
	searchStr   string
	replaceStr  string
}

type ReplaceString struct {
	fields []ReplaceStrField
	abstractComponents.AbstractComponent
}

func (c *ReplaceString) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	c.SetStartTime()
	defer close(*outdata)
	defer c.SetEndTime()
	for _, field := range c.fields {
		if _, ok := datameta[field.outputField]; !ok {
			datameta[field.outputField] = map[string]interface{}{
				"index":  len(datameta),
				"type":   "string",
				"format": "",
			}
		}
	}
	c.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	c.SetStatus(1)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		c.ReadCnt += len(dataBatch.([][]interface{}))
		data := make([][]interface{}, 0)
		for _, value := range dataBatch.([][]interface{}) {
			for _, field := range c.fields {
				if field.outputField == "" {
					field.outputField = field.inputField
				} else if value[datameta[field.inputField]["index"].(int)] == nil {
					data = append(data, value)
					continue
				}
				data = append(data,processRow(value, datameta[field.inputField]["index"].(int), datameta[field.outputField]["index"].(int), field.searchStr, field.replaceStr).([]interface{}))
			}
		}
		*outdata <- data
		c.WriteCnt += len(data)
	}
}

func processRow(value []interface{}, srcIdx int, dstIdx int, searchStr string, replaceStr string) interface{} {
	value[dstIdx] = strings.Replace(value[srcIdx].(string), searchStr, replaceStr, -1)
	return value
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	c := &ReplaceString{
		fields: make([]ReplaceStrField, 0),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "replaceString",
			Status: 0,
			ChanNum: 1,
		},
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		c.fields = append(c.fields, ReplaceStrField{
			inputField:  field.(map[string]interface{})["inputField"].(string),
			outputField: field.(map[string]interface{})["outputField"].(string),
			searchStr:   field.(map[string]interface{})["searchStr"].(string),
			replaceStr:  field.(map[string]interface{})["replaceStr"].(string),
		})
	}
	return c, nil
}
