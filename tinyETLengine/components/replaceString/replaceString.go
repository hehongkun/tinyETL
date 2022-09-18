package replaceString

import (
	"strings"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
	untilId "tinyETL/tinyETLengine/utils"
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

func (c *ReplaceString) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}) {
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
	for {
		vaule, ok := <-*indata
		if !ok {
			break
		}
		c.ReadCnt++
		for _, field := range c.fields {
			if field.outputField == "" {
				field.outputField = field.inputField
			}
			vaule = processRow(vaule, datameta[field.inputField]["index"].(int), datameta[field.outputField]["index"].(int), field.searchStr, field.replaceStr)
		}
		*outdata <- vaule
		c.WriteCnt++
	}
}

func processRow(value interface{}, srcIdx int, dstIdx int, searchStr string, replaceStr string) interface{} {
	value.([]interface{})[dstIdx] = strings.Replace(value.([]interface{})[srcIdx].(string), searchStr, replaceStr, -1)
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
		},
	}
	c.Id,_ = untilId.GenerateUUID()
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
