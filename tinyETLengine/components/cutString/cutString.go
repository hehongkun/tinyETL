package cutString

import (
	"errors"
	"strconv"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type stringCutField struct {
	inputField  string
	outputField string
	startPos    int
	endPos      int
}

type cutString struct {
	fields []stringCutField
	abstractComponents.AbstractComponent
}

func (c *cutString) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
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
				data = append(data, processRow(value, datameta[field.inputField]["index"].(int), datameta[field.outputField]["index"].(int), field.startPos, field.endPos).([]interface{}))
			}
		}
		*outdata <- data
		c.WriteCnt += len(data)
	}
}

func processRow(value interface{}, srcIdx int, dstIdx int, startPos int, endPos int) interface{} {
	if startPos >= len(value.([]interface{})[srcIdx].(string)) {
		value.([]interface{})[dstIdx] = ""
	} else if endPos >= len(value.([]interface{})[srcIdx].(string)) {
		value.([]interface{})[dstIdx] = value.([]interface{})[srcIdx].(string)[startPos:]
	} else {
		value.([]interface{})[dstIdx] = value.([]interface{})[srcIdx].(string)[startPos:endPos]
	}
	return value
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	c := &cutString{
		fields: make([]stringCutField, 0),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "cutString",
			Status: 0,
			ChanNum: 1,
		},
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		s, err := strconv.Atoi(field.(map[string]interface{})["startPos"].(string))
		if err != nil {
			return nil, errors.New("cutString: startPos is not a number")
		}
		e, err := strconv.Atoi(field.(map[string]interface{})["endPos"].(string))
		if err != nil {
			return nil, errors.New("cutString: startPos is not a number")
		}
		c.fields = append(c.fields, stringCutField{
			inputField:  field.(map[string]interface{})["inputField"].(string),
			outputField: field.(map[string]interface{})["outputField"].(string),
			startPos:    s,
			endPos:      e,
		})
	}
	return c, nil
}
