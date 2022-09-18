package cutString

import (
	"errors"
	"strconv"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
	untilId "tinyETL/tinyETLengine/utils"
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

func (c *cutString) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}) {
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
			vaule = processRow(vaule, datameta[field.inputField]["index"].(int), datameta[field.outputField]["index"].(int), field.startPos, field.endPos)
		}
		*outdata <- vaule
		c.WriteCnt++
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
		},
	}
	c.Id,_ = untilId.GenerateUUID()

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
