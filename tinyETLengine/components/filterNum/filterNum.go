package filterNum

import (
	"strconv"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type FilterNumField struct {
	field string
	value string
}

type FilterNum struct {
	abstractComponents.AbstractComponent
	fields []FilterNumField
}

func (f *FilterNum) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	f.SetStartTime()
	defer close(*outdata)
	defer f.SetEndTime()
	f.DataMeta = datameta
	f.SetStatus(1)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		f.ReadCnt += len(dataBatch.([][]interface{}))
		data := make([][]interface{}, 0)
		for _, value := range dataBatch.([][]interface{}) {
			if processRow(value, f.fields, datameta) {
				data = append(data, value)
			}
		}
		*outdata <- data
		f.WriteCnt += len(data)
	}
}

func processRow(data interface{}, fields []FilterNumField, datameta map[string]map[string]interface{}) bool {
	for _, field := range fields {
		if data.([]interface{})[datameta[field.field]["index"].(int)] == nil {
			continue
		} else if datameta[field.field]["type"].(string) == "int" {
			val, _ := strconv.ParseInt(field.value, 10, 64)
			if data.([]interface{})[datameta[field.field]["index"].(int)].(int64) == val {
				return false
			}
		} else {
			val, _ := strconv.ParseFloat(field.value, 64)
			if data.([]interface{})[datameta[field.field]["index"].(int)].(float64) == val {
				return false
			}
		}
	}
	return true
}


func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FilterNum{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Status:   0,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "filterNum",
			ChanNum: 1,
		},
		fields: make([]FilterNumField, 0),
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, FilterNumField{field: field.(map[string]interface{})["field"].(string), value: field.(map[string]interface{})["value"].(string)})
	}
	return f, nil
}
