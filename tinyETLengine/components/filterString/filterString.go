package filterString

import (
	"strings"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type filterStringField struct {
	field      string
	value      string
	filterType string
}

type FilterString struct {
	abstractComponents.AbstractComponent
	fields []filterStringField
}

func (f *FilterString) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	f.SetStartTime()
	defer close(*outdata)
	defer f.SetEndTime()
	f.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
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

func processRow(data interface{}, fields []filterStringField, datameta map[string]map[string]interface{}) bool {
	for _, field := range fields {
		if data.([]interface{})[datameta[field.field]["index"].(int)] == nil {
			continue
		} else if field.filterType == "equal" {
			if data.([]interface{})[datameta[field.field]["index"].(int)].(string) == field.value {
				return false
			}
		} else if field.filterType == "notEqual" {
			if data.([]interface{})[datameta[field.field]["index"].(int)].(string) != field.value {
				return false
			}
		} else if field.filterType == "contain" {
			if strings.Contains(data.([]interface{})[datameta[field.field]["index"].(int)].(string), field.value) {
				return false
			}
		} else if field.filterType == "notContain" {
			if !strings.Contains(data.([]interface{})[datameta[field.field]["index"].(int)].(string), field.value) {
				return false
			}
		}
	}
	return true
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FilterString{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "FilterString",
			Status:   0,
			ChanNum:  1,
		},
		fields: make([]filterStringField, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, filterStringField{
			field:      value.(map[string]interface{})["field"].(string),
			value:      value.(map[string]interface{})["value"].(string),
			filterType: value.(map[string]interface{})["filterType"].(string),
		})
	}
	return f, nil
}
