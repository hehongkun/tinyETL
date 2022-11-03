package filterNumRange

import (
	"strconv"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type filterNumRangeField struct {
	field      string
	minValue   string
	maxValue   string
	filterType string
}

type FilterNumRange struct {
	abstractComponents.AbstractComponent
	fields []filterNumRangeField
}

func (f *FilterNumRange) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
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

func processRow(data interface{}, fields []filterNumRangeField, datameta map[string]map[string]interface{}) bool {
	for _, field := range fields {
		if data.([]interface{})[datameta[field.field]["index"].(int)] == nil {
			continue
		} else if field.filterType == "in" {
			if datameta[field.field]["type"].(string) == "int" {
				minVal,_ := strconv.ParseInt(field.minValue, 10, 64)
				maxVal,_ := strconv.ParseInt(field.maxValue, 10, 64)
				if data.([]interface{})[datameta[field.field]["index"].(int)].(int64) >= minVal && data.([]interface{})[datameta[field.field]["index"].(int)].(int64) <= maxVal {
					return false
				}
			}else if datameta[field.field]["type"].(string) == "float" {
				minVal,_ := strconv.ParseFloat(field.minValue, 64)
				maxVal,_ := strconv.ParseFloat(field.maxValue, 64)
				if data.([]interface{})[datameta[field.field]["index"].(int)].(float64) >= minVal && data.([]interface{})[datameta[field.field]["index"].(int)].(float64) <= maxVal {
					return false
				}
			}
		} else {
			if datameta[field.field]["type"].(string) == "int" {
				minVal,_ := strconv.ParseInt(field.minValue, 10, 64)
				maxVal,_ := strconv.ParseInt(field.maxValue, 10, 64)
				if data.([]interface{})[datameta[field.field]["index"].(int)].(int64) < minVal || data.([]interface{})[datameta[field.field]["index"].(int)].(int64) > maxVal {
					return false
				}
			}else if datameta[field.field]["type"].(string) == "float" {
				minVal,_ := strconv.ParseFloat(field.minValue, 64)
				maxVal,_ := strconv.ParseFloat(field.maxValue, 64)
				if data.([]interface{})[datameta[field.field]["index"].(int)].(float64) < minVal || data.([]interface{})[datameta[field.field]["index"].(int)].(float64) > maxVal {
					return false
				}
			}
		}
	}
	return true
}


func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FilterNumRange{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "FilterString",
			Status:   0,
			ChanNum: 1,
		},
		fields: make([]filterNumRangeField, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, filterNumRangeField{
			field:      value.(map[string]interface{})["field"].(string),
			minValue:      value.(map[string]interface{})["minValue"].(string),
			maxValue:      value.(map[string]interface{})["maxValue"].(string),
			filterType: value.(map[string]interface{})["filterType"].(string),
		})
	}
	return f, nil
}
