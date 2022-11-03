package filterDateRange

import (
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type filterDateRangeField struct {
	field      string
	minValue   string
	maxValue   string
	filterType string
}

type FilterDateRange struct {
	abstractComponents.AbstractComponent
	fields []filterDateRangeField
}

func (f *FilterDateRange) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
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
		if len(data) > 0 {
			*outdata <- data
			f.WriteCnt += len(data)
		}
	}
}

func processRow(data interface{}, fields []filterDateRangeField, datameta map[string]map[string]interface{}) bool {
	for _, field := range fields {
		if data.([]interface{})[datameta[field.field]["index"].(int)] == nil {
			continue
		} else if field.filterType == "in" {
			if datameta[field.field]["format"].(string) == "YYYY-MM-DD" {
				minVal, _ := time.Parse("2006-01-02", field.minValue)
				maxVal, _ := time.Parse("2006-01-02", field.maxValue)
				if data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).After(minVal) && data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).Before(maxVal) {
					return false
				}
			} else if datameta[field.field]["format"].(string) == "YYYY-MM-DD HH:MM:SS" {
				minVal, _ := time.Parse("2006-01-02 15:04:05", field.minValue)
				maxVal, _ := time.Parse("2006-01-02 15:04:05", field.maxValue)
				if data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).After(minVal) && data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).Before(maxVal) {
					return false
				}
			}
		}else {
			if datameta[field.field]["format"].(string) == "YYYY-MM-DD" {
				minVal, _ := time.Parse("2006-01-02", field.minValue)
				maxVal, _ := time.Parse("2006-01-02", field.maxValue)
				if data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).After(maxVal) || data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).Before(minVal) {
					return false
				}
			} else if datameta[field.field]["format"].(string) == "YYYY-MM-DD HH:MM:SS" {
				minVal, _ := time.Parse("2006-01-02 15:04:05", field.minValue)
				maxVal, _ := time.Parse("2006-01-02 15:04:05", field.maxValue)
				if data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).After(maxVal) || data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).Before(minVal) {
					return false
				}
			}
		}
	}
	return true
}
func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FilterDateRange{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "FilterString",
			Status:   0,
			ChanNum: 1,
		},
		fields: make([]filterDateRangeField, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, filterDateRangeField{
			field:      value.(map[string]interface{})["field"].(string),
			minValue:   value.(map[string]interface{})["minValue"].(string),
			maxValue:   value.(map[string]interface{})["maxValue"].(string),
			filterType: value.(map[string]interface{})["filterType"].(string),
		})
	}
	return f, nil
}
