package filterDate

import (
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type FilterDateField struct {
	field string
	date  string
}

type FilterDate struct {
	abstractComponents.AbstractComponent
	fields []FilterDateField
}

func (f *FilterDate) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
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

func processRow(data interface{}, fields []FilterDateField, datameta map[string]map[string]interface{}) bool {
	for _, field := range fields {
		if data.([]interface{})[datameta[field.field]["index"].(int)] == nil {
			continue
		} else if datameta[field.field]["format"].(string) == "YYYY-MM-DD" {
			tmpVal, _ := time.Parse("2006-01-02 15:04:05", field.date)
			val, _ := time.Parse("2006-01-02", tmpVal.Format("2006-01-02"))
			srcVal, _ := time.Parse("2006-01-02", data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).Format("2006-01-02"))
			if srcVal == val {
				return false
			}
		} else if datameta[field.field]["format"].(string) == "YYYY-MM-DD HH:MM:SS" {
			tmpVal, _ := time.Parse("2006-01-02 15:04:05:", field.date)
			val, _ := time.Parse("2006-01-02 15:04:05", tmpVal.Format("2006-01-02 15:04:05"))
			srcVal, _ := time.Parse("2006-01-02 15:04:05", data.([]interface{})[datameta[field.field]["index"].(int)].(time.Time).Format("2006-01-02 15:04:05"))
			if srcVal == val {
				return false
			}
		}
	}
	return true
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FilterDate{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Status:   0,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "filterDate",
			ChanNum:  1,
		},
		fields: make([]FilterDateField, 0),
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, FilterDateField{field: field.(map[string]interface{})["field"].(string), date: field.(map[string]interface{})["value"].(string)})
	}
	return f, nil
}
