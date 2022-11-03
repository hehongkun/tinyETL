package fillString

import "tinyETL/tinyETLengine/components/abstractComponents"

type fillStringField struct {
	field 	string
	value 	string
}

type FillString struct {
	abstractComponents.AbstractComponent
	fields []fillStringField
}

func (f *FillString) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
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
		for _, value := range dataBatch.([][]interface{}) {
			f.processRow(&value, f.fields)
		}
		*outdata <- dataBatch
		f.WriteCnt += len(dataBatch.([][]interface{}))
	}
}


func (f *FillString) processRow(row *[]interface{}, fields []fillStringField) {
	for _, field := range fields {
		if (*row)[f.DataMeta[field.field]["index"].(int)] == nil {
			(*row)[f.DataMeta[field.field]["index"].(int)] = field.value
		}
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FillString{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id: id,
			ReadCnt: 0,
			WriteCnt: 0,
			Name: "FilterString",
			Status: 0,
			ChanNum: 1,
		},
		fields: make([]fillStringField, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, fillStringField{
			field:      value.(map[string]interface{})["field"].(string),
			value:      value.(map[string]interface{})["value"].(string),
		})
	}
	return f, nil
}
