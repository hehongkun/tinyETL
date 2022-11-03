package fillDate

import "tinyETL/tinyETLengine/components/abstractComponents"

type fillDateField struct {
	field string
	value string
}

type FillDate struct {
	abstractComponents.AbstractComponent
	fields []fillDateField
}

func (f *FillDate) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
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
			f.processRow(&value, f.fields, &data)
		}
		*outdata <- data
		f.WriteCnt += len(data)
	}
}


func (f *FillDate) processRow(row *[]interface{}, fields []fillDateField, data *[][]interface{}) {
	for _, field := range fields {
		if (*row)[f.DataMeta[field.field]["index"].(int)] == nil {
			(*row)[f.DataMeta[field.field]["index"].(int)] = field.value
		}
	}
	*data = append(*data, *row)
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FillDate{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id: id,
			ReadCnt: 0,
			WriteCnt: 0,
			Name: "FillDate",
			Status: 0,
			ChanNum: 1,
		},
		fields: []fillDateField{},
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, fillDateField{
			field:      value.(map[string]interface{})["field"].(string),
			value:      value.(map[string]interface{})["value"].(string),
		})
	}
	return f, nil
}
