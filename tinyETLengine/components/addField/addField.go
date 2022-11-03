package addField

import "tinyETL/tinyETLengine/components/abstractComponents"

type fieldConfig struct {
	field     string
	fieldType string
	format    string
}

type AddField struct {
	abstractComponents.AbstractComponent
	fields []fieldConfig
}

func (a *AddField) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	a.SetStartTime()
	defer close(*outdata)
	defer a.SetEndTime()
	a.DataMeta = datameta
	for _,field := range a.fields {
		a.DataMeta[field.field] = map[string]interface{}{
			"index":  len(a.DataMeta),
			"type":   field.fieldType,
			"format": field.format,
		}
	}
	a.SetStatus(1)
	data := make([][]interface{}, 0)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		a.ReadCnt += len(dataBatch.([][]interface{}))
		for _, value := range dataBatch.([][]interface{}) {
			a.processRow(&value, a.fields, &data)
		}
		*outdata <- data
		a.WriteCnt += len(data)
	}
}

func (a *AddField) processRow(data *[]interface{}, fields []fieldConfig, outdata *[][]interface{}) {
	for _ = range fields {
		*data = append(*data, nil)
	}
	*outdata = append(*outdata, *data)
}



func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &AddField{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "AddField",
			Status:   0,
			ChanNum: 1,
		},
		fields: make([]fieldConfig, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, fieldConfig{
			field:    value.(map[string]interface{})["field"].(string),
			fieldType:    value.(map[string]interface{})["fieldType"].(string),
			format: value.(map[string]interface{})["format"].(string),
		})
	}
	return f, nil
}
