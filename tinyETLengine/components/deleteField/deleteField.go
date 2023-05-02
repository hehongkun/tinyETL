package deleteField

import (
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/utils"
)

type DeleteField struct {
	abstractComponents.AbstractComponent
	fields []string
}

func (d *DeleteField) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	d.SetStartTime()
	defer close(*outdata)
	defer d.SetEndTime()
	d.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	for _, field := range d.fields {
		delete(d.DataMeta, field)
	}
	d.SetStatus(1)
	data := make([][]interface{}, 0)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		d.ReadCnt += len(dataBatch.([][]interface{}))
		for _, value := range dataBatch.([][]interface{}) {
			d.processRow(&value, d.fields, &data, datameta)
		}
		*outdata <- data
		d.WriteCnt += len(data)
	}
}

func (d *DeleteField) processRow(data *[]interface{}, fields []string, outdata *[][]interface{}, datameta map[string]map[string]interface{}) {
	for _, field := range fields {
		(*data) = append((*data)[:datameta[field]["index"].(int)], (*data)[datameta[field]["index"].(int)+1:]...)
	}
	*outdata = append(*outdata, *data)
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &DeleteField{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "DeleteField",
			Status:   0,
			ChanNum:  1,
		},
		fields: make([]string, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, value.(map[string]interface{})["field"].(string))
	}
	return f, nil
}
