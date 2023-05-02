package fieldSelect

import (
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type selectField struct {
	srcField  string
	destField string
}

type fieldSelect struct {
	abstractComponents.AbstractComponent
	selectFields []selectField
}

func (f *fieldSelect) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	f.SetStartTime()
	defer close(*outdata)
	defer f.SetEndTime()
	f.DataMeta = make(map[string]map[string]interface{})
	for _, field := range f.selectFields {
		f.DataMeta[field.destField] = map[string]interface{}{
			"index":  len(f.DataMeta),
			"type":   datameta[field.srcField]["type"],
			"format": datameta[field.srcField]["format"],
		}
	}
	f.SetStatus(1)
	tmpDataMeta := utils.DeepCopy(datameta).(map[string]map[string]interface{})
	for {
		dataBatch, ok := <-*indata
		f.StartTime = time.Now()
		if !ok {
			break
		}
		f.ReadCnt += len(dataBatch.([][]interface{}))
		data := make([][]interface{}, 0)
		for _, value := range dataBatch.([][]interface{}) {
			data = append(data, f.processRow(value, tmpDataMeta))
		}
		*outdata <- data
		f.WriteCnt += len(data)
	}
}

func (f *fieldSelect) processRow(value []interface{}, datameta map[string]map[string]interface{}) []interface{} {
	res := make([]interface{}, len(f.DataMeta))
	for _, field := range f.selectFields {
		res[f.DataMeta[field.destField]["index"].(int)] = value[datameta[field.srcField]["index"].(int)]
	}
	return res
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &fieldSelect{
		selectFields: make([]selectField, 0),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "fieldSelect",
			Status:   0,
			ChanNum:  1,
		},
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.selectFields = append(f.selectFields, selectField{
			srcField:  field.(map[string]interface{})["srcField"].(string),
			destField: field.(map[string]interface{})["destField"].(string),
		})
	}
	return f, nil
}
