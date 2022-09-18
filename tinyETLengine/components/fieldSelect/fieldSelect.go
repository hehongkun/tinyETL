package fieldSelect

import (
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
	untilId "tinyETL/tinyETLengine/utils"
)

type selectField struct {
	srcField  string
	destField string
}

type fieldSelect struct {
	abstractComponents.AbstractComponent
	selectFields []selectField
}


func (f *fieldSelect)Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}) {
	defer close(*outdata)
	f.DataMeta = make(map[string]map[string]interface{})
	for _,field := range f.selectFields {
		f.DataMeta[field.destField] = map[string]interface{}{
			"index":  len(f.DataMeta),
			"type":   datameta[field.srcField]["type"],
			"format": datameta[field.srcField]["format"],
		}
	}
	tmpDataMeta:= utils.DeepCopy(datameta).(map[string]map[string]interface{})
	for {
		vaule, ok := <-*indata
		if !ok {
			break
		}
		*outdata <- f.processRow(vaule.([]interface{}),tmpDataMeta)
	}
}

func (f *fieldSelect)processRow(value []interface{},datameta map[string]map[string]interface{}) []interface{} {
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
			Status: 0,
		},
	}
	f.Id,_ = untilId.GenerateUUID()
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.selectFields = append(f.selectFields, selectField{
			srcField:  field.(map[string]interface{})["srcField"].(string),
			destField: field.(map[string]interface{})["destField"].(string),
		})
	}
	return f, nil
}
