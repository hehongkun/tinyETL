package rowToColumn

import (
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type aimField struct {
	fieldName string
	keyValue   string
	valueField string
}


type rowToColumn struct {
	keyField string
	aimFields []aimField
	transformField string
	abstractComponents.AbstractComponent
}

func (r *rowToColumn)Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	defer close(*outdata)
	r.SetStartTime()
	defer r.SetEndTime()
	r.DataMeta = make(map[string]map[string]interface{})
	r.DataMeta[r.keyField] = map[string]interface{}{
		"index":  0,
		"type":   datameta[r.keyField]["type"],
		"format": datameta[r.keyField]["format"],
	}
	r.DataMeta[r.transformField] = map[string]interface{}{
		"index":  1,
		"type":   "string",
		"format": "",
	}
	r.DataMeta[r.aimFields[0].valueField] = map[string]interface{}{
		"index":  2,
		"type":   datameta[r.aimFields[0].valueField]["type"],
		"format": datameta[r.aimFields[0].valueField]["format"],
	}
	r.SetStatus(1)
	for {
		databBatch, ok := <-*indata
		if !ok {
			break
		}
		r.ReadCnt++
		data := make([][]interface{}, 0)
		for _, value := range databBatch.([][]interface{}) {
			for i, _ := range r.aimFields {
				tmpData := make([]interface{}, 3)
				if _,ok := datameta[r.aimFields[i].fieldName]; ok {
					tmpData[r.DataMeta[r.keyField]["index"].(int)] = value[datameta[r.keyField]["index"].(int)]
					tmpData[r.DataMeta[r.transformField]["index"].(int)] = r.aimFields[i].keyValue
					tmpData[r.DataMeta[r.aimFields[i].valueField]["index"].(int)] = value[datameta[r.aimFields[i].fieldName]["index"].(int)]
					data = append(data, tmpData)
					r.WriteCnt++
				}
			}
		}
		*outdata <- data
	}
}


func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	r := &rowToColumn{
		keyField: parameters.(map[string]interface{})["keyField"].(string),
		transformField: parameters.(map[string]interface{})["transformField"].(string),
		aimFields: make([]aimField, 0),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id: id,
			ReadCnt: 0,
			WriteCnt: 0,
			Name: "rowToColumn",
			Status: 0,
			ChanNum: 1,
		},
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		r.aimFields = append(r.aimFields, aimField{
			fieldName: field.(map[string]interface{})["fieldName"].(string),
			keyValue: field.(map[string]interface{})["keyValue"].(string),
			valueField: field.(map[string]interface{})["valueField"].(string),
		})
	}
	return r, nil
}