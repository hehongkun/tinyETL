package rowFlatten

import (
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type RowFlatten struct {
	abstractComponents.AbstractComponent
	flattenField string
	targetFields []string
}

func (f *RowFlatten) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	f.SetStartTime()
	defer close(*outdata)
	defer f.SetEndTime()
	f.DataMeta = make(map[string]map[string]interface{})
	for _,field := range f.targetFields {
		f.DataMeta[field] = map[string]interface{}{
			"type":   datameta[f.flattenField]["type"],
			"index":  len(f.DataMeta),
			"format": datameta[f.flattenField]["format"],
		}
	}
	f.SetStatus(1)
	data := make([][]interface{}, 0)
	tmpData := make([]interface{},0)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		f.ReadCnt += len(dataBatch.([][]interface{}))
		for _, value := range dataBatch.([][]interface{}) {
			tmpData = append(tmpData, value[datameta[f.flattenField]["index"].(int)])
			if len(tmpData) == len(f.targetFields) {
				data = append(data, tmpData)
				tmpData = make([]interface{},0)
			}
		}
		if len(data) >= 1000 {
			*outdata <- data
			f.WriteCnt += len(data)
			data = make([][]interface{}, 0)
		}
	}
	if len(tmpData)>0 {
		data = append(data, tmpData)
	}
	if len(data) > 0 {
		*outdata <- data
		f.WriteCnt += len(data)
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &RowFlatten{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Name:     "RowFlatten",
			Status:   0,
			ReadCnt:  0,
			WriteCnt: 0,
			ChanNum: 1,
		},
		flattenField: parameters.(map[string]interface{})["flattenField"].(string),
		targetFields: []string{},
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.targetFields = append(f.targetFields, value.(map[string]interface {})["targetField"].(string))
	}
	return f, nil
}
