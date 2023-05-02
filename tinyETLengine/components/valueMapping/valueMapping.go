package valueMapping

import (
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type ValueMapping struct {
	srcField     string
	dstField     string
	defaultValue string
	mappings     map[string]string
	abstractComponents.AbstractComponent
}

func (v *ValueMapping) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	v.SetStartTime()
	defer close(*outdata)
	defer v.SetEndTime()
	if v.dstField == "" {
		v.dstField = v.srcField
	} else {
		if _, ok := datameta[v.dstField]; !ok {
			datameta[v.dstField] = map[string]interface{}{
				"index":  len(datameta),
				"type":   "string",
				"format": "",
			}
		}
	}
	v.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	v.SetStatus(1)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		v.ReadCnt += len(dataBatch.([][]interface{}))
		data := make([][]interface{}, 0)
		for _, value := range dataBatch.([][]interface{}) {
			data = append(data, processRow(value, v.mappings, v.defaultValue, datameta[v.srcField]["index"].(int), datameta[v.dstField]["index"].(int)).([]interface{}))
			v.WriteCnt++
		}
		*outdata <- data
	}
}

func processRow(data interface{}, mappings map[string]string, defaultValue string, srcFieldIdx int, dstFieldIdx int) interface{} {
	if data.([]interface{})[srcFieldIdx] == nil {
		if defaultValue == "" {
			data.([]interface{})[dstFieldIdx] = nil
		} else {
			data.([]interface{})[dstFieldIdx] = defaultValue
		}
	} else {
		if v, ok := mappings[data.([]interface{})[srcFieldIdx].(string)]; ok {
			data.([]interface{})[dstFieldIdx] = v
		} else {
			if defaultValue == "" {
				data.([]interface{})[dstFieldIdx] = nil
			} else {
				data.([]interface{})[dstFieldIdx] = defaultValue
			}
		}
	}
	return data
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	v := ValueMapping{
		srcField:     parameters.(map[string]interface{})["srcField"].(string),
		dstField:     parameters.(map[string]interface{})["dstField"].(string),
		defaultValue: parameters.(map[string]interface{})["defaultValue"].(string),
		mappings:     make(map[string]string),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Status:   0,
			Name:     "valueMapping",
			ChanNum:  1,
		},
	}
	for _, m := range parameters.(map[string]interface{})["mappings"].([]interface{}) {
		v.mappings[m.(map[string]interface{})["srcValue"].(string)] = m.(map[string]interface{})["destValue"].(string)
	}
	return &v, nil
}
