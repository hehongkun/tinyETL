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

func (v *ValueMapping) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}) {
	v.SetStartTime()
	defer close(*outdata)
	defer v.SetEndTime()
	if v.dstField == "" {
		v.dstField = v.srcField
	} else {
		datameta[v.dstField] = map[string]interface{}{
			"index":  len(datameta),
			"type":   "string",
			"format": "",
		}
	}
	v.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	for {
		value, ok := <-*indata
		if !ok {
			break
		}
		v.ReadCnt++
		*outdata <- processRow(value, v.mappings, v.defaultValue, datameta[v.srcField]["index"].(int), datameta[v.dstField]["index"].(int))
		v.WriteCnt++
	}
}

func processRow(data interface{}, mappings map[string]string, defaultValue string, srcFieldIdx int, dstFieldIdx int) interface{} {
	if srcFieldIdx == dstFieldIdx {
		if dstValue, ok := mappings[data.([]interface{})[srcFieldIdx].(string)]; ok {
			data.([]interface{})[dstFieldIdx] = dstValue
		} else {
			if defaultValue != "" {
				data.([]interface{})[dstFieldIdx] = defaultValue
			}
		}
	} else {
		if dstValue, ok := mappings[data.([]interface{})[srcFieldIdx].(string)]; ok {
			data = append(data.([]interface{}), dstValue)
		} else {
			if defaultValue != "" {
				data = append(data.([]interface{}), defaultValue)
			} else {
				data = append(data.([]interface{}), data.([]interface{})[srcFieldIdx])
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
			Name: "ValueMapping",
		},
	}
	for _, m := range parameters.(map[string]interface{})["mappings"].([]interface{}) {
		v.mappings[m.(map[string]interface{})["srcValue"].(string)] = m.(map[string]interface{})["destValue"].(string)
	}
	return &v, nil
}
