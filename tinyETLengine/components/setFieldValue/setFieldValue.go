package setFieldValue

import "tinyETL/tinyETLengine/components/abstractComponents"

type setFieldValueField struct {
	srcField    string
	targetField string
}

type SetFieldValue struct {
	abstractComponents.AbstractComponent
	fields []setFieldValueField
}

func (s *SetFieldValue) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	s.SetStartTime()
	defer close(*outdata)
	defer s.SetEndTime()
	s.DataMeta = datameta
	for _, v := range s.fields {
		if _, ok := s.DataMeta[v.targetField]; !ok {
			s.DataMeta[v.targetField] = map[string]interface{}{
				"index":  len(s.DataMeta),
				"type":   s.DataMeta[v.srcField]["type"],
				"format": s.DataMeta[v.srcField]["format"],
			}
		} else {
			s.DataMeta[v.targetField]["type"] = s.DataMeta[v.srcField]["type"]
			s.DataMeta[v.targetField]["format"] = s.DataMeta[v.srcField]["format"]
		}
	}
	s.SetStatus(1)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		s.ReadCnt += len(dataBatch.([][]interface{}))
		data := make([][]interface{}, 0)
		for _, value := range dataBatch.([][]interface{}) {
			tmpData := make([]interface{}, len(s.DataMeta))
			for i, v := range value {
				tmpData[i] = v
			}
			for _, v := range s.fields {
				if value[s.DataMeta[v.srcField]["index"].(int)] == nil {
					tmpData[s.DataMeta[v.targetField]["index"].(int)] = nil
				} else {
					tmpData[s.DataMeta[v.targetField]["index"].(int)] = value[s.DataMeta[v.srcField]["index"].(int)]
				}
			}
			data = append(data, tmpData)
		}
		*outdata <- data
		s.WriteCnt += len(data)
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &SetFieldValue{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Name:     "SplitFieldToRows",
			Status:   0,
			ReadCnt:  0,
			WriteCnt: 0,
			ChanNum:  1,
		},
		fields: make([]setFieldValueField, 0),
	}
	for _, v := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, setFieldValueField{
			srcField:    v.(map[string]interface{})["srcField"].(string),
			targetField: v.(map[string]interface{})["targetField"].(string),
		})
	}
	return f, nil
}
