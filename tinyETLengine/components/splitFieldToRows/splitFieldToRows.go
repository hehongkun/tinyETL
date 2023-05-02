package splitFieldToRows

import (
	"regexp"
	"strings"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type SplitFieldToRows struct {
	abstractComponents.AbstractComponent
	field         string
	separator     string
	isRegexp      bool
	newField      string
	generateRowId bool
	rowIdField    string
	resetRowId    bool
}

func (s *SplitFieldToRows) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	s.SetStartTime()
	defer close(*outdata)
	defer s.SetEndTime()
	s.DataMeta = datameta
	if _, ok := s.DataMeta[s.newField]; !ok {
		s.DataMeta[s.newField] = map[string]interface{}{
			"index":  len(s.DataMeta),
			"type":   "string",
			"format": "",
		}
	}
	if s.generateRowId {
		if _, ok := s.DataMeta[s.rowIdField]; !ok {
			s.DataMeta[s.rowIdField] = map[string]interface{}{
				"index":  len(s.DataMeta),
				"type":   "int",
				"format": "",
			}
		}
	}
	s.SetStatus(1)
	data := make([][]interface{}, 0)
	rowId := 0
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		s.ReadCnt += len(dataBatch.([][]interface{}))
		for _, value := range dataBatch.([][]interface{}) {
			s.processRow(value, &data, &rowId)
		}
		for len(data) >= 1000 {
			*outdata <- data[:1000]
			s.WriteCnt += 1000
			data = data[1000:]
		}
	}
	if len(data) > 0 {
		*outdata <- data
		s.WriteCnt += len(data)
	}
}

func (s *SplitFieldToRows) processRow(value []interface{}, data *[][]interface{}, rowId *int) {
	if s.resetRowId {
		*rowId = 0
	}
	if value[s.DataMeta[s.field]["index"].(int)] == nil {
		value = append(value, nil)
		if s.generateRowId {
			value = append(value, *rowId)
			*rowId++
		}
		*data = append(*data, value)
		return
	}
	if s.isRegexp {
		spaceRe, _ := regexp.Compile(s.separator)
		for _, v := range spaceRe.Split(value[s.DataMeta[s.field]["index"].(int)].(string), -1) {
			newValue := make([]interface{}, len(value))
			copy(newValue, value)
			newValue = append(newValue, v)
			if s.generateRowId {
				newValue = append(newValue, *rowId)
				*rowId++
			}
			*data = append(*data, newValue)
		}
	} else {
		for _, v := range strings.Split(value[s.DataMeta[s.field]["index"].(int)].(string), s.separator) {
			newValue := make([]interface{}, len(value))
			copy(newValue, value)
			newValue = append(newValue, v)
			if s.generateRowId {
				newValue = append(newValue, *rowId)
				*rowId++
			}
			*data = append(*data, newValue)
		}
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &SplitFieldToRows{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Name:     "SplitFieldToRows",
			Status:   0,
			ReadCnt:  0,
			WriteCnt: 0,
			ChanNum:  1,
		},
		field:         parameters.(map[string]interface{})["field"].(string),
		separator:     parameters.(map[string]interface{})["separator"].(string),
		isRegexp:      parameters.(map[string]interface{})["isRegexp"].(bool),
		newField:      parameters.(map[string]interface{})["newField"].(string),
		generateRowId: parameters.(map[string]interface{})["generateRowId"].(bool),
		rowIdField:    parameters.(map[string]interface{})["rowIdField"].(string),
		resetRowId:    parameters.(map[string]interface{})["resetRowId"].(bool),
	}
	return f, nil
}
