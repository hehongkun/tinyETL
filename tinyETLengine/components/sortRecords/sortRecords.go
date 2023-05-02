package sortRecords

import (
	"sort"
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type SortFields struct {
	Field    string
	SortType string
}

type SortRecords struct {
	abstractComponents.AbstractComponent
	Fields []SortFields
}

type SortRecord struct {
	Fields   []SortFields
	Data     []interface{}
	DataMeta map[string]map[string]interface{}
}

type SortSlice []SortRecord

func (s SortSlice) Len() int {
	return len(s)
}

func (s SortSlice) Less(i, j int) bool {
	for _, v := range s[i].Fields {
		if s[i].Data[s[i].DataMeta[v.Field]["index"].(int)] == nil {
			if v.SortType == "ascend" {
				return true
			} else {
				return false
			}
		} else if s[j].Data[s[j].DataMeta[v.Field]["index"].(int)] == nil {
			if v.SortType == "ascend" {
				return false
			} else {
				return true
			}
		} else if s[i].Data[s[i].DataMeta[v.Field]["index"].(int)] != s[j].Data[s[j].DataMeta[v.Field]["index"].(int)] {
			if s[i].DataMeta[v.Field]["type"].(string) == "string" {
				if v.SortType == "ascend" {
					return s[i].Data[s[i].DataMeta[v.Field]["index"].(int)].(string) < s[j].Data[s[j].DataMeta[v.Field]["index"].(int)].(string)
				} else {
					return s[i].Data[s[i].DataMeta[v.Field]["index"].(int)].(string) > s[j].Data[s[j].DataMeta[v.Field]["index"].(int)].(string)
				}
			} else if s[i].DataMeta[v.Field]["type"].(string) == "int" {
				if v.SortType == "ascend" {
					return s[i].Data[s[i].DataMeta[v.Field]["index"].(int)].(int64) < s[j].Data[s[j].DataMeta[v.Field]["index"].(int)].(int64)
				} else {
					return s[i].Data[s[i].DataMeta[v.Field]["index"].(int)].(int64) > s[j].Data[s[j].DataMeta[v.Field]["index"].(int)].(int64)
				}
			} else if s[i].DataMeta[v.Field]["type"].(string) == "float" {
				if v.SortType == "ascend" {
					return s[i].Data[s[i].DataMeta[v.Field]["index"].(int)].(float64) < s[j].Data[s[j].DataMeta[v.Field]["index"].(int)].(float64)
				} else {
					return s[i].Data[s[i].DataMeta[v.Field]["index"].(int)].(float64) > s[j].Data[s[j].DataMeta[v.Field]["index"].(int)].(float64)
				}
			} else if s[i].DataMeta[v.Field]["type"].(string) == "time" {
				if v.SortType == "ascend" {
					return s[i].Data[s[i].DataMeta[v.Field]["index"].(int)].(time.Time).Before(s[j].Data[s[j].DataMeta[v.Field]["index"].(int)].(time.Time))
				} else {
					return s[j].Data[s[j].DataMeta[v.Field]["index"].(int)].(time.Time).Before(s[i].Data[s[i].DataMeta[v.Field]["index"].(int)].(time.Time))
				}
			}
		}
	}
	return false
}

func (s SortSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s *SortRecords) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	s.SetStartTime()
	defer close(*outdata)
	defer s.SetEndTime()
	s.DataMeta = datameta
	s.SetStatus(1)
	data := make([]SortRecord, 0)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		s.ReadCnt += len(dataBatch.([][]interface{}))
		for _, value := range dataBatch.([][]interface{}) {
			tmpData := SortRecord{
				Data:     value,
				Fields:   s.Fields,
				DataMeta: s.DataMeta,
			}
			data = append(data, tmpData)
		}
	}
	sort.Sort(SortSlice(data))
	idx := 0
	oData := make([][]interface{}, 0)
	for d := range data {
		oData = append(oData, data[d].Data)
		idx++
		if idx == 1000 {
			*outdata <- oData
			s.WriteCnt += idx
			idx = 0
			oData = make([][]interface{}, 0)
		}
	}
	if idx > 0 {
		*outdata <- oData
		s.WriteCnt += idx
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	s := &SortRecords{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Name:     "SortRecords",
			Status:   0,
			ReadCnt:  0,
			WriteCnt: 0,
			ChanNum:  1,
		},
		Fields: make([]SortFields, 0),
	}
	for _, v := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		s.Fields = append(s.Fields, SortFields{
			Field:    v.(map[string]interface{})["field"].(string),
			SortType: v.(map[string]interface{})["sortType"].(string),
		})
	}
	return s, nil
}
