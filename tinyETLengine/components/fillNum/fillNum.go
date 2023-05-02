package fillNum

import (
	"strconv"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type fillNumField struct {
	field    string
	value    string
	fillType string
}

type FillNum struct {
	abstractComponents.AbstractComponent
	fields []fillNumField
}

func (f *FillNum) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	f.SetStartTime()
	defer close(*outdata)
	defer f.SetEndTime()
	f.DataMeta = datameta
	f.SetStatus(1)
	fillData := make([][]interface{}, 0)
	valueRecord := make([]interface{}, len(f.fields))
	cntRecord := make([]int64, len(f.fields))
	data := make([][]interface{}, 0)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		f.ReadCnt += len(dataBatch.([][]interface{}))

		for _, value := range dataBatch.([][]interface{}) {
			f.processRow(&value, f.fields, &data, &fillData, &valueRecord, &cntRecord)
		}
		if len(data) >= 1000 {
			*outdata <- data
			f.WriteCnt += len(data)
			data = make([][]interface{}, 0)
		}
	}
	if len(data) > 0 {
		*outdata <- data
		f.WriteCnt += len(data)
	}
	if len(fillData) > 0 {
		f.fillNilData(&fillData, &valueRecord, &cntRecord)
		for len(fillData) > 0 {
			if len(fillData) > 1000 {
				*outdata <- fillData[:1000]
				f.WriteCnt += len(fillData[:1000])
			} else {
				*outdata <- fillData
				f.WriteCnt += len(fillData)
			}
			if len(fillData) > 1000 {
				fillData = fillData[1000:]
			} else {
				fillData = make([][]interface{}, 0)
			}
		}
	}
}

func (f *FillNum) fillNilData(fillData *[][]interface{}, valueRecord *[]interface{}, cntRecord *[]int64) {
	for _, data := range *fillData {
		for idx, field := range f.fields {
			if data[f.DataMeta[field.field]["index"].(int)] == nil {
				if field.fillType == "mean" {
					if f.DataMeta[field.field]["type"].(string) == "int" {
						data[f.DataMeta[field.field]["index"].(int)] = int((*valueRecord)[idx].(int64) / (*cntRecord)[idx])
					} else if f.DataMeta[field.field]["type"].(string) == "float" {
						data[f.DataMeta[field.field]["index"].(int)] = (*valueRecord)[idx].(float64) / float64((*cntRecord)[idx])
					}
				} else {
					data[f.DataMeta[field.field]["index"].(int)] = (*valueRecord)[idx]
				}
			}
		}
	}
}

func (f *FillNum) processRow(value *[]interface{}, fields []fillNumField, data *[][]interface{}, fillData *[][]interface{}, valueRecord *[]interface{}, cntRecord *[]int64) {
	flag := false
	for index, field := range fields {
		if (*value)[f.DataMeta[field.field]["index"].(int)] == nil {
			flag = true
		} else {
			if field.fillType == "max" {
				if (*valueRecord)[index] == nil {
					(*valueRecord)[index] = (*value)[f.DataMeta[field.field]["index"].(int)]
				} else {
					if f.DataMeta[field.field]["type"].(string) == "int" {
						if (*value)[f.DataMeta[field.field]["index"].(int)].(int64) > (*valueRecord)[index].(int64) {
							(*valueRecord)[index] = (*value)[f.DataMeta[field.field]["index"].(int)]
						}
					} else if f.DataMeta[field.field]["type"].(string) == "float" {
						if (*value)[f.DataMeta[field.field]["index"].(int)].(float64) > (*valueRecord)[index].(float64) {
							(*valueRecord)[index] = (*value)[f.DataMeta[field.field]["index"].(int)]
						}
					}
				}
			} else if field.fillType == "min" {
				if (*valueRecord)[index] == nil {
					(*valueRecord)[index] = (*value)[f.DataMeta[field.field]["index"].(int)]
				} else {
					if f.DataMeta[field.field]["type"].(string) == "int" {
						if (*value)[f.DataMeta[field.field]["index"].(int)].(int64) < (*valueRecord)[index].(int64) {
							(*valueRecord)[index] = (*value)[f.DataMeta[field.field]["index"].(int)]
						}
					} else if f.DataMeta[field.field]["type"].(string) == "float" {
						if (*value)[f.DataMeta[field.field]["index"].(int)].(float64) < (*valueRecord)[index].(float64) {
							(*valueRecord)[index] = (*value)[f.DataMeta[field.field]["index"].(int)]
						}
					}
				}
			} else if field.fillType == "mean" {
				if f.DataMeta[field.field]["type"].(string) == "int" {
					if (*valueRecord)[index] == nil {
						(*valueRecord)[index] = int64(0)
					}
					(*valueRecord)[index] = (*valueRecord)[index].(int64) + (*value)[f.DataMeta[field.field]["index"].(int)].(int64)
					(*cntRecord)[index] += 1
				} else if f.DataMeta[field.field]["type"].(string) == "float" {
					if (*valueRecord)[index] == nil {
						(*valueRecord)[index] = 0.0
					}
					(*valueRecord)[index] = (*valueRecord)[index].(float64) + (*value)[f.DataMeta[field.field]["index"].(int)].(float64)
					(*cntRecord)[index] += 1
				}
			} else if field.fillType == "constant" {
				if f.DataMeta[field.field]["type"].(string) == "int" {
					val, _ := strconv.ParseInt(field.value, 10, 64)
					(*valueRecord)[index] = val
				} else if f.DataMeta[field.field]["type"].(string) == "float" {
					val, _ := strconv.ParseFloat(field.value, 64)
					(*valueRecord)[index] = val
				}
			}
		}
	}
	if flag {
		*fillData = append(*fillData, *value)
	} else {
		*data = append(*data, *value)
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FillNum{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "FillNum",
			Status:   0,
			ChanNum:  1,
		},
		fields: make([]fillNumField, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, fillNumField{
			field:    value.(map[string]interface{})["field"].(string),
			value:    value.(map[string]interface{})["value"].(string),
			fillType: value.(map[string]interface{})["fillType"].(string),
		})
	}
	return f, nil
}
