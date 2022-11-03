package getFieldLength

import (
	"strconv"
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type fieldLengthField struct {
	field       string
	targetField string
}

type GetFieldLength struct {
	abstractComponents.AbstractComponent
	fields []fieldLengthField
}

func (g *GetFieldLength) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	g.SetStartTime()
	defer close(*outdata)
	defer g.SetEndTime()
	g.DataMeta = datameta
	g.SetStatus(1)
	data := make([][]interface{}, 0)
	for _,field := range g.fields {
		if _,ok := g.DataMeta[field.targetField]; !ok {
			g.DataMeta[field.targetField] = map[string]interface{}{
				"type": "int",
				"index": len(g.DataMeta),
				"format": "",
			}
		} else{
			g.DataMeta[field.targetField]["type"] = "int"
			g.DataMeta[field.targetField]["format"] = ""
		}
	}
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		g.ReadCnt += len(dataBatch.([][]interface{}))

		for _, value := range dataBatch.([][]interface{}) {
			g.processRow(&value, g.fields, &data)
		}
		*outdata <- data
		data = make([][]interface{}, 0)
		g.WriteCnt += len(data)
	}
}

func (g *GetFieldLength) processRow(data *[]interface{}, fields []fieldLengthField, outdata *[][]interface{}) {
	tmpData := make([]interface{}, len(g.DataMeta))
	for idx,val := range *data {
		tmpData[idx] = val
	}
	for _, field := range fields {
		if (*data)[g.DataMeta[field.field]["index"].(int)] == nil {
			tmpData[g.DataMeta[field.targetField]["index"].(int)] = 0
		} else if g.DataMeta[field.field]["type"].(string) == "string" {
			tmpData[g.DataMeta[field.targetField]["index"].(int)] = len((*data)[g.DataMeta[field.field]["index"].(int)].(string))
		} else if g.DataMeta[field.field]["type"].(string) == "int" {
			val := strconv.FormatInt(int64((*data)[g.DataMeta[field.field]["index"].(int)].(int64)), 10)
			tmpData[g.DataMeta[field.targetField]["index"].(int)] = len(val)
		} else if g.DataMeta[field.field]["type"].(string) == "float" {
			val := strconv.FormatFloat((*data)[g.DataMeta[field.field]["index"].(int)].(float64), 'f', -1, 64)
			tmpData[g.DataMeta[field.targetField]["index"].(int)] = len(val)
		} else if g.DataMeta[field.field]["type"].(string) == "time" {
			if g.DataMeta[field.field]["format"].(string) == "YYYY-MM-DD" {
				val := (*data)[g.DataMeta[field.field]["index"].(int)].(time.Time).Format("2006-01-02")
				tmpData[g.DataMeta[field.targetField]["index"].(int)] = len(val)
			} else if g.DataMeta[field.field]["format"].(string) == "YYYY-MM-DD HH:MM:SS" {
				val := (*data)[g.DataMeta[field.field]["index"].(int)].(time.Time).Format("2006-01-02 15:04:05")
				tmpData[g.DataMeta[field.targetField]["index"].(int)] = len(val)
			}
		}
	}
	*outdata = append(*outdata, tmpData)
}


func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &GetFieldLength{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "GetFieldLength",
			Status:   0,
			ChanNum: 1,
		},
		fields: make([]fieldLengthField, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, fieldLengthField{
			field:       value.(map[string]interface{})["field"].(string),
			targetField: value.(map[string]interface{})["targetField"].(string),
		})
	}
	return f, nil
}
