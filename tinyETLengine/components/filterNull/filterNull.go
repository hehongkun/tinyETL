package filterNull

import (
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type FilterNull struct {
	fields []string
	abstractComponents.AbstractComponent
}

func (f *FilterNull) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	f.SetStartTime()
	defer close(*outdata)
	defer f.SetEndTime()
	f.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	f.SetStatus(1)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		f.ReadCnt += len(dataBatch.([][]interface{}))
		data := make([][]interface{}, 0)
		for _, value := range dataBatch.([][]interface{}) {
			if processRow(value, f.fields, datameta) {
				data = append(data, value)
			}
		}
		*outdata <- data
		f.WriteCnt += len(data)
	}
}

func processRow(data interface{}, fields []string, datameta map[string]map[string]interface{}) bool {
	for _, field := range fields {
		if data.([]interface{})[datameta[field]["index"].(int)] == nil {
			return false
		}
	}
	return true
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &FilterNull{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			Status:   0,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "filterNull",
			ChanNum:  1,
		},
		fields: make([]string, 0),
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, field.(map[string]interface{})["field"].(string))
	}
	return f, nil
}
