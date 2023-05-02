package addSequence

import (
	"strconv"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type addSequence struct {
	abstractComponents.AbstractComponent
	field    string
	startNum int64
	step     int64
	maxNum   int64
}

func (a *addSequence) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	a.SetStartTime()
	defer close(*outdata)
	defer a.SetEndTime()
	a.DataMeta = datameta
	if _, ok := a.DataMeta[a.field]; !ok {
		a.DataMeta[a.field] = map[string]interface{}{
			"type":   "int",
			"index":  len(a.DataMeta),
			"format": "",
		}
	} else {
		a.DataMeta[a.field]["type"] = "int"
		a.DataMeta[a.field]["format"] = ""
	}
	a.SetStatus(1)
	data := make([][]interface{}, 0)
	cnt := a.startNum
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		a.ReadCnt += len(dataBatch.([][]interface{}))
		for _, value := range dataBatch.([][]interface{}) {
			value = append(value, cnt)
			cnt += a.step
			if cnt > a.maxNum {
				cnt = a.startNum
			}
			data = append(data, value)
		}
		*outdata <- data
		data = make([][]interface{}, 0)
		a.WriteCnt += len(data)
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	a := &addSequence{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "AddSequence",
			Status:   0,
			ChanNum:  1,
		},
		field: parameters.(map[string]interface{})["field"].(string),
	}
	a.startNum, _ = strconv.ParseInt(parameters.(map[string]interface{})["startNum"].(string), 10, 64)
	a.step, _ = strconv.ParseInt(parameters.(map[string]interface{})["step"].(string), 10, 64)
	a.maxNum, _ = strconv.ParseInt(parameters.(map[string]interface{})["maxNum"].(string), 10, 64)
	return a, nil
}
