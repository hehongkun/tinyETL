package rightJoin

import (
	"log"
	"os"
	"sync"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/utils"
)

type joinFields struct {
	FirstField  string
	SecondField string
}

type RightJoin struct {
	abstractComponents.AbstractComponent
	first  string
	second string
	fields []joinFields
}

func DumpData(group *sync.WaitGroup, indata *chan interface{}, file *os.File, length *int) {
	defer group.Done()
	for {
		data, ok := <-*indata
		if !ok {
			break
		}
		err := utils.DumpToFile(data.([][]interface{}), file)
		if err != nil {
			log.Println(err)
			return
		}
		*length += len(data.([][]interface{}))
	}
}

func (l *RightJoin) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	l.SetStartTime()
	defer close(*outdata)
	defer l.SetEndTime()
	l.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	var datameta1 map[string]map[string]interface{}
	var indata1 *chan interface{}
	for _, o := range otherChannels {
		switch o.(type) {
		case map[string]map[string]interface{}:
			datameta1 = utils.DeepCopy(o.(map[string]map[string]interface{})).(map[string]map[string]interface{})
			for key, value := range o.(map[string]map[string]interface{}) {
				flag := false
				for _, field := range l.fields {
					if field.SecondField == key {
						flag = true
						break
					}
				}
				if !flag {
					if _, ok := l.DataMeta[key]; !ok {
						l.DataMeta[key] = value
						l.DataMeta[key]["index"] = len(l.DataMeta) - 1
					} else {
						datameta1[key+"_1"] = value
						l.DataMeta[key+"_1"] = value
						delete(datameta1, key)
						for k, h := range l.fields {
							if h.SecondField == key {
								l.fields[k].SecondField = key + "_1"
							}
						}
					}
				}
			}
		case *chan interface{}:
			indata1 = o.(*chan interface{})
		}
	}
	l.SetStatus(1)
	var err error
	fIdx := 0
	fLen := 0
	sIdx := 0
	sLen := 0
	fFileIdx := 0
	sFileIdx := 0
	fFilename := "./tmpFiles/" + utils.RandFileName("")
	sFilename := "./tmpFiles/" + utils.RandFileName("")
	for sFilename == fFilename {
		sFilename = "./tmpFiles/" + utils.RandFileName("")
	}
	var fFile *os.File
	var sFile *os.File
	fFile, err = os.OpenFile(fFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Println(err)
	}
	sFile, err = os.OpenFile(sFilename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		log.Println(err)
	}
	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go DumpData(&waitGroup, indata, fFile, &fLen)
	go DumpData(&waitGroup, indata1, sFile, &sLen)
	waitGroup.Wait()
	fData := make([][]interface{}, 0)
	sData := make([][]interface{}, 0)
	oData := make([][]interface{}, 0)
	for fIdx < fLen {
		joinFlag := true
		if fIdx == fFileIdx {
			fFileIdx += Min(100000, fLen-fFileIdx)
			fData, err = utils.LoadFromFile(fFile, datameta, fIdx, fFileIdx)
			if err != nil {
				log.Println(err)
				return
			}
		}
		for sIdx < sLen {
			if sIdx == sFileIdx {
				sFileIdx += Min(100000, sLen-sFileIdx)
				sData, err = utils.LoadFromFile(sFile, datameta1, sIdx, sFileIdx)
				if err != nil {
					log.Println(err)
					return
				}
			}
			flag := true
			for _, field := range l.fields {
				if fData[fIdx][datameta[field.FirstField]["index"].(int)] != sData[sIdx][datameta1[field.SecondField]["index"].(int)] {
					flag = false
					break
				}
			}
			if flag {
				tmpData := make([]interface{}, len(l.DataMeta))
				for _, value := range datameta {
					tmpData[value["index"].(int)] = fData[fIdx][value["index"].(int)]
				}
				for key, value := range datameta1 {
					secondFieldFlag := true
					for _, field := range l.fields {
						if field.SecondField == key {
							secondFieldFlag = false
							break
						}
					}
					if secondFieldFlag {
						tmpData[l.DataMeta[key]["index"].(int)] = sData[sIdx][value["index"].(int)]
					}
				}
				oData = append(oData, tmpData)
				joinFlag = false
			}
			sIdx++
		}
		if joinFlag {
			tmpData := make([]interface{}, len(l.DataMeta))
			for _, value := range datameta {
				tmpData[value["index"].(int)] = fData[fIdx][value["index"].(int)]
			}
			oData = append(oData, tmpData)
		}
		if len(oData) >= 1000 {
			*outdata <- oData
			l.WriteCnt += len(oData)
			oData = make([][]interface{}, 0)
		}
		sIdx = 0
		if sLen > 100000 {
			sFileIdx = 0
		}
		fIdx++
	}
	if len(oData) > 0 {
		*outdata <- oData
		l.WriteCnt += len(oData)
	}
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			log.Println(err)
		}
	}(fFilename)
	defer func(name string) {
		err := os.Remove(name)
		if err != nil {
			log.Println(err)
		}
	}(sFilename)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println(err)
		}
	}(sFile)
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println(err)
		}
	}(fFile)
}

func Min(i int, i2 int) int {
	if i < i2 {
		return i
	}
	return i2
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	f := &RightJoin{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:           id,
			ReadCnt:      0,
			WriteCnt:     0,
			Name:         "RightJoin",
			Status:       0,
			ChanNum:      2,
			FirstInNode:  parameters.(map[string]interface{})["second"].(string),
			SecondInNode: parameters.(map[string]interface{})["first"].(string),
		},
		first:  parameters.(map[string]interface{})["second"].(string),
		second: parameters.(map[string]interface{})["first"].(string),
		fields: make([]joinFields, 0),
	}
	for _, value := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		f.fields = append(f.fields, joinFields{
			FirstField:  value.(map[string]interface{})["secondField"].(string),
			SecondField: value.(map[string]interface{})["firstField"].(string),
		})
	}
	return f, nil
}
