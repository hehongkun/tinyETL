package csvOutput

import (
	"encoding/csv"
	"log"
	"os"
	"strconv"
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type CsvOutput struct {
	abstractComponents.AbstractComponent
	Filename     string
	FilenameDate bool
	AllFields    bool
	Header       bool
	Fields       []string
}

func (c *CsvOutput) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	c.SetStartTime()
	defer close(*outdata)
	defer c.SetEndTime()
	c.DataMeta = datameta
	c.SetStatus(1)
	var file *os.File
	if c.FilenameDate {
		c.Filename = c.Filename + "_" + time.Now().Format("20060102")
	}
	c.Filename = c.Filename + ".csv"
	file, _ = os.OpenFile(c.Filename, os.O_CREATE|os.O_WRONLY, 0777)
	defer file.Close()
	writer := csv.NewWriter(file)
	Header := make([]string, 0)
	headerIdx := make(map[string]int)
	if c.Header {
		if c.AllFields {
			idx := 0
			for k, _ := range datameta {
				Header = append(Header, k)
				headerIdx[k] = idx
				idx++
			}
		} else {
			idx := 0
			for k, _ := range datameta {
				flag := false
				for _, f := range c.Fields {
					if k == f {
						flag = true
						break
					}
				}
				if flag {
					Header = append(Header, k)
					headerIdx[k] = idx
					idx++
				}
			}
		}
		err := writer.Write(Header)
		if err != nil {
			log.Println(err)
		}
	}
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		c.ReadCnt += len(dataBatch.([][]interface{}))
		if c.AllFields {
			for _, value := range dataBatch.([][]interface{}) {
				writeData := make([]string, len(dataBatch.([][]interface{})[0]))
				for k, v := range datameta {
					if value[v["index"].(int)] == nil {
						writeData[headerIdx[k]] = ""
						continue
					}
					if v["type"] == "string" {
						writeData[headerIdx[k]] = value[v["index"].(int)].(string)
					} else if v["type"] == "int" {
						writeData[headerIdx[k]] = strconv.FormatInt(value[v["index"].(int)].(int64), 10)
					} else if v["type"] == "float" {
						writeData[headerIdx[k]] = strconv.FormatFloat(value[v["index"].(int)].(float64), 'f', -1, 64)
					} else if v["type"] == "time" {
						if v["format"] == "YYYY-MM-DD HH:MM:SS" {
							writeData[headerIdx[k]] = value[v["index"].(int)].(time.Time).Format("2006-01-02 15:04:05")
						} else if v["format"] == "YYYY-MM-DD" {
							writeData[headerIdx[k]] = value[v["index"].(int)].(time.Time).Format("2006-01-02")
						} else {
							writeData[headerIdx[k]] = value[v["index"].(int)].(time.Time).Format("2006-01-02 03:04:05")
						}
					}
				}
				err := writer.Write(writeData)
				if err != nil {
					log.Println(err)
				}
			}
		} else {
			for _, value := range dataBatch.([][]interface{}) {
				writeData := make([]string, len(c.Fields))
				for k, v := range datameta {
					flag := false
					for _, f := range c.Fields {
						if k == f {
							flag = true
							break
						}
					}
					if flag {
						if value[v["index"].(int)] == nil {
							writeData[headerIdx[k]] = ""
							continue
						} else {
							if v["type"] == "string" {
								writeData[headerIdx[k]] = value[v["index"].(int)].(string)
							} else if v["type"] == "int" {
								writeData[headerIdx[k]] = strconv.FormatInt(value[v["index"].(int)].(int64), 10)
							} else if v["type"] == "float" {
								writeData[headerIdx[k]] = strconv.FormatFloat(value[v["index"].(int)].(float64), 'f', -1, 64)
							} else if v["type"] == "time" {
								if v["format"] == "YYYY-MM-DD HH:MM:SS" {
									writeData[headerIdx[k]] = value[v["index"].(int)].(time.Time).Format("2006-01-02 15:04:05")
								} else if v["format"] == "YYYY-MM-DD" {
									writeData[headerIdx[k]] = value[v["index"].(int)].(time.Time).Format("2006-01-02")
								} else {
									writeData[headerIdx[k]] = value[v["index"].(int)].(time.Time).Format("2006-01-02 03:04:05")
								}
							}
						}
					}
				}
			}
		}
	}
	writer.Flush()
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	c := &CsvOutput{
		Fields:       make([]string, 0),
		Filename:     parameters.(map[string]interface{})["filename"].(string),
		FilenameDate: parameters.(map[string]interface{})["filenameDate"].(bool),
		AllFields:    parameters.(map[string]interface{})["allFields"].(bool),
		Header:       parameters.(map[string]interface{})["header"].(bool),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "csvOutput",
			Status:   0,
			ChanNum:  1,
		},
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		c.Fields = append(c.Fields, field.(map[string]interface{})["field"].(string))
	}
	return c, nil
}
