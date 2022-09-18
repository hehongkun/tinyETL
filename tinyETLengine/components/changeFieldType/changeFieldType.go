package changeFieldType

import (
	"log"
	"reflect"
	"strconv"
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
	untilId "tinyETL/tinyETLengine/utils"
)

type dstFieldType struct {
	fieldName   string `json:"fieldName"`
	fieldType   string `json:"fieldType"`
	fieldFormat string `json:"fieldFormat"`
}

type changeFieldType struct {
	dstFieldTypes []dstFieldType
	abstractComponents.AbstractComponent
}

func (c *changeFieldType) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}) {
	c.SetStartTime()
	defer close(*outdata)
	defer c.SetEndTime()
	c.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	for _, dstFieldType := range c.dstFieldTypes {
		c.DataMeta[dstFieldType.fieldName]["type"] = dstFieldType.fieldType
		c.DataMeta[dstFieldType.fieldName]["format"] = dstFieldType.fieldFormat
	}
	tmpDataMeta := utils.DeepCopy(datameta).(map[string]map[string]interface{})
	for {
		data, ok := <-*indata
		if !ok {
			break
		}
		res := c.processRow(data, tmpDataMeta, c.dstFieldTypes)
		*outdata <- res
	}
}

func (c *changeFieldType)processRow(data interface{}, srcDatameta map[string]map[string]interface{}, dstFiledTypes []dstFieldType) interface{} {
	row := data.([]interface{})
	var err error
	for _, dstFieldType := range dstFiledTypes {
		colIdx := srcDatameta[dstFieldType.fieldName]["index"].(int)
		srcColType := reflect.TypeOf(row[colIdx]).Name()
		if dstFieldType.fieldType == srcColType {
			continue
		}
		if dstFieldType.fieldType == "string" {
			if srcColType == "int" {
				row[colIdx] = strconv.Itoa(row[colIdx].(int))
			} else if srcColType == "float" {
				row[colIdx] = strconv.FormatFloat(row[colIdx].(float64), 'f', -1, 64)
			} else if srcColType == "bool" {
				row[colIdx] = strconv.FormatBool(row[colIdx].(bool))
			} else if srcColType == "time" {
				row[colIdx] = time.Time(row[colIdx].(time.Time)).Format(dstFieldType.fieldFormat)
			} else if srcColType == "interface" {
				row[colIdx] = row[colIdx].(string)
			} else if srcColType == "[]uint8" {
				row[colIdx] = string(row[colIdx].([]uint8))
			} else {
				log.Println("Unsupported type:", srcColType)
			}
		} else if dstFieldType.fieldType == "int" {
			if srcColType == "string" {
				row[colIdx], err = strconv.Atoi(row[colIdx].(string))
				if err != nil {
					log.Println(err)
				}
			} else if srcColType == "float" {
				row[colIdx] = int(row[colIdx].(float64))
			} else if srcColType == "interface" {
				row[colIdx] = row[colIdx].(int)
			} else if srcColType == "[]uint8" {
				row[colIdx], err = strconv.Atoi(string(row[colIdx].([]uint8)))
				if err != nil {
					log.Println(err)
				}
			} else {
				log.Println("Unsupported type:", srcColType)
			}
		} else if dstFieldType.fieldType == "float" {
			if srcColType == "string" {
				row[colIdx], err = strconv.ParseFloat(row[colIdx].(string), 64)
				if err != nil {
					log.Println(err)
				}
			} else if srcColType == "int" {
				row[colIdx] = float64(row[colIdx].(int))
			} else if srcColType == "interface" {
				row[colIdx] = row[colIdx].(float64)
			} else if srcColType == "[]uint8" {
				row[colIdx], err = strconv.ParseFloat(string(row[colIdx].([]uint8)), 64)
				if err != nil {
					log.Println(err)
				}
			} else {
				log.Println("Unsupported type:", srcColType)
			}
		} else if dstFieldType.fieldType == "bool" {
			if srcColType == "string" {
				row[colIdx], err = strconv.ParseBool(row[colIdx].(string))
				if err != nil {
					log.Println(err)
				}
			} else if srcColType == "int" {
				row[colIdx] = row[colIdx].(int) != 0
			} else if srcColType == "interface" {
				row[colIdx] = row[colIdx].(bool)
			} else if srcColType == "[]uint8" {
				row[colIdx], err = strconv.ParseBool(string(row[colIdx].([]uint8)))
				if err != nil {
					log.Println(err)
				}
			} else {
				log.Println("Unsupported type:", srcColType)
			}
		} else if dstFieldType.fieldType == "time" {
			if srcColType == "string" {
				if row[colIdx] == nil {
					row[colIdx] = nil
				}
				if dstFieldType.fieldFormat == "YYYY-MM-DD HH:MM:SS" {
					row[colIdx], err = time.Parse("2006-01-02 15:04:05", row[colIdx].(string))
				} else if dstFieldType.fieldFormat == "YYYY-MM-DD" {
					row[colIdx], err = time.Parse("2006-01-02", row[colIdx].(string))
				} else {
					row[colIdx], err = time.Parse("2006-01-02 03:04:05", row[colIdx].(string))
				}
			} else if srcColType == "int" {
				row[colIdx] = time.Unix(int64(row[colIdx].(int)), 0)
			} else if srcColType == "float" {
				row[colIdx] = time.Unix(int64(row[colIdx].(float64)), 0)
			} else if srcColType == "interface" {
				row[colIdx] = time.Time(row[colIdx].(time.Time))
			} else if srcColType == "[]uint8" {
				row[colIdx], err = time.Parse(dstFieldType.fieldFormat, string(row[colIdx].([]uint8)))
				if err != nil {
					log.Println(err)
				}
			} else {
				log.Println("Unsupported type:", srcColType)
			}
		}
	}
	return row
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	params := parameters.(map[string]interface{})
	c := changeFieldType{
		AbstractComponent: abstractComponents.AbstractComponent{
			Id: id,
			ReadCnt: 0,
			WriteCnt: 0,
			Name: "ChangeFieldType",
			Status: 0,
		},
		dstFieldTypes: []dstFieldType{},
	}
	c.Id,_ = untilId.GenerateUUID()
	for _, v := range params["fields"].([]interface{}) {
		dstFieldType := dstFieldType{
			fieldName:   v.(map[string]interface{})["src"].(string),
			fieldType:   v.(map[string]interface{})["fieldType"].(string),
			fieldFormat: v.(map[string]interface{})["fieldFormat"].(string),
		}
		c.dstFieldTypes = append(c.dstFieldTypes, dstFieldType)
	}
	c.SetName("changeFieldType")
	return &c, nil
}
