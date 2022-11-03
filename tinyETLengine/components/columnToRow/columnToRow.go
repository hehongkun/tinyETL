package columnToRow

import (
	"math"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type aimField struct {
	targetField string
	dataField   string
	groupByType string
	keyValue    string
}

type columnToRow struct {
	keyField      string
	groupByFields []string
	aimFields     []aimField
	abstractComponents.AbstractComponent
}

func (c *columnToRow) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	c.SetStartTime()
	defer close(*outdata)
	defer c.SetEndTime()
	c.DataMeta = make(map[string]map[string]interface{})
	for _, field := range c.groupByFields {
		c.DataMeta[field] = map[string]interface{}{
			"index":  len(c.DataMeta),
			"type":   datameta[field]["type"],
			"format": datameta[field]["format"],
		}
	}
	for _, aimField := range c.aimFields {
		if _, ok := c.DataMeta[aimField.targetField]; !ok {
			c.DataMeta[aimField.targetField] = map[string]interface{}{
				"index":  len(c.DataMeta),
				"type":   datameta[aimField.dataField]["type"],
				"format": "",
			}
		} else {
			c.DataMeta[aimField.targetField]["type"] = "int"
			c.DataMeta[aimField.targetField]["format"] = ""
		}
	}
	delete(c.DataMeta, c.keyField)
	c.SetStatus(1)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		c.ReadCnt += len(dataBatch.([][]interface{}))
		data := make([][]interface{}, 0)
		for _,value := range dataBatch.([][]interface{}) {
			if value[datameta[c.keyField]["index"].(int)] == nil {
				continue
			} else if value[datameta[c.keyField]["index"].(int)] == nil {
				continue
			}
			c.processRow(value, &data, datameta)
		}
		c.WriteCnt += len(data)
		*outdata <- data
	}
}

func (c *columnToRow) processRow(value []interface{}, data *[][]interface{}, datameta map[string]map[string]interface{}) {
	findFlag := false
	for i, tmpData := range *data {
		flag := false
		for _, field := range c.groupByFields {
			if tmpData[c.DataMeta[field]["index"].(int)] != value[datameta[field]["index"].(int)] {
				flag = true
				break
			}
		}
		if !flag {
			for _, targetField := range c.aimFields {
				if value[datameta[c.keyField]["index"].(int)] == targetField.keyValue {
					if targetField.groupByType == "max" {
						if utils.CheckLess(tmpData[c.DataMeta[targetField.targetField]["index"].(int)], value[datameta[targetField.dataField]["index"].(int)], c.DataMeta[targetField.targetField]["type"].(string)) {
							(*data)[i][c.DataMeta[targetField.targetField]["index"].(int)] = value[datameta[targetField.dataField]["index"].(int)]
						}
					} else {
						if utils.CheckGreater(tmpData[c.DataMeta[targetField.targetField]["index"].(int)], value[datameta[targetField.dataField]["index"].(int)], c.DataMeta[targetField.targetField]["type"].(string)) {
							(*data)[i][c.DataMeta[targetField.targetField]["index"].(int)] = value[datameta[targetField.dataField]["index"].(int)]
						}
					}
					break
				}
			}
			findFlag = true
			break
		}
	}
	if !findFlag {
		tmpData := make([]interface{}, len(c.DataMeta))
		for _, field := range c.groupByFields {
			tmpData[c.DataMeta[field]["index"].(int)] = value[datameta[field]["index"].(int)]
		}
		for _, aimField := range c.aimFields {
			if aimField.groupByType == "max" {
				if c.DataMeta[aimField.targetField]["type"].(string) == "int" {
					tmpData[c.DataMeta[aimField.targetField]["index"].(int)] = math.MinInt64
				} else if c.DataMeta[aimField.targetField]["type"].(string) == "float" {
					tmpData[c.DataMeta[aimField.targetField]["index"].(int)] = -math.MaxFloat64
				}
			} else if aimField.groupByType == "min" {
				if c.DataMeta[aimField.targetField]["type"].(string) == "int" {
					tmpData[c.DataMeta[aimField.targetField]["index"].(int)] = math.MaxInt64
				} else if c.DataMeta[aimField.targetField]["type"].(string) == "float" {
					tmpData[c.DataMeta[aimField.targetField]["index"].(int)] = math.MaxFloat64
				}
			}
		}
		for _, targetField := range c.aimFields {
			if value[datameta[c.keyField]["index"].(int)] == targetField.keyValue {
				tmpData[c.DataMeta[targetField.targetField]["index"].(int)] = value[datameta[targetField.dataField]["index"].(int)]
				break
			}
		}
		*data = append(*data, tmpData)
	}
}
func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	c := &columnToRow{
		keyField:      parameters.(map[string]interface{})["keyField"].(string),
		groupByFields: make([]string, 0),
		aimFields:     make([]aimField, 0),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "columnToRow",
			Status: 0,
			ChanNum: 1,
		},
	}
	for _, field := range parameters.(map[string]interface{})["groupByFields"].([]interface{}) {
		c.groupByFields = append(c.groupByFields, field.(map[string]interface{})["groupByField"].(string))
	}
	for _, field := range parameters.(map[string]interface{})["fields"].([]interface{}) {
		c.aimFields = append(c.aimFields, aimField{
			targetField: field.(map[string]interface{})["targetField"].(string),
			dataField:   field.(map[string]interface{})["dataField"].(string),
			groupByType: field.(map[string]interface{})["groupByType"].(string),
			keyValue:    field.(map[string]interface{})["keyValue"].(string),
		})
	}
	return c, nil
}
