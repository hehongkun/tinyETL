package mysqlOutput

import (
	"bytes"
	"database/sql"
	"log"
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type fieldMapping struct {
	srcField string
	dstField string
}

type mysqlOutput struct {
	username      string `json:"username"`
	password      string `json:"password"`
	host          string `json:"ip"`
	port          string `json:"port"`
	database      string `json:"database"`
	table         string `json:"table"`
	fieldMappings []fieldMapping
	abstractComponents.AbstractComponent
}

func (m *mysqlOutput) GetBatchInsertSql(dataBatchArg *[][]interface{}) (string, []interface{}) {
	dataBatch := *dataBatchArg
	var buffer bytes.Buffer
	buffer.WriteString("insert into ")
	buffer.WriteString(m.table)
	buffer.WriteString(" (")
	for idx, v := range m.fieldMappings {
		buffer.WriteString(v.dstField)
		if idx != len(m.fieldMappings)-1 {
			buffer.WriteString(",")
		}
	}
	vals := make([]interface{}, len(dataBatch)*len(m.fieldMappings))
	buffer.WriteString(") values ")
	for idx, data := range dataBatch {
		buffer.WriteString("(")
		for idx1, v := range m.fieldMappings {
			buffer.WriteString("?")
			if idx1 != len(m.fieldMappings)-1 {
				buffer.WriteString(",")
			}
			vals[idx*len(m.fieldMappings)+idx1] = data[m.DataMeta[v.srcField]["index"].(int)]
		}
		buffer.WriteString(")")
		if idx != len(dataBatch)-1 {
			buffer.WriteString(",")
		}
	}
	insertSql := buffer.String()
	return insertSql, vals
}

func (m *mysqlOutput) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	m.SetStartTime()
	defer close(*outdata)
	defer m.SetEndTime()
	m.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	m.SetStatus(1)
	db, _ := sql.Open("mysql", m.username+":"+m.password+"@tcp("+m.host+":"+m.port+")/"+m.database)
	db.SetConnMaxLifetime(60 * time.Second)
	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		if len(dataBatch.([][]interface{})) == 0 {
			continue
		}
		if len(dataBatch.([][]interface{})) > 10000 {
			for i := 0; i < len(dataBatch.([][]interface{})); i += 10000 {
				j := 1000
				if (i + 1000) > len(dataBatch.([][]interface{})) {
					j = len(dataBatch.([][]interface{})) - i
				}
				tmpDataBatch := dataBatch.([][]interface{})[i:j]
				batchInsertSql, vals := m.GetBatchInsertSql(&tmpDataBatch)
				_, err := db.Exec(batchInsertSql, vals...)
				if err != nil {
					log.Println(err)
				}
			}
		} else {
			tmpDataBatch := dataBatch.([][]interface{})
			batchInsertSql, vals := m.GetBatchInsertSql(&tmpDataBatch)
			_, err := db.Exec(batchInsertSql, vals...)
			if err != nil {
				log.Println(err)
			}
		}
	}
	err := db.Close()
	if err != nil {
		log.Println(err)
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	params := parameters.(map[string]interface{})
	m := mysqlOutput{
		username:      params["username"].(string),
		password:      params["password"].(string),
		host:          params["host"].(string),
		port:          params["port"].(string),
		database:      params["database"].(string),
		table:         params["table"].(string),
		fieldMappings: []fieldMapping{},
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "mysqlOutput",
			Status:   0,
			ChanNum:  1,
		},
	}
	for _, v := range params["fieldMappings"].([]interface{}) {
		m.fieldMappings = append(m.fieldMappings, fieldMapping{
			srcField: v.(map[string]interface{})["srcField"].(string),
			dstField: v.(map[string]interface{})["destField"].(string),
		})
	}
	return &m, nil
}
