package mysqlOutput

import (
	"database/sql"
	"log"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
	untilId "tinyETL/tinyETLengine/utils"
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

func (m *mysqlOutput) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}) {
	m.SetStartTime()
	defer close(*outdata)
	defer m.SetEndTime()
	var s string
	var stmt *sql.Stmt
	var err error
	s = "INSERT INTO " + m.table + " ("
	for _, v := range m.fieldMappings {
		s += v.dstField + ","
	}
	s = s[:len(s)-1] + ") VALUES ("
	for _ = range m.fieldMappings {
		s += "?,"
	}
	s = s[:len(s)-1] + ")"
	flag := true
	for {
		value, ok := <-*indata
		if !ok {
			break
		}
		if flag {
			m.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
			db, _ := sql.Open("mysql", m.username+":"+m.password+"@tcp("+m.host+":"+m.port+")/"+m.database)
			defer func(db *sql.DB) {
				err := db.Close()
				if err != nil {
					log.Println(err)
				}
			}(db)
			stmt, err = db.Prepare(s)
			if err != nil {
				log.Println(err)
				return
			}
			flag = false
		}
		values := []interface{}{}
		for _, v := range m.fieldMappings {
			if value.([]interface{})[datameta[v.srcField]["index"].(int)] == nil {
				values = append(values, "")
			} else {
				values = append(values, value.([]interface{})[datameta[v.srcField]["index"].(int)])
			}
		}
		_, err = stmt.Exec(values...)
		if err != nil {
			log.Println("mysqlOutput:" + err.Error())
		}
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
		},
	}
	m.Id, _ = untilId.GenerateUUID()
	for _, v := range params["fieldMappings"].([]interface{}) {
		m.fieldMappings = append(m.fieldMappings, fieldMapping{
			srcField: v.(map[string]interface{})["srcField"].(string),
			dstField: v.(map[string]interface{})["destField"].(string),
		})
	}
	return &m, nil
}
