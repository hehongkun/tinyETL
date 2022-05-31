package mysqlOutput

import (
	"database/sql"
	"log"
	"tinyETL/tinyETLengine/components/abstractComponents"
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

func (m mysqlOutput) Run(indata chan interface{}, outdata chan interface{}, datameta map[string]map[string]interface{}) {
	defer close(outdata)
	db, _ := sql.Open("mysql", m.username+":"+m.password+"@tcp("+m.host+":"+m.port+")/"+m.database)
	var sql string
	sql = "INSERT INTO " + m.table + " ("
	for _, v := range m.fieldMappings {
		sql += v.dstField + ","
	}
	sql = sql[:len(sql)-1] + ") VALUES ("
	for _ = range m.fieldMappings {
		sql += "?,"
	}
	sql = sql[:len(sql)-1] + ")"
	stmt, err := db.Prepare(sql)
	if err != nil {
		log.Fatalln(err)
		return
	}
	for {
		value, ok := <-indata
		if !ok {
			break
		}
		var values []interface{}
		for _, v := range m.fieldMappings {
			if value.([]interface{})[datameta[v.srcField]["index"].(int)] == nil {
				values = append(values, "")
			} else {
				values = append(values, value.([]interface{})[datameta[v.srcField]["index"].(int)])
			}
		}
		_, err = stmt.Exec(values...)
		if err != nil {
			log.Fatalln("mysqlOutput:" + err.Error())
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
	}
	for _, v := range params["fieldMappings"].([]interface{}) {
		m.fieldMappings = append(m.fieldMappings, fieldMapping{
			srcField: v.(map[string]interface{})["srcField"].(string),
			dstField: v.(map[string]interface{})["destField"].(string),
		})
	}
	return &m, nil
}
