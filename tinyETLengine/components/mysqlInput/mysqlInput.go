package mysqlInput

import (
	"database/sql"
	"log"
	"tinyETL/tinyETLengine/components/abstractComponents"
	untilId "tinyETL/tinyETLengine/utils"
)

type mysqlInput struct {
	username string `json:"username"`
	password string `json:"password"`
	host     string `json:"ip"`
	port     string `json:"port"`
	database string `json:"database"`
	sql      string `json:"sql"`
	abstractComponents.AbstractComponent
}

func (m *mysqlInput) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}) {
	m.SetStartTime()
	defer close(*outdata)
	defer m.SetEndTime()
	db, _ := sql.Open("mysql", m.username+":"+m.password+"@tcp("+m.host+":"+m.port+")/"+m.database)
	rows, err := db.Query(m.sql)
	if err != nil {
		log.Fatalln(err)
		return
	}
	cols, err := rows.Columns()
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Println(err)
		}
	}(rows)
	if err != nil {
		log.Fatalln(err)
		return
	}
	dataMeta := make(map[string]map[string]interface{})
	for idx, v := range cols {
		dataMeta[v] = make(map[string]interface{})
		dataMeta[v]["type"] = "string"
		dataMeta[v]["index"] = idx
		dataMeta[v]["format"] = ""
	}
	m.DataMeta = dataMeta
	values := make([]interface{}, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			log.Fatalln(err)
			return
		}
		row := make([]interface{}, len(cols))
		for idx, v := range values {
			row[idx] = string(v.([]byte))
		}
		*outdata <- row
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	params := parameters.(map[string]interface{})
	m := mysqlInput{
		username: params["username"].(string),
		password: params["password"].(string),
		host:     params["host"].(string),
		port:     params["port"].(string),
		database: params["database"].(string),
		sql:      params["sql"].(string),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id: id,
			ReadCnt: 0,
			WriteCnt: 0,
			Name: "mysqlInput",
			Status: 0,
		},
	}
	m.Id,_ = untilId.GenerateUUID()
	m.SetName("mysqlInput")
	return &m, nil
}
