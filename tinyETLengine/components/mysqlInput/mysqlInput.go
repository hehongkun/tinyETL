package mysqlInput

import (
	"database/sql"
	"fmt"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type mysqlInput struct {
	username string `json:"username"`
	password string `json:"password"`
	host       string `json:"ip"`
	port     string `json:"port"`
	database string `json:"database"`
	sql      string `json:"sql"`
	abstractComponents.AbstractComponent
}


func (m mysqlInput) Run(indata chan interface{}, outdata chan interface{},datameta map[string]map[string]interface{}) {
	defer close(outdata)
	db,_ := sql.Open("mysql", m.username+":"+m.password+"@tcp("+m.host+":"+m.port+")/"+m.database)
	rows, err := db.Query(m.sql)
	if err != nil {
		fmt.Printf("数据导出:执行sql语句错误,%v\n", err)
		return
	}
	cols, err := rows.Columns()
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			fmt.Printf("数据导出:关闭数据库连接错误,%v\n", err)
		}
	}(rows)
	if err != nil {
		fmt.Printf("mysql获取数据错误,%v\n", err)
		return
	}
	dataMeta := make(map[string]map[string]interface{})
	for idx,v := range cols{
		dataMeta[v] = make(map[string]interface{})
		dataMeta[v]["type"] = "string"
		dataMeta[v]["index"] = idx
		dataMeta[v]["format"] = ""
	}
	m.SetDataMeta(dataMeta)
	values := make([][]byte, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			fmt.Printf("数据导出:读取结果集数据错误,%v\n", err)
			return
		}
		row := make([]string, len(cols))
		for idx, v := range values {
			row[idx] = string(v)
		}
		outdata <- row
	}
}



func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents,error) {
	params := parameters.(map[string]interface{})
	m := mysqlInput{
		username: params["username"].(string),
		password: params["password"].(string),
		host:       params["host"].(string),
		port:     params["port"].(string),
		database: params["database"].(string),
		sql:      params["sql"].(string),
	}
	return &m,nil
}

