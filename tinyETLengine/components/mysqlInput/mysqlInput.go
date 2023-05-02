package mysqlInput

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"strconv"
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
)

type mysqlInput struct {
	username string `json:"username"`
	password string `json:"password"`
	host     string `json:"ip"`
	port     string `json:"port"`
	database string `json:"database"`
	table    string `json:"table"`
	sql      string `json:"sql"`
	abstractComponents.AbstractComponent
}

func (m *mysqlInput) equal(m1 mysqlInput) bool {
	if m.database == m1.database && m.host == m1.host && m.port == m1.port {
		return true
	}
	return false
}

func (m *mysqlInput) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	m.SetStartTime()
	defer close(*outdata)
	defer m.SetEndTime()
	db, _ := sql.Open("mysql", m.username+":"+m.password+"@tcp("+m.host+":"+m.port+")/"+m.database)
	db.SetConnMaxLifetime(time.Second * 600)
	rows, err := db.Query(m.sql)
	if err != nil {
		log.Fatalln(err)
		return
	}
	cols, err := rows.Columns()
	if err != nil {
		log.Fatalln(err)
		return
	}
	m.setDataMeta(*db, cols)
	m.SetStatus(1)
	values := make([]interface{}, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}
	rowBatch := make([][]interface{}, 0)
	for rows.Next() {
		if err := rows.Scan(scans...); err != nil {
			log.Fatalln(err)
			return
		}
		row := make([]interface{}, len(cols))
		for idx, v := range values {
			if v == nil {
				row[idx] = nil
			} else {
				if m.DataMeta[cols[idx]]["type"] == "int" {
					row[idx], _ = strconv.ParseInt(string(v.([]byte)), 10, 64)
				} else if m.DataMeta[cols[idx]]["type"] == "float" {
					row[idx], _ = strconv.ParseFloat(string(v.([]byte)), 64)
				} else if m.DataMeta[cols[idx]]["type"] == "time" {
					if m.DataMeta[cols[idx]]["format"] == "YYYY-MM-DD HH:MM:SS" {
						row[idx], _ = time.Parse("2006-01-02 15:04:05", string(v.([]byte)))
					} else if m.DataMeta[cols[idx]]["format"] == "YYYY-MM-DD" {
						row[idx], _ = time.Parse("2006-01-02", string(v.([]byte)))
					} else {
						row[idx], _ = time.Parse("2006-01-02 03:04:05", string(v.([]byte)))
					}
				} else {
					row[idx] = string(v.([]byte))
				}
			}
		}
		rowBatch = append(rowBatch, row)
		if len(rowBatch) == 1000 {
			*outdata <- rowBatch
			rowBatch = make([][]interface{}, 0)
		}
	}
	if len(rowBatch) > 0 {
		*outdata <- rowBatch
	}
	err = rows.Close()
	if err != nil {
		log.Println(err)
	}
	err = db.Close()
	if err != nil {
		log.Println(err)
	}
}

func (m *mysqlInput) setDataMeta(db sql.DB, cols []string) {
	m.DataMeta = make(map[string]map[string]interface{})
	dataMetaSql := "select column_name, data_type from information_schema.columns where TABLE_NAME = ? AND TABLE_SCHEMA = ?"
	rows, err := db.Query(dataMetaSql, m.table, m.database)
	if err != nil {
		log.Println(err)
		return
	}
	for rows.Next() {
		var columnName string
		var dataType string
		err := rows.Scan(&columnName, &dataType)
		if err != nil {
			log.Println(err)
			return
		}
		m.DataMeta[columnName] = make(map[string]interface{})
		if dataType == "int" {
			m.DataMeta[columnName]["type"] = "int"
			m.DataMeta[columnName]["format"] = ""
		} else if dataType == "float" {
			m.DataMeta[columnName]["type"] = "float"
			m.DataMeta[columnName]["format"] = ""
		} else if dataType == "decimal" {
			m.DataMeta[columnName]["type"] = "float"
			m.DataMeta[columnName]["format"] = ""
		} else if dataType == "bigint" {
			m.DataMeta[columnName]["type"] = "int"
			m.DataMeta[columnName]["format"] = ""
		} else if dataType == "integer" {
			m.DataMeta[columnName]["type"] = "int"
			m.DataMeta[columnName]["format"] = ""
		} else if dataType == "datetime" {
			m.DataMeta[columnName]["type"] = "time"
			m.DataMeta[columnName]["format"] = "YYYY-MM-DD HH:MM:SS"
		} else if dataType == "date" {
			m.DataMeta[columnName]["type"] = "time"
			m.DataMeta[columnName]["format"] = "YYYY-MM-DD"
		} else if dataType == "timestamp" {
			m.DataMeta[columnName]["type"] = "time"
			m.DataMeta[columnName]["format"] = "YYYY-MM-DD HH:MM:SS"
		} else if dataType == "time" {
			m.DataMeta[columnName]["type"] = "time"
			m.DataMeta[columnName]["format"] = "YYYY-MM-DD HH:MM:SS"
		} else {
			m.DataMeta[columnName]["type"] = "string"
			m.DataMeta[columnName]["format"] = ""
		}
		for idx, col := range cols {
			if col == columnName {
				m.DataMeta[columnName]["index"] = idx
				break
			}
		}
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Println(err)
		}
	}(rows)
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	params := parameters.(map[string]interface{})
	m := mysqlInput{
		username: params["username"].(string),
		password: params["password"].(string),
		host:     params["host"].(string),
		port:     params["port"].(string),
		database: params["database"].(string),
		table:    params["table"].(string),
		sql:      params["sql"].(string),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id:       id,
			ReadCnt:  0,
			WriteCnt: 0,
			Name:     "mysqlInput",
			Status:   0,
			ChanNum:  1,
		},
	}
	m.SetName("mysqlInput")
	return &m, nil
}
