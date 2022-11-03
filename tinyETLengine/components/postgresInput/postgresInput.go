package postgresInput

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
	"reflect"
	"strconv"
	"time"
	"tinyETL/tinyETLengine/components/abstractComponents"
)


type postgresInput struct {
	username string
	password string
	host     string
	port     string
	database string
	table    string
	sql      string
	abstractComponents.AbstractComponent
}

func (p *postgresInput) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	p.SetStartTime()
	defer close(*outdata)
	defer p.SetEndTime()
	db,err := sql.Open("postgres", "user="+p.username+" password="+p.password+" host="+p.host+" port="+p.port+" dbname="+p.database+" sslmode=disable")
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Println(err)
		}
	}(db)
	rows, err := db.Query(p.sql)
	if err != nil {
		log.Fatalln(err)
		return
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Println(err)
		}
	}(rows)
	cols, err := rows.Columns()
	if err != nil {
		log.Fatalln(err)
		return
	}
	p.setDataMeta(db, cols)
	p.SetStatus(1)
	values := make([]interface{}, len(cols))
	scans := make([]interface{}, len(cols))
	for i := range values {
		scans[i] = &values[i]
	}
	rowBatch := make([][]interface{}, 0)
	for rows.Next() {
		p.ReadCnt++
		if err := rows.Scan(scans...); err != nil {
			log.Fatalln(err)
			return
		}
		row := make([]interface{}, len(cols))
		for idx, v := range values {
			if v == nil{
				row[idx] = nil
			} else {
				if p.DataMeta[cols[idx]]["type"] == "int" {
					row[idx] = v.(int64)
				}else if p.DataMeta[cols[idx]]["type"] == "float" {
					row[idx], _ = strconv.ParseFloat(string(v.([]byte)), 64)
				}else if p.DataMeta[cols[idx]]["type"] == "time" {
					if p.DataMeta[cols[idx]]["format"] == "YYYY-MM-DD HH:MM:SS" {
						row[idx] = v.(time.Time).Format("2006-01-02 15:04:05")
					}else if p.DataMeta[cols[idx]]["format"] == "YYYY-MM-DD" {
						row[idx] = v.(time.Time).Format("2006-01-02")
					} else {
						row[idx] = v.(time.Time).Format("2006-01-02 03:04:05")
					}
				} else {
					colType := reflect.TypeOf(v)
					if colType.Name() == "string" {
						row[idx] = v.(string)
					} else {
						row[idx] = string(v.([]byte))
					}
				}
			}
		}
		rowBatch = append(rowBatch, row)
		if len(rowBatch) == 1000 {
			*outdata <- rowBatch
			p.WriteCnt += len(rowBatch)
			rowBatch = make([][]interface{}, 0)
		}
	}
	if len(rowBatch) > 0 {
		*outdata <- rowBatch
		p.WriteCnt += len(rowBatch)
	}
}

func (p *postgresInput) setDataMeta(db *sql.DB, cols []string) {
	datametaSql := "select column_name,data_type from information_schema.columns where table_name = '" + p.table + "'"
	rows, err := db.Query(datametaSql)
	if err != nil {
		log.Println(err)
		return
	}
	p.DataMeta = make(map[string]map[string]interface{})
	for rows.Next() {
		var columnName string
		var dataType string
		err = rows.Scan(&columnName, &dataType)
		if err != nil {
			log.Println(err)
			return
		}
		if dataType == "character varying" {
			p.DataMeta[columnName] = map[string]interface{}{
				"type": "string",
				"format": "",
			}
		} else if dataType == "character" {
			p.DataMeta[columnName] = map[string]interface{}{
				"type": "string",
				"format": "",
			}
		} else if dataType == "integer" {
			p.DataMeta[columnName] = map[string]interface{}{
				"type": "int",
				"format": "",
			}
		} else if dataType == "timestamp without time zone" {
			p.DataMeta[columnName] = map[string]interface{}{
				"type": "time",
				"format": "YYYY-MM-DD HH:MM:SS",
			}
		} else if dataType == "date" {
			p.DataMeta[columnName] = map[string]interface{}{
				"type": "time",
				"format": "YYYY-MM-DD",
			}
		} else if dataType == "numeric" {
			p.DataMeta[columnName] = map[string]interface{}{
				"type": "float",
				"format": "",
			}
		} else {
			p.DataMeta[columnName] = map[string]interface{}{
				"type": "string",
				"format": "",
			}
		}
		for idx, col := range cols {
			if col == columnName {
				p.DataMeta[columnName]["index"] = idx
				break
			}
		}
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	params := parameters.(map[string]interface{})
	p := postgresInput{
		username: params["username"].(string),
		password: params["password"].(string),
		host:     params["host"].(string),
		port:     params["port"].(string),
		database: params["database"].(string),
		table: params["table"].(string),
		sql:      params["sql"].(string),
		AbstractComponent: abstractComponents.AbstractComponent{
			Id: id,
			ReadCnt: 0,
			WriteCnt: 0,
			Name: "postgresInput",
			Status: 0,
			ChanNum: 1,
		},
	}
	return &p, nil
}
