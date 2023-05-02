package postgresOutput

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
	"strconv"
	"strings"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/utils"
)

type fieldMapping struct {
	srcField string
	dstField string
}

type postgresOutput struct {
	username      string `json:"username"`
	password      string `json:"password"`
	host          string `json:"ip"`
	port          string `json:"port"`
	database      string `json:"database"`
	table         string `json:"table"`
	fieldMappings []fieldMapping
	abstractComponents.AbstractComponent
}

func (m *postgresOutput) BatchInsert(dataBatchArg interface{}, db *sql.DB) {
	dataBatch := dataBatchArg.([][]interface{})
	var sql strings.Builder
	sql.Grow(1024)
	sql.Write([]byte("insert into " + m.table + " ("))
	for idx, v := range m.fieldMappings {
		sql.Write([]byte(v.dstField))
		if idx != len(m.fieldMappings)-1 {
			sql.Write([]byte(","))
		}
	}
	vals := make([]interface{}, len(dataBatch)*len(m.fieldMappings))
	sql.Write([]byte(") values "))
	for idx, data := range dataBatch {
		sql.Write([]byte("("))
		for idx1, v := range m.fieldMappings {
			sql.Write([]byte("$" + strconv.Itoa(idx*len(m.fieldMappings)+idx1+1)))
			if idx1 != len(m.fieldMappings)-1 {
				sql.Write([]byte(","))
			}
			vals[idx*len(m.fieldMappings)+idx1] = data[m.DataMeta[v.srcField]["index"].(int)]
		}
		sql.Write([]byte(")"))
		if idx != len(dataBatch)-1 {
			sql.Write([]byte(","))
		}
	}
	_, err := db.Exec(sql.String(), vals...)
	if err != nil {
		log.Println(err)
	}
}

func (m *postgresOutput) Run(indata *chan interface{}, outdata *chan interface{}, datameta map[string]map[string]interface{}, otherChannels ...interface{}) {
	m.SetStartTime()
	defer close(*outdata)
	defer m.SetEndTime()
	m.DataMeta = utils.DeepCopy(datameta).(map[string]map[string]interface{})
	m.SetStatus(1)
	db, _ := sql.Open("postgres", "user="+m.username+" password="+m.password+" host="+m.host+" port="+m.port+" dbname="+m.database+" sslmode=disable")
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Println(err)
		}
	}(db)

	for {
		dataBatch, ok := <-*indata
		if !ok {
			break
		}
		if len(dataBatch.([][]interface{})) == 0 {
			continue
		}
		j := 0
		for i := 0; i < len(dataBatch.([][]interface{})); i++ {
			if (i-j)*len(m.fieldMappings) > 65535 {
				m.BatchInsert(dataBatch.([][]interface{})[j:i-1], db)
				j = i - 1
			} else if i == len(dataBatch.([][]interface{}))-1 {
				m.BatchInsert(dataBatch.([][]interface{})[j:], db)
			}
		}
	}
}

func NewComponents(id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	params := parameters.(map[string]interface{})
	m := postgresOutput{
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
			Name:     "postgresOutput",
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
