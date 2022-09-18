package componentsFactory

import (
	"fmt"
	"log"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/changeFieldType"
	"tinyETL/tinyETLengine/components/columnToRow"
	"tinyETL/tinyETLengine/components/cutString"
	"tinyETL/tinyETLengine/components/fieldSelect"
	"tinyETL/tinyETLengine/components/mysqlInput"
	"tinyETL/tinyETLengine/components/mysqlOutput"
	"tinyETL/tinyETLengine/components/removeDuplicateRecord"
	"tinyETL/tinyETLengine/components/replaceString"
	"tinyETL/tinyETLengine/components/rowToColumn"
	"tinyETL/tinyETLengine/components/valueMapping"
)

func GetComponents(name string,id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	if name == "mysqlInput" {
		return mysqlInput.NewComponents(id,parameters)
	}  else if name == "changeFieldType" {
		return changeFieldType.NewComponents(id,parameters)
	} else if name == "mysqlOutput" {
		return mysqlOutput.NewComponents(id,parameters)
	} else if name == "valueMapping" {
		return valueMapping.NewComponents(id, parameters)
	} else if name == "cutString" {
		return cutString.NewComponents(id, parameters)
	} else if name == "replaceString" {
		return replaceString.NewComponents(id, parameters)
	} else if name == "removeDuplicateRecord" {
		return removeDuplicateRecord.NewComponents(id, parameters)
	}else if name == "columnToRow" {
		return columnToRow.NewComponents(id, parameters)
	}else if name == "fieldSelect" {
		return fieldSelect.NewComponents(id, parameters)
	}else if name == "rowToColumn" {
		return rowToColumn.NewComponents(id, parameters)
	} else {
		log.Println("component not found")
		return nil, fmt.Errorf("component not found")
	}
}
