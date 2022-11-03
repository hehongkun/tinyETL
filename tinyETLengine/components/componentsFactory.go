package componentsFactory

import (
	"fmt"
	"log"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/addField"
	"tinyETL/tinyETLengine/components/addSequence"
	"tinyETL/tinyETLengine/components/changeFieldType"
	"tinyETL/tinyETLengine/components/changeSequenceByValue"
	"tinyETL/tinyETLengine/components/columnToRow"
	"tinyETL/tinyETLengine/components/cutString"
	"tinyETL/tinyETLengine/components/deleteField"
	"tinyETL/tinyETLengine/components/fieldSelect"
	"tinyETL/tinyETLengine/components/fillDate"
	"tinyETL/tinyETLengine/components/fillNum"
	"tinyETL/tinyETLengine/components/fillString"
	"tinyETL/tinyETLengine/components/filterDate"
	"tinyETL/tinyETLengine/components/filterDateRange"
	"tinyETL/tinyETLengine/components/filterNull"
	"tinyETL/tinyETLengine/components/filterNum"
	"tinyETL/tinyETLengine/components/filterNumRange"
	"tinyETL/tinyETLengine/components/filterString"
	"tinyETL/tinyETLengine/components/getFieldLength"
	"tinyETL/tinyETLengine/components/leftJoin"
	"tinyETL/tinyETLengine/components/mysqlInput"
	"tinyETL/tinyETLengine/components/mysqlOutput"
	"tinyETL/tinyETLengine/components/postgresInput"
	"tinyETL/tinyETLengine/components/postgresOutput"
	"tinyETL/tinyETLengine/components/removeDuplicateRecord"
	"tinyETL/tinyETLengine/components/replaceString"
	"tinyETL/tinyETLengine/components/rowFlatten"
	"tinyETL/tinyETLengine/components/rowToColumn"
	"tinyETL/tinyETLengine/components/setFieldValue"
	"tinyETL/tinyETLengine/components/splitFieldToRows"
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
	}else if name == "filterNull" {
		return filterNull.NewComponents(id, parameters)
	}else if name == "filterNum" {
		return filterNum.NewComponents(id, parameters)
	}else if name == "filterDate" {
		return filterDate.NewComponents(id, parameters)
	}else if name == "filterString" {
		return filterString.NewComponents(id, parameters)
	}else if name == "getFieldLength" {
		return getFieldLength.NewComponents(id, parameters)
	}else if name == "fillNum" {
		return fillNum.NewComponents(id, parameters)
	}else if name == "filterNumRange" {
		return filterNumRange.NewComponents(id, parameters)
	}else if name == "setFieldValue" {
		return setFieldValue.NewComponents(id, parameters)
	}else if name == "fillDate" {
		return fillDate.NewComponents(id, parameters)
	}else if name == "fillString" {
		return fillString.NewComponents(id, parameters)
	}else if name == "leftJoin" {
		return leftJoin.NewComponents(id, parameters)
	}else if name == "splitFieldToRows" {
		return splitFieldToRows.NewComponents(id, parameters)
	}else if name == "deleteField" {
		return deleteField.NewComponents(id, parameters)
	}else if name == "filterDateRange" {
		return filterDateRange.NewComponents(id, parameters)
	}else if name == "postgresInput" {
		return postgresInput.NewComponents(id, parameters)
	}else if name == "addSequence" {
		return addSequence.NewComponents(id, parameters)
	}else if name == "addField" {
		return addField.NewComponents(id, parameters)
	}else if name == "changeSequenceByValue" {
		return changeSequenceByValue.NewComponents(id, parameters)
	}else if name == "postgresOutput" {
		return postgresOutput.NewComponents(id, parameters)
	}else if name == "rowFlatten" {
		return rowFlatten.NewComponents(id, parameters)
	} else {
		log.Println("component not found")
		return nil, fmt.Errorf("component not found")
	}
}




