package componentsFactory

import (
	"fmt"
	"log"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/changeFieldType"
	"tinyETL/tinyETLengine/components/mysqlInput"
	"tinyETL/tinyETLengine/components/mysqlOutput"
)

func GetComponents(name string,id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	if name == "mysqlInput" {
		return mysqlInput.NewComponents(id,parameters)
	}  else if name == "changeFieldType" {
		return changeFieldType.NewComponents(id,parameters)
	} else if name == "mysqlOutput" {
		return mysqlOutput.NewComponents(id,parameters)
	}
	log.Fatalln("component not found")
	return nil, fmt.Errorf("Component %s not found", name)
}
