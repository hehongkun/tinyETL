package componentsFactory

import (
	"fmt"
	"tinyETL/tinyETLengine/components/abstractComponents"
	"tinyETL/tinyETLengine/components/mysqlInput"
)

func GetComponents(name string,id string, parameters interface{}) (abstractComponents.VirtualComponents, error) {
	if name == "mysqlInput" {
		return mysqlInput.NewComponents(id,parameters)
	}
	return nil, fmt.Errorf("Component %s not found", name)
}
