package utils

import (
	"log"
	"strconv"
	"time"
)

func DeepCopy(value map[string]map[string]interface{}) interface{} {
	newMap := make(map[string]map[string]interface{})
	for k, v := range value {
		newMap[k] = make(map[string]interface{})
		for k1, v1 := range v {
			newMap[k][k1] = v1
		}
	}
	return newMap
}


func ConvertToString(value interface{}) string {
	switch value.(type) {
	case string:
		return value.(string)
	case int:
		return strconv.Itoa(value.(int))
	case int64:
		return strconv.FormatInt(value.(int64), 10)
	case float64:
		return strconv.FormatFloat(value.(float64), 'f', -1, 64)
	case bool:
		return strconv.FormatBool(value.(bool))
	case time.Time:
		return value.(time.Time).Format("2006-01-02 15:04:05")
	default:
		log.Println("convert to string error, unknown type")
		return ""
	}
}