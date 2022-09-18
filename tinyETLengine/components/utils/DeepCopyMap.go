package utils


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