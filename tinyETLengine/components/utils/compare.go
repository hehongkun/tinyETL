package utils

import "strconv"

func CheckEqual(a interface{}, b interface{}, dataType string) bool {
	if dataType == "int" {
		c, _ := strconv.Atoi(b.(string))
		return a.(int) == c
	} else if dataType == "string" {
		return a.(string) == b.(string)
	} else if dataType == "float" {
		c, _ := strconv.ParseFloat(b.(string), 64)
		return a.(float64) == c
	} else {
		return false
	}
}


func CheckGreater(a interface{}, b interface{}, dataType string) bool {
	if dataType == "int" {
		return a.(int64) > b.(int64)
	} else if dataType == "string" {
		return a.(string) > b.(string)
	} else if dataType == "float" {
		return a.(float64) > b.(float64)
	} else {
		return false
	}
}


func CheckLess(a interface{}, b interface{}, dataType string) bool {
	if dataType == "int" {
		return a.(int) < b.(int)
	} else if dataType == "string" {
		return a.(string) < b.(string)
	} else if dataType == "float" {
		return a.(float64) < b.(float64)
	} else {
		return false
	}
}