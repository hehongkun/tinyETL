package utils

import (
	"bufio"
	"encoding/json"
	"github.com/google/uuid"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

func GenerateUUID() (string, error) {
	uid, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}
	return uid.String(), nil
}

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


// 获取随机字母+数字组合字符串
func getRandstring(length int) string {
	if length < 1 {
		return ""
	}
	char := "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	charArr := strings.Split(char, "")
	charlen := len(charArr)
	ran := rand.New(rand.NewSource(time.Now().Unix()))
	var rchar string = ""
	for i := 1; i <= length; i++ {
		rchar = rchar + charArr[ran.Intn(charlen)]
	}
	return rchar
}
// 随机文件名
func RandFileName(fileName string) string{
	randStr := getRandstring(16)
	return randStr + filepath.Ext(fileName)
}

func DumpToFile(data interface{}, filename string) error {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0777)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, v := range data.([][]interface{}) {
		tmp, _ := json.Marshal(v)
		_, err := file.WriteString(string(tmp) + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func LoadFromFile(filename string, datameta map[string]map[string]interface{}, start int, end int) (data [][]interface{}, err error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println(err)
		}
	}(file)
	r := bufio.NewReader(file)
	l := 0
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		l += 1
		if l < start {
			continue
		}
		if l > end {
			break
		}
		var tmp []interface{}
		err = json.Unmarshal([]byte(line), &tmp)
		if err != nil {
			return nil, err
		}
		for i, v := range tmp {
			if tmp[i] == nil {
				continue
			}
			for _, v1 := range datameta {
				if v1["index"].(int) == i {
					if v1["type"].(string) == "int" {
						tmp[i] = int64(v.(float64))
					} else if v1["type"].(string) == "float" {
						tmp[i], _ = v.(float64)
					} else if v1["type"].(string) == "string" {
						tmp[i] = v.(string)
					} else if v1["type"].(string) == "time" {
						if v1["format"].(string) == "YYYY-MM-DD" {
							if reflect.TypeOf(v).String() == "string" {
								tmp[i], err = time.Parse(time.RFC3339, v.(string))
								if err != nil{
									log.Println(err)
								}
							} else if reflect.TypeOf(v).String() == "time.Time" {
								tmp[i] = v.(time.Time).Format("2006-01-02")
							}
						} else if v1["format"].(string) == "YYYY-MM-DD HH:MM:SS" {
							if reflect.TypeOf(v).String() == "string" {
								tmp[i], err = time.Parse(time.RFC3339, v.(string))
								if err != nil{
									log.Println(err)
								}
							} else if reflect.TypeOf(v).String() == "time.Time" {
								tmp[i] = v.(time.Time).Format("2006-01-02 15:04:05")
							}
						} else {
							if reflect.TypeOf(v).String() == "string" {
								tmp[i], _ = time.Parse("2006-01-02 03:04:05", v.(string))
							} else if reflect.TypeOf(v).String() == "time.Time" {
								tmp[i] = v.(time.Time).Format("2006-01-02 03:04:05")
							}
						}
					}
				}
			}
		}
		data = append(data, tmp)
	}
	return data, nil
}
