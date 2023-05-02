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
	"strconv"
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

// RandFileName 随机文件名
func RandFileName(fileName string) string {
	uuid, _ := GenerateUUID()
	randStr := uuid + getRandstring(16)
	return randStr + filepath.Ext(fileName)
}

func DumpToFile(data interface{}, file *os.File) error {
	for _, v := range data.([][]interface{}) {
		tmp, _ := json.Marshal(v)
		_, err := file.WriteString(string(tmp) + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func LoadFromFile(file *os.File, datameta map[string]map[string]interface{}, start int, end int) (data [][]interface{}, err error) {
	file.Seek(0, 0)
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
						if reflect.TypeOf(v).String() == "float64" {
							tmp[i] = int64(v.(float64))
						} else if reflect.TypeOf(v).String() == "string" {
							tmp[i], _ = strconv.ParseInt(v.(string), 10, 64)
						} else {
							tmp[i] = v.(int64)
						}
					} else if v1["type"].(string) == "float" {
						tmp[i], _ = v.(float64)
					} else if v1["type"].(string) == "string" {
						if reflect.TypeOf(v).String() == "float64" {
							tmp[i] = strconv.FormatFloat(v.(float64), 'f', -1, 64)
						} else {
							tmp[i] = v.(string)
						}
					} else if v1["type"].(string) == "time" {
						if v1["format"].(string) == "YYYY-MM-DD" {
							if reflect.TypeOf(v).String() == "string" {
								tmp[i], err = time.Parse(time.RFC3339, v.(string))
								if err != nil {
									log.Println(err)
								}
							} else if reflect.TypeOf(v).String() == "time.Time" {
								tmp[i] = v.(time.Time).Format("2006-01-02")
							}
						} else if v1["format"].(string) == "YYYY-MM-DD HH:MM:SS" {
							if reflect.TypeOf(v).String() == "string" {
								tmp[i], err = time.Parse(time.RFC3339, v.(string))
								if err != nil {
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
