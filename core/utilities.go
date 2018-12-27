package core

import (
	"reflect"

	"golang.org/x/text/encoding/simplifiedchinese"
)

var (
	decoder = simplifiedchinese.GBK.NewDecoder()
)

func struct2Map(obj interface{}) map[string]interface{} {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)

	var data = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		if b, ok := v.Field(i).Interface().([]byte); ok {
			str, _ := decoder.String(string(b))
			data[t.Field(i).Name] = str
		} else {
			data[t.Field(i).Name] = v.Field(i).Interface()
		}
	}
	return data
}
