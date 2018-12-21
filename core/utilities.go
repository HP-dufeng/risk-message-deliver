package core

import "reflect"

func struct2Map(obj interface{}) map[string]interface{} {
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)

	var data = make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {
		if b, ok := v.Field(i).Interface().([]byte); ok {
			data[t.Field(i).Name] = string(b)
		} else {
			data[t.Field(i).Name] = v.Field(i).Interface()
		}
	}
	return data
}
