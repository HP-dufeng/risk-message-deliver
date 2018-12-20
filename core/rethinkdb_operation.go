package core

import (
	"reflect"

	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type Subscriber interface {
	Read(buffer int) <-chan interface{}
	Convert(in <-chan interface{}) <-chan *Message
	Write(in <-chan *Message)
}

type Message struct {
	TableName  string
	ActionFlag uint32
	ActionKey  string
	Msg        interface{}
}

func (msg *Message) Replace(session *r.Session) error {

	if msg.ActionFlag != 2 {
		// var m map[string]interface{}
		// j, _ := json.Marshal(msg.Msg)
		// json.Unmarshal(j, &m)
		m := struct2Map(msg.Msg)
		m["ActionKey"] = msg.ActionKey
		_, err := r.Table(msg.TableName).Get(msg.ActionKey).Replace(m).RunWrite(session)
		if err != nil {
			return err
		}
	} else {
		_, err := r.Table(msg.TableName).Get(msg.ActionKey).Replace(nil).RunWrite(session)
		if err != nil {
			return err
		}
	}

	return nil
}

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
