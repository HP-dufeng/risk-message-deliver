package core

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type Subscriber interface {
	Subscribe(buffer int) <-chan interface{}
}

type Pipeline interface {
	BufferTime(in <-chan interface{}) <-chan []interface{}
	BufferCount(in <-chan interface{}) <-chan []interface{}
	Write(in <-chan []interface{})
}

type pipeline struct {
	ctx     context.Context
	session *r.Session
	table   string
}

func NewPipeline(ctx context.Context, session *r.Session, table string) Pipeline {
	return &pipeline{
		ctx:     ctx,
		session: session,
		table:   table,
	}
}

func (s *pipeline) BufferTime(in <-chan interface{}) <-chan []interface{} {
	out := make(chan []interface{})

	bufferTime := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-time.After(100 * time.Millisecond):
				bufferTime <- true
			case <-s.ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer close(out)

		messages := []interface{}{}
		for {
			select {
			case msg := <-in:
				messages = append(messages, msg)
			case <-bufferTime:
				if len(messages) > 0 {
					out <- messages
					messages = nil
				}

			case <-s.ctx.Done():
				return

			}
		}
	}()

	return out
}

func (s *pipeline) BufferCount(in <-chan interface{}) <-chan []interface{} {
	out := make(chan []interface{})

	bufferTime := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-time.After(1 * time.Second):
				bufferTime <- true
			case <-s.ctx.Done():
				return
			}
		}
	}()

	go func() {
		defer close(out)
		var messages []interface{}
		for msg := range in {
			select {
			case <-bufferTime:
				if len(messages) > 0 {
					out <- messages
					messages = nil
				}
			default:
				messages = append(messages, msg)
				if len(messages) >= 100 {
					out <- messages
					messages = nil
				}
			}
		}
	}()

	return out
}

func (s *pipeline) Write(in <-chan []interface{}) {
	for messages := range in {
		// log.Infof("Write : %v", len(messages))
		res, err := r.Table(s.table).Insert(messages, r.InsertOpts{Durability: "hard", Conflict: "update", ReturnChanges: false}).Run(s.session)
		if err != nil {
			log.Error(err)
		}
		res.Close()
	}
}
