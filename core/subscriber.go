package core

import (
	"context"
	"strconv"
	"time"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type Subscriber2 interface {
	Read(buffer int) <-chan interface{}
	Convert(in <-chan interface{}) <-chan interface{}
	BufferTime(in <-chan interface{}) <-chan []interface{}
	BufferCount(in <-chan interface{}) <-chan []interface{}
	Write(in <-chan []interface{})
}

type subscriber struct {
	ctx     context.Context
	client  pb.RiskMonitorServerClient
	session *r.Session
	table   string
}

func NewSubscriber(ctx context.Context, table string, client pb.RiskMonitorServerClient, session *r.Session) Subscriber2 {
	return &subscriber{
		ctx:     ctx,
		client:  client,
		session: session,
		table:   table,
	}
}

// func (s *subscriber) Read(buffer int) <-chan interface{} {
// 	log.Infof("%s started...", s.table)

// 	out := make(chan interface{}, buffer)

// 	go func() {
// 		defer close(out)

// 		stream, err := s.client.SubscribeCustRisk(s.ctx, &pb.SubscribeReq{})
// 		if err != nil {
// 			log.Errorf("%s failed : %v", s.table, err)
// 			return
// 		}
// 		for {
// 			select {
// 			default:
// 				item, err := stream.Recv()
// 				if err == io.EOF {
// 					log.Warnf("%s receive EOF : %v", s.table, err)
// 					return
// 				}
// 				if err != nil {
// 					log.Errorf("%s receive failed : %v", s.table, err)
// 					return
// 				}

// 				out <- item
// 			case <-s.ctx.Done():
// 				return
// 			}

// 		}
// 	}()

// 	return out
// }

func (s *subscriber) Read(buffer int) <-chan interface{} {
	log.Infof("%s started...", s.table)

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		for {
			select {
			default:
				item := &pb.CustRiskRtn{
					ActionFlag:   randomActionFlags(),
					MonitorNo:    []byte(strconv.Itoa(randomIndex(20))),
					CustNo:       []byte(strconv.Itoa(randomIndex(10))),
					CustClass:    []byte("CustClass"),
					CustName:     []byte("CustName"),
					MobilePhone:  []byte("MobilePhone"),
					CurrencyCode: []byte(strconv.Itoa(randomIndex(2))),
				}

				out <- item
			case <-s.ctx.Done():
				return
			}

		}
	}()

	return out
}

func (s *subscriber) Convert(in <-chan interface{}) <-chan interface{} {
	out := make(chan interface{})
	go func() {
		defer close(out)
		for n := range in {
			item := n.(*pb.CustRiskRtn)

			msg := struct2Map(*item)
			msg["ActionKey"] = string(item.MonitorNo) + "#" + string(item.CustNo) + "#" + string(item.CurrencyCode)

			out <- msg
		}

	}()
	return out
}

func (s *subscriber) BufferTime(in <-chan interface{}) <-chan []interface{} {
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

func (s *subscriber) BufferCount(in <-chan interface{}) <-chan []interface{} {
	out := make(chan []interface{})

	go func() {
		defer close(out)
		var messages []interface{}
		for msg := range in {
			messages = append(messages, msg)
			if len(messages) >= 100 {
				out <- messages
				messages = nil
			}
		}
	}()

	return out
}

func (s *subscriber) Write(in <-chan []interface{}) {
	for messages := range in {
		log.Infof("Write : %v", len(messages))
		res, err := r.Table(s.table).Insert(messages, r.InsertOpts{Durability: "hard", Conflict: "update", ReturnChanges: false}).Run(s.session)
		if err != nil {
			log.Error(err)
		}
		res.Close()
	}
}
