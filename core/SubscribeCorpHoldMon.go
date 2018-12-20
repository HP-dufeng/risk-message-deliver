package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type corpHoldMonSubscriber struct {
	ctx     context.Context
	client  pb.RiskMonitorServerClient
	session *r.Session
}

func NewCorpHoldMonSubscriber(ctx context.Context, client pb.RiskMonitorServerClient, session *r.Session) Subscriber {
	return &corpHoldMonSubscriber{
		ctx:     ctx,
		client:  client,
		session: session,
	}
}

func (c *corpHoldMonSubscriber) Read(buffer int) <-chan interface{} {
	log.Infoln("SubscribeCorpHoldMon started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeCorpHoldMon(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeCorpHoldMon failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeCorpHoldMon receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeCorpHoldMon receive failed : %v", err)
					return
				}

				out <- item
			case <-c.ctx.Done():
				return
			}

		}
	}()

	return out
}

func (c *corpHoldMonSubscriber) Convert(in <-chan interface{}) <-chan *Message {
	out := make(chan *Message)
	go func() {
		defer close(out)
		for n := range in {
			item := n.(*pb.CorpHoldMonRtn)

			msg := &Message{
				TableName:  TableName_SubscribeCorpHoldMon,
				ActionFlag: item.ActionFlag,
				ActionKey:  string(item.MonitorNo) + "#" + string(item.ContractCode),
				Msg:        *item,
			}

			out <- msg
		}

	}()
	return out
}

func (c *corpHoldMonSubscriber) Write(in <-chan *Message) {
	for msg := range in {

		err := msg.Replace(c.session)
		if err != nil {
			log.Errorf("Write CorpHoldMonRtn message failed : err: %v, message: %+v", err, *msg)
			return
		}
	}
}
