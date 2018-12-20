package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type custRiskSubscriber struct {
	ctx     context.Context
	client  pb.RiskMonitorServerClient
	session *r.Session
}

func NewCustRiskSubscriber(ctx context.Context, client pb.RiskMonitorServerClient, session *r.Session) Subscriber {
	return &custRiskSubscriber{
		ctx:     ctx,
		client:  client,
		session: session,
	}
}

func (c *custRiskSubscriber) Read(buffer int) <-chan interface{} {
	log.Infoln("SubscribeCustRisk started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeCustRisk(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeCustRisk failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeCustRisk receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeCustRisk receive failed : %v", err)
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

func (c *custRiskSubscriber) Convert(in <-chan interface{}) <-chan *Message {
	out := make(chan *Message)
	go func() {
		defer close(out)
		for n := range in {
			item := n.(*pb.CustRiskRtn)

			msg := &Message{
				TableName:  TableName_SubscribeCustRisk,
				ActionFlag: item.ActionFlag,
				ActionKey:  string(item.MonitorNo) + "#" + string(item.CustNo) + "#" + string(item.CurrencyCode),
				Msg:        *item,
			}

			out <- msg
		}

	}()
	return out
}

func (c *custRiskSubscriber) Write(in <-chan *Message) {
	for msg := range in {
		err := msg.Replace(c.session)
		if err != nil {
			log.Errorf("Write CustRisk message failed : err: %v, message: %+v", err, *msg)
			return
		}
	}
}
