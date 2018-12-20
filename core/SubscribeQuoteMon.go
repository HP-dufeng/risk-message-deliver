package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type quoteMonSubscriber struct {
	ctx     context.Context
	client  pb.RiskMonitorServerClient
	session *r.Session
}

func NewQuoteMonSubscriber(ctx context.Context, client pb.RiskMonitorServerClient, session *r.Session) Subscriber {
	return &quoteMonSubscriber{
		ctx:     ctx,
		client:  client,
		session: session,
	}
}

func (c *quoteMonSubscriber) Read(buffer int) <-chan interface{} {
	log.Infoln("SubscribeQuoteMon started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeQuoteMon(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeQuoteMon failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeQuoteMon receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeQuoteMon receive failed : %v", err)
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

func (c *quoteMonSubscriber) Convert(in <-chan interface{}) <-chan *Message {
	out := make(chan *Message)
	go func() {
		defer close(out)
		for n := range in {
			item := n.(*pb.QuoteMonRtn)

			msg := &Message{
				TableName:  TableName_SubscribeQuoteMon,
				ActionFlag: item.ActionFlag,
				ActionKey:  string(item.MonitorNo) + "#" + string(item.ContractCode),
				Msg:        *item,
			}

			out <- msg
		}

	}()
	return out
}

func (c *quoteMonSubscriber) Write(in <-chan *Message) {
	for msg := range in {

		err := msg.Replace(c.session)
		if err != nil {
			log.Errorf("Write QuoteMon message failed : err: %v, message: %+v", err, *msg)
			return
		}
	}
}
