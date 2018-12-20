package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type tunnelRealFundSubscriber struct {
	ctx     context.Context
	client  pb.RiskMonitorServerClient
	session *r.Session
}

func NewTunnelRealFundSubscriber(ctx context.Context, client pb.RiskMonitorServerClient, session *r.Session) Subscriber {
	return &tunnelRealFundSubscriber{
		ctx:     ctx,
		client:  client,
		session: session,
	}
}

func (c *tunnelRealFundSubscriber) Read(buffer int) <-chan interface{} {
	log.Infoln("SubscribeTunnelRealFund started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeTunnelRealFund(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeTunnelRealFund failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeTunnelRealFund receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeTunnelRealFund receive failed : %v", err)
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

func (c *tunnelRealFundSubscriber) Convert(in <-chan interface{}) <-chan *Message {
	out := make(chan *Message)
	go func() {
		defer close(out)
		for n := range in {
			item := n.(*pb.TunnelRealFundRtn)

			msg := &Message{
				TableName:  TableName_SubscribeTunnelRealFund,
				ActionFlag: item.ActionFlag,
				ActionKey:  string(item.MonitorNo) + "#" + string(item.TunnelCode) + "#" + string(item.CurrencyCode),
				Msg:        *item,
			}

			out <- msg
		}

	}()
	return out
}

func (c *tunnelRealFundSubscriber) Write(in <-chan *Message) {
	for msg := range in {

		err := msg.Replace(c.session)
		if err != nil {
			log.Errorf("Write TunnelRealFundRtn message failed : err: %v, message: %+v", err, *msg)
			return
		}
	}
}
