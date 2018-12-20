package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type nearDediveHoldSubscriber struct {
	ctx     context.Context
	client  pb.RiskMonitorServerClient
	session *r.Session
}

func NewnearDediveHoldSubscriber(ctx context.Context, client pb.RiskMonitorServerClient, session *r.Session) Subscriber {
	return &nearDediveHoldSubscriber{
		ctx:     ctx,
		client:  client,
		session: session,
	}
}

func (c *nearDediveHoldSubscriber) Read(buffer int) <-chan interface{} {
	log.Infoln("SubscribeNearDediveHold started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeNearDediveHold(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeNearDediveHold failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeNearDediveHold receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeNearDediveHold receive failed : %v", err)
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

func (c *nearDediveHoldSubscriber) Convert(in <-chan interface{}) <-chan *Message {
	out := make(chan *Message)
	go func() {
		defer close(out)
		for n := range in {
			item := n.(*pb.NearDediveHoldRtn)

			msg := &Message{
				TableName:  TableName_SubscribeNearDediveHold,
				ActionFlag: item.ActionFlag,
				ActionKey:  string(item.MonitorNo) + "#" + string(item.CustNo) + "#" + string(item.ExchCode) + "#" + string(item.ContractCode),
				Msg:        *item,
			}

			out <- msg
		}

	}()
	return out
}

func (c *nearDediveHoldSubscriber) Write(in <-chan *Message) {
	for msg := range in {

		err := msg.Replace(c.session)
		if err != nil {
			log.Errorf("Write NearDediveHoldRtn message failed : err: %v, message: %+v", err, *msg)
			return
		}
	}
}
