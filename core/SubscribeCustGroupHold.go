package core

import (
	"context"
	"io"
	"strconv"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type sustGroupHoldSubscriber struct {
	ctx     context.Context
	client  pb.RiskMonitorServerClient
	session *r.Session
}

func NewsustGroupHoldSubscriber(ctx context.Context, client pb.RiskMonitorServerClient, session *r.Session) Subscriber {
	return &sustGroupHoldSubscriber{
		ctx:     ctx,
		client:  client,
		session: session,
	}
}

func (c *sustGroupHoldSubscriber) Read(buffer int) <-chan interface{} {
	log.Infoln("SubscribeCustGroupHold started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeCustGroupHold(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeCustGroupHold failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeCustGroupHold receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeCustGroupHold receive failed : %v", err)
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

func (c *sustGroupHoldSubscriber) Convert(in <-chan interface{}) <-chan *Message {
	out := make(chan *Message)
	go func() {
		defer close(out)
		for n := range in {
			item := n.(*pb.CustGroupHoldRtn)

			msg := &Message{
				TableName:  TableName_SubscribeCustGroupHold,
				ActionFlag: item.ActionFlag,
				ActionKey:  string(item.MonitorNo) + "#" + string(item.CustGroupNo) + "#" + string(item.ContractCode) + "#" + strconv.Itoa(int(item.HoldType)),
				Msg:        *item,
			}

			out <- msg
		}

	}()
	return out
}

func (c *sustGroupHoldSubscriber) Write(in <-chan *Message) {
	for msg := range in {

		err := msg.Replace(c.session)
		if err != nil {
			log.Errorf("Write CustGroupHoldRtn message failed : err: %v, message: %+v", err, *msg)
			return
		}
	}
}
