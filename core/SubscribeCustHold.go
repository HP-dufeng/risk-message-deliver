package core

import (
	"context"
	"io"
	"strconv"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
	r "gopkg.in/rethinkdb/rethinkdb-go.v5"
)

type custHoldSubscriber struct {
	ctx     context.Context
	client  pb.RiskMonitorServerClient
	session *r.Session
}

func NewCustHoldSubscriber(ctx context.Context, client pb.RiskMonitorServerClient, session *r.Session) Subscriber {
	return &custHoldSubscriber{
		ctx:     ctx,
		client:  client,
		session: session,
	}
}

func (c *custHoldSubscriber) Read(buffer int) <-chan interface{} {
	log.Infoln("SubscribeCustHold started...")

	out := make(chan interface{}, buffer)

	go func() {
		defer close(out)

		stream, err := c.client.SubscribeCustHold(c.ctx, &pb.SubscribeReq{})
		if err != nil {
			log.Errorf("SubscribeCustHold failed : %v", err)
			return
		}
		for {
			select {
			default:
				item, err := stream.Recv()
				if err == io.EOF {
					log.Warnf("SubscribeCustHold receive EOF : %v", err)
					return
				}
				if err != nil {
					log.Errorf("SubscribeCustHold receive failed : %v", err)
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

func (c *custHoldSubscriber) Convert(in <-chan interface{}) <-chan *Message {
	out := make(chan *Message)
	go func() {
		defer close(out)
		for n := range in {
			item := n.(*pb.CustHoldRtn)

			msg := &Message{
				TableName:  TableName_SubscribeCustHold,
				ActionFlag: item.ActionFlag,
				ActionKey:  string(item.MonitorNo) + "#" + string(item.CustNo) + "#" + string(item.ContractCode) + "#" + strconv.Itoa(int(item.HoldType)),
				Msg:        *item,
			}

			out <- msg
		}

	}()
	return out
}

func (c *custHoldSubscriber) Write(in <-chan *Message) {
	for msg := range in {

		err := msg.Replace(c.session)
		if err != nil {
			log.Errorf("Write CustHoldRtn message failed : err: %v, message: %+v", err, *msg)
			return
		}
	}
}
