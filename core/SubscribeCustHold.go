package core

import (
	"context"
	"io"
	"strconv"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
)

type custHoldSubscriber struct {
	ctx    context.Context
	client pb.RiskMonitorServerClient
}

func NewCustHoldSubscriber(ctx context.Context, client pb.RiskMonitorServerClient) Subscriber {
	return &custHoldSubscriber{
		ctx:    ctx,
		client: client,
	}
}

func (c *custHoldSubscriber) Subscribe(buffer int) <-chan interface{} {
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

				msg := struct2Map(*item)
				msg["ActionKey"] = string(item.MonitorNo) + "#" + string(item.CustNo) + "#" + string(item.ContractCode) + "#" + strconv.Itoa(int(item.HoldType))
				out <- msg
			case <-c.ctx.Done():
				return
			}

		}
	}()

	return out
}
