package core

import (
	"context"
	"io"

	pb "github.com/fengdu/risk-monitor-server/pb"

	log "github.com/sirupsen/logrus"
)

type nearDediveHoldSubscriber struct {
	ctx    context.Context
	client pb.RiskMonitorServerClient
}

func NewNearDediveHoldSubscriber(ctx context.Context, client pb.RiskMonitorServerClient) Subscriber {
	return &nearDediveHoldSubscriber{
		ctx:    ctx,
		client: client,
	}
}

func (c *nearDediveHoldSubscriber) Subscribe(buffer int) <-chan interface{} {
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

				msg := struct2Map(*item)
				msg["ActionKey"] = string(item.MonitorNo) + "#" + string(item.CustNo) + "#" + string(item.ExchCode) + "#" + string(item.ContractCode)
				out <- msg
			case <-c.ctx.Done():
				return
			}

		}
	}()

	return out
}
